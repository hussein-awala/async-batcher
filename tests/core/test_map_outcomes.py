from __future__ import annotations

import asyncio
from contextlib import aclosing
from dataclasses import FrozenInstanceError

import pytest
from async_batcher import AsyncBatcher, MapOutcome

from tests.core.test_bounded_map import ItemGates
from tests.core_helpers import wait_until


@pytest.mark.asyncio
@pytest.mark.parametrize("ordered", [False, True])
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("eager", [False, True])
async def test_mixed_outcomes_preserve_index_input_and_successful_none(harness, ordered, asynchronous, eager):
    if eager and not hasattr(asyncio, "eager_task_factory"):
        pytest.skip("Eager tasks require Python 3.12+")
    loop = asyncio.get_running_loop()
    old_factory = loop.get_task_factory()
    if eager:
        loop.set_task_factory(asyncio.eager_task_factory)
    error = ValueError("bad input")
    items = [{"id": 3}, {"id": -1}, {"id": 0}, {"id": 3}]

    class Mixed(AsyncBatcher):
        async def process_batch(self, batch):
            return [
                error if item["id"] < 0 else None if item["id"] == 0 else item["id"] * 2 for item in batch
            ]

    batcher = harness.create(Mixed, max_batch_size=2, max_queue_size=1, max_queue_time=0)

    async def source():
        for item in items:
            yield item

    try:
        outcomes = [
            outcome
            async for outcome in batcher.map_outcomes(
                source() if asynchronous else items, max_pending=3, ordered=ordered
            )
        ]
        if ordered:
            assert [outcome.index for outcome in outcomes] == list(range(4))
        outcomes.sort(key=lambda outcome: outcome.index)
        assert [outcome.result for outcome in outcomes] == [6, None, None, 6]
        assert [outcome.succeeded for outcome in outcomes] == [True, False, True, True]
        assert outcomes[1].error is error
        assert all(outcome.item is items[outcome.index] for outcome in outcomes)
        with pytest.raises(FrozenInstanceError):
            outcomes[0].index = 9
        assert batcher.stats().failed == 1
        assert await batcher.process({"id": 5}) == 10
    finally:
        loop.set_task_factory(old_factory)


@pytest.mark.asyncio
async def test_buffered_failures_count_toward_pending_bound_and_do_not_abort_ordered_map(harness):
    batcher = harness.create(ItemGates)
    batcher.errors[1] = ValueError("one")
    batcher.errors[2] = ValueError("two")
    consumed, closed = [], []

    def source():
        try:
            for item in range(20):
                consumed.append(item)
                yield item
        finally:
            closed.append(True)

    async with aclosing(batcher.map_outcomes(source(), max_pending=3)) as stream:
        first = harness.spawn(anext(stream))
        await wait_until(lambda: batcher.started.qsize() == 3)
        batcher.release(1, 2)
        await wait_until(lambda: batcher.stats().failed == 2)
        assert not first.done()
        assert consumed == [0, 1, 2]
        batcher.release(0)
        assert (await asyncio.wait_for(first, 2)).index == 0
        await wait_until(lambda: len(consumed) == 4)
        assert consumed == [0, 1, 2, 3]
        assert (await anext(stream)).error is batcher.errors[1]
        assert (await anext(stream)).error is batcher.errors[2]
    assert closed == [True]


@pytest.mark.asyncio
async def test_unordered_outcomes_keep_completion_order_while_consumer_pauses(harness):
    batcher = harness.create(ItemGates)
    error = batcher.errors[2] = LookupError("two")
    async with aclosing(batcher.map_outcomes(range(4), max_pending=4, ordered=False)) as stream:
        first = harness.spawn(anext(stream))
        await wait_until(lambda: batcher.started.qsize() == 4)
        batcher.release(3)
        assert (await asyncio.wait_for(first, 2)).index == 3
        batcher.release(2)
        await wait_until(lambda: batcher.stats().failed == 1)
        batcher.release(1)
        await wait_until(lambda: batcher.stats().succeeded == 2)
        batcher.release(0)
        await wait_until(lambda: batcher.stats().succeeded == 3)
        remaining = [outcome async for outcome in stream]
        assert [outcome.index for outcome in remaining] == [2, 1, 0]
        assert remaining[0].item == 2 and remaining[0].error is error


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_source_errors_are_terminal_and_close_owned_tasks(harness, asynchronous):
    batcher = harness.create(ItemGates)
    closed = []
    error = RuntimeError("source failed")

    def source():
        try:
            yield 0
            raise error
        finally:
            closed.append(True)

    async def async_source():
        try:
            yield 0
            await asyncio.sleep(0)
            raise error
        finally:
            closed.append(True)

    stream = batcher.map_outcomes(async_source() if asynchronous else source(), max_pending=2)
    with pytest.raises(RuntimeError, match="source failed") as caught:
        await asyncio.wait_for(anext(stream), 2)
    assert caught.value is error
    assert closed == [True]
    await wait_until(lambda: batcher.stats().pending_items == 0)
    batcher.release(0, 9)
    assert await batcher.process(9) == 18


@pytest.mark.asyncio
async def test_cancelled_consumer_closes_source_and_preserves_unrelated_requests(harness):
    batcher = harness.create(ItemGates)
    unrelated = harness.submit(batcher, 99)[0]
    closed = asyncio.Event()

    async def source():
        try:
            yield 0
            await asyncio.Event().wait()
        finally:
            closed.set()

    first = harness.spawn(anext(batcher.map_outcomes(source(), max_pending=2)))
    await wait_until(lambda: batcher.started.qsize() == 2)
    first.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first
    assert closed.is_set()
    assert not unrelated.done()
    batcher.release(99)
    assert await unrelated == 198


@pytest.mark.asyncio
async def test_item_cancellation_terminates_even_behind_pending_ordered_result(harness):
    batcher = harness.create(ItemGates)
    batcher.errors[1] = asyncio.CancelledError()
    first = harness.spawn(anext(batcher.map_outcomes([0, 1], max_pending=2)))
    await wait_until(lambda: batcher.started.qsize() == 2)
    batcher.release(1)
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(first, 2)
    await wait_until(lambda: batcher.stats().pending_items == 0)


@pytest.mark.asyncio
async def test_ordinary_admission_error_is_correlated_and_later_items_continue(harness):
    batcher = harness.create(
        max_batch_size=2, max_queue_time=0, max_batch_cost=2, item_cost=lambda item: item
    )
    outcomes = [outcome async for outcome in batcher.map_outcomes([1, 3, 2])]
    assert [outcome.index for outcome in outcomes] == [0, 1, 2]
    assert isinstance(outcomes[1].error, ValueError)
    assert outcomes[0].result == 2 and outcomes[2].result == 4
    assert batcher.stats().accepted == 2


@pytest.mark.asyncio
async def test_ready_error_does_not_wait_for_stalled_async_source(harness):
    batcher = harness.create(ItemGates)
    error = batcher.errors[1] = ValueError("one")
    batcher.release(1)
    closed = asyncio.Event()

    async def source():
        try:
            yield 1
            await asyncio.Event().wait()
        finally:
            closed.set()

    async with aclosing(batcher.map_outcomes(source(), max_pending=2)) as stream:
        assert (await asyncio.wait_for(anext(stream), 2)).error is error
    assert closed.is_set()


@pytest.mark.parametrize("limit", [0, -1, True, 1.5, None])
def test_outcome_map_validates_window_before_consuming_source(harness, limit):
    batcher = AsyncBatcher.from_callable(lambda batch: batch)
    with pytest.raises(ValueError, match="max_pending"):
        batcher.map_outcomes([], max_pending=limit)
    with pytest.raises(TypeError, match="boolean"):
        batcher.map_outcomes([], ordered=1)


@pytest.mark.asyncio
async def test_empty_source_has_no_outcomes_or_started_dispatcher(harness):
    batcher = harness.create()
    assert [outcome async for outcome in batcher.map_outcomes([])] == []
    assert not await batcher.is_running()
    assert MapOutcome(0, "input", result=None).succeeded
