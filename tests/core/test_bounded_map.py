from __future__ import annotations

import asyncio
from contextlib import aclosing

import pytest
from async_batcher.batcher import AsyncBatcher

from tests.core_helpers import wait_until


class ItemGates(AsyncBatcher):
    def __init__(self, **kwargs):
        super().__init__(max_batch_size=1, max_queue_time=0, concurrency=-1, **kwargs)
        self.started = asyncio.Queue()
        self.gates = {}
        self.errors = {}

    async def process_batch(self, batch):
        item = batch[0]
        gate = self.gates.setdefault(item, asyncio.Event())
        self.started.put_nowait(item)
        await gate.wait()
        return [self.errors.get(item, item * 2)]

    def release(self, *items):
        for item in items:
            self.gates.setdefault(item, asyncio.Event()).set()


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("ordered", [False, True])
async def test_map_handles_finite_sources_and_bounded_queue(harness, asynchronous, ordered):
    batcher = harness.create(max_batch_size=4, max_queue_size=2, max_queue_time=0)

    async def source():
        for item in range(25):
            yield item

    result = [
        item
        async for item in batcher.map(source() if asynchronous else range(25), max_pending=5, ordered=ordered)
    ]
    assert result == list(range(0, 50, 2)) if ordered else sorted(result) == list(range(0, 50, 2))
    assert await batcher.is_running()
    assert await batcher.process(30) == 60


@pytest.mark.asyncio
@pytest.mark.parametrize("ordered", [False, True])
async def test_completion_order_is_selectable(harness, ordered):
    batcher = harness.create(ItemGates)
    stream = batcher.map(range(3), max_pending=3, ordered=ordered)
    first = harness.spawn(anext(stream))
    for _ in range(3):
        await asyncio.wait_for(batcher.started.get(), 2)
    batcher.release(2)
    if ordered:
        await asyncio.sleep(0)
        assert not first.done()
        batcher.release(0)
        assert await asyncio.wait_for(first, 2) == 0
        batcher.release(1)
        assert [result async for result in stream] == [2, 4]
    else:
        assert await asyncio.wait_for(first, 2) == 4
        batcher.release(1)
        assert await asyncio.wait_for(anext(stream), 2) == 2
        batcher.release(0)
        assert await asyncio.wait_for(anext(stream), 2) == 0
        with pytest.raises(StopAsyncIteration):
            await anext(stream)


@pytest.mark.asyncio
async def test_slow_first_result_and_paused_consumer_keep_source_consumption_bounded(harness):
    consumed = []
    closed = []
    batcher = harness.create(ItemGates)

    def source():
        try:
            for item in range(100):
                consumed.append(item)
                yield item
        finally:
            closed.append(True)

    stream = batcher.map(source(), max_pending=3)
    first = harness.spawn(anext(stream))
    for _ in range(3):
        await asyncio.wait_for(batcher.started.get(), 2)
    batcher.release(1, 2)
    await wait_until(lambda: len(batcher._pending_futures) == 1)
    assert consumed == [0, 1, 2]
    assert not first.done()
    batcher.release(0)
    assert await asyncio.wait_for(first, 2) == 0
    await asyncio.wait_for(batcher.started.get(), 2)
    assert consumed == [0, 1, 2, 3]
    await stream.aclose()
    assert closed == [True]


@pytest.mark.asyncio
async def test_ready_result_does_not_wait_for_slow_async_source(harness):
    batcher = harness.create(max_batch_size=1, max_queue_time=0)
    source_waiting = asyncio.Event()
    source_closed = asyncio.Event()

    async def source():
        try:
            yield 1
            source_waiting.set()
            await asyncio.Event().wait()
        finally:
            source_closed.set()

    async with aclosing(batcher.map(source(), max_pending=2)) as stream:
        assert await asyncio.wait_for(anext(stream), 2) == 2
        await asyncio.wait_for(source_waiting.wait(), 2)
    assert source_closed.is_set()
    assert await batcher.process(2) == 4


@pytest.mark.asyncio
@pytest.mark.parametrize("source_kind", ["sync", "async"])
async def test_source_failure_is_fail_fast_and_closes_pending_requests(harness, source_kind):
    batcher = harness.create(ItemGates)
    closed = []
    error = ValueError("source failed")

    def source():
        try:
            yield 1
            raise error
        finally:
            closed.append(True)

    async def async_source():
        try:
            yield 1
            await asyncio.sleep(0)
            raise error
        finally:
            closed.append(True)

    stream = batcher.map(source() if source_kind == "sync" else async_source())
    with pytest.raises(ValueError, match="source failed") as caught:
        await asyncio.wait_for(anext(stream), 2)
    assert caught.value is error
    assert closed == [True]
    await wait_until(lambda: not batcher._pending_futures)
    batcher.release(1, 2)
    assert await batcher.process(2) == 4


@pytest.mark.asyncio
@pytest.mark.parametrize("ordered", [False, True])
@pytest.mark.parametrize("error", [ValueError("item failed"), asyncio.CancelledError()])
async def test_request_failure_is_fail_fast_even_behind_blocked_first_result(harness, ordered, error):
    batcher = harness.create(ItemGates)
    batcher.errors[1] = error
    stream = batcher.map([0, 1], max_pending=2, ordered=ordered)
    first = harness.spawn(anext(stream))
    for _ in range(2):
        await asyncio.wait_for(batcher.started.get(), 2)
    batcher.release(1)
    with pytest.raises(type(error)):
        await asyncio.wait_for(first, 2)
    await wait_until(lambda: not batcher._pending_futures)
    batcher.release(0, 2)
    assert await batcher.process(2) == 4


@pytest.mark.asyncio
async def test_cancelled_iteration_closes_source_without_cancelling_other_users(harness):
    batcher = harness.create(ItemGates, max_queue_size=1)
    closed = asyncio.Event()

    async def source():
        try:
            for item in range(10):
                yield item
        finally:
            closed.set()

    unrelated = harness.submit(batcher, 99)[0]
    await asyncio.wait_for(batcher.started.get(), 2)
    first = harness.spawn(anext(batcher.map(source(), max_pending=3)))
    await asyncio.wait_for(batcher.started.get(), 2)
    first.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first
    assert closed.is_set()
    assert not unrelated.done()
    assert await batcher.is_running()
    batcher.release(99)
    assert await unrelated == 198


@pytest.mark.asyncio
async def test_early_break_with_aclosing_cleans_stream_but_not_batcher(harness):
    batcher = harness.create(max_batch_size=1, max_queue_time=0)
    closed = []

    def source():
        try:
            yield from range(10)
        finally:
            closed.append(True)

    async with aclosing(batcher.map(source(), max_pending=2)) as stream:
        async for result in stream:
            assert result == 0
            break
    assert closed == [True]
    assert await batcher.process(99) == 198


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_empty_source_does_not_start_dispatcher(harness, asynchronous):
    batcher = harness.create()

    async def empty():
        for item in ():
            yield item

    assert [result async for result in batcher.map(empty() if asynchronous else [])] == []
    assert not await batcher.is_running()
    assert batcher._current_task is None


@pytest.mark.parametrize("limit", [0, -1, True, 1.5, None])
def test_invalid_pending_limit_is_rejected_immediately(limit):
    class Batcher(AsyncBatcher):
        async def process_batch(self, batch):
            return batch

    with pytest.raises(ValueError, match="max_pending"):
        Batcher().map([], max_pending=limit)


@pytest.mark.asyncio
async def test_ordered_flag_and_invalid_source_are_rejected(harness):
    batcher = harness.create()
    with pytest.raises(TypeError, match="boolean"):
        batcher.map([], ordered=1)
    with pytest.raises(TypeError):
        await anext(batcher.map(None))


@pytest.mark.asyncio
async def test_unordered_results_keep_completion_order_while_consumer_is_paused(harness):
    batcher = harness.create(ItemGates)
    async with aclosing(batcher.map(range(4), max_pending=4, ordered=False)) as stream:
        first = harness.spawn(anext(stream))
        for _ in range(4):
            await asyncio.wait_for(batcher.started.get(), 2)
        batcher.release(3)
        assert await asyncio.wait_for(first, 2) == 6
        for item, remaining in [(2, 2), (1, 1), (0, 0)]:
            batcher.release(item)
            await wait_until(lambda remaining=remaining: len(batcher._pending_futures) == remaining)
        assert [result async for result in stream] == [4, 2, 0]


@pytest.mark.asyncio
async def test_close_discards_source_item_returned_after_source_suppresses_cancellation(harness):
    batcher = harness.create(max_batch_size=1, max_queue_time=0)

    class Source:
        def __init__(self):
            self.first = True
            self.waiting = asyncio.Event()
            self.closed = False
            self.cancelled = False

        def __aiter__(self):
            return self

        async def __anext__(self):
            if self.first:
                self.first = False
                return 1
            self.waiting.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                self.cancelled = True
                return 99

        async def aclose(self):
            self.closed = True

    source = Source()
    stream = batcher.map(source, max_pending=2)
    assert await asyncio.wait_for(anext(stream), 2) == 2
    await asyncio.wait_for(source.waiting.wait(), 2)
    await asyncio.wait_for(stream.aclose(), 2)
    assert source.closed and source.cancelled
    assert batcher.batches == [[1]]
    assert await batcher.process(2) == 4


@pytest.mark.asyncio
@pytest.mark.parametrize("ordered", [False, True])
@pytest.mark.parametrize("max_pending", [1, 3, 17])
@pytest.mark.parametrize(
    "eager",
    [
        False,
        pytest.param(
            True,
            marks=pytest.mark.skipif(
                not hasattr(asyncio, "eager_task_factory"), reason="Requires Python 3.12+"
            ),
        ),
    ],
)
async def test_bounded_stream_stress_restarts_cleanly_under_normal_and_eager_scheduling(
    harness, ordered, max_pending, eager
):
    loop = asyncio.get_running_loop()
    previous_factory = loop.get_task_factory()
    original_tasks = asyncio.all_tasks()
    if eager:
        loop.set_task_factory(asyncio.eager_task_factory)
    batcher = harness.create(max_batch_size=3, max_queue_size=2, max_queue_time=0, concurrency=2)
    try:
        for _ in range(3):
            counts = {"consumed": 0, "yielded": 0}

            async def source(counts=counts):
                for item in range(100):
                    counts["consumed"] += 1
                    assert counts["consumed"] - counts["yielded"] <= max_pending
                    if item % 7 == 0:
                        await asyncio.sleep(0)
                    yield item

            results = []
            async with asyncio.timeout(3):
                async for result in batcher.map(source(), max_pending=max_pending, ordered=ordered):
                    counts["yielded"] += 1
                    results.append(result)
                    if counts["yielded"] % 5 == 0:
                        await asyncio.sleep(0)
            assert results == list(range(0, 200, 2)) if ordered else sorted(results) == list(range(0, 200, 2))
            await batcher.stop()
            assert not batcher._pending_futures
            assert not batcher._running_batches
            assert not (asyncio.all_tasks() - original_tasks)
    finally:
        loop.set_task_factory(previous_factory)


@pytest.mark.asyncio
async def test_request_failure_interrupts_pending_async_source_read(harness):
    batcher = harness.create(ItemGates)
    batcher.errors[1] = ValueError("failed while reading source")
    reading = asyncio.Event()
    closed = asyncio.Event()

    async def source():
        try:
            yield 0
            yield 1
            reading.set()
            await asyncio.Event().wait()
        finally:
            closed.set()

    first = harness.spawn(anext(batcher.map(source(), max_pending=3)))
    await asyncio.wait_for(reading.wait(), 2)
    batcher.release(1)
    with pytest.raises(ValueError, match="failed while reading source"):
        await asyncio.wait_for(first, 2)
    assert closed.is_set()
    await wait_until(lambda: not batcher._pending_futures)
    batcher.release(0, 2)
    assert await batcher.process(2) == 4


@pytest.mark.asyncio
async def test_cancellation_during_join_still_closes_source(harness):
    batcher = harness.create(max_batch_size=1, max_queue_time=0)

    class Source:
        def __init__(self):
            self.first = True
            self.reading = asyncio.Event()
            self.cleaning = asyncio.Event()
            self.closed = False

        def __aiter__(self):
            return self

        async def __anext__(self):
            if self.first:
                self.first = False
                return 1
            self.reading.set()
            try:
                await asyncio.Event().wait()
            finally:
                self.cleaning.set()
                await asyncio.Event().wait()

        async def aclose(self):
            self.closed = True

    source = Source()
    stream = batcher.map(source, max_pending=2)
    assert await asyncio.wait_for(anext(stream), 2) == 2
    await asyncio.wait_for(source.reading.wait(), 2)
    closing = harness.spawn(stream.aclose())
    await asyncio.wait_for(source.cleaning.wait(), 2)
    closing.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(closing, 2)
    assert source.closed
    assert await batcher.process(2) == 4


@pytest.mark.asyncio
@pytest.mark.parametrize("ordered", [False, True])
async def test_failure_while_consumer_pauses_precedes_buffered_successes(harness, ordered):
    failure_seen = asyncio.Event()

    class ObservedGates(ItemGates):
        async def process(self, item, **kwargs):
            try:
                return await super().process(item, **kwargs)
            except ValueError:
                failure_seen.set()
                raise

    batcher = harness.create(ObservedGates)
    error = batcher.errors[7] = ValueError("failure behind buffered successes")
    stream = batcher.map(range(8), max_pending=8, ordered=ordered)
    async with aclosing(stream):
        first = harness.spawn(anext(stream))
        await wait_until(lambda: batcher.started.qsize() == 8)
        batcher.release(0)
        assert await asyncio.wait_for(first, 2) == 0
        batcher.release(1, 2, 3, 4, 5, 6)
        await wait_until(lambda: batcher.stats().succeeded == 7)
        batcher.release(7)
        await asyncio.wait_for(failure_seen.wait(), 2)
        with pytest.raises(ValueError, match="buffered successes") as caught:
            await anext(stream)
        assert caught.value is error
    assert batcher.stats().pending_items == 0
    batcher.release(9)
    assert await batcher.process(9) == 18
