from __future__ import annotations

import asyncio
from fractions import Fraction
from unittest.mock import Mock

import pytest
from async_batcher import AsyncBatcher, QueueFullException


class RecordingBatcher(AsyncBatcher):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.batches = []

    async def process_batch(self, batch):
        self.batches.append(batch)
        return batch


class GatedBatcher(RecordingBatcher):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def process_batch(self, batch):
        self.batches.append(batch)
        self.started.set()
        await self.release.wait()
        return batch


async def wait_until(predicate):
    async with asyncio.timeout(2):
        while not predicate():
            await asyncio.sleep(0)


async def cleanup(batcher, tasks):
    try:
        await batcher.stop(force=True, timeout=2)
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


@pytest.mark.parametrize("limit", [True, False, 0, -1, float("inf"), float("nan"), "2", 1j, 10**1000])
def test_invalid_budget_is_rejected(limit):
    with pytest.raises(ValueError, match="max_batch_cost must be a finite, positive real number"):
        RecordingBatcher(max_batch_cost=limit, item_cost=len)


@pytest.mark.parametrize("kwargs", [{"max_batch_cost": 1}, {"item_cost": len}])
def test_budget_and_cost_function_must_be_configured_together(kwargs):
    with pytest.raises(ValueError, match="configured together"):
        RecordingBatcher(**kwargs)


def test_cost_function_must_be_synchronous_callable():
    async def asynchronous_cost(item):
        return 1

    for callback in (3, asynchronous_cost):
        with pytest.raises(TypeError, match="synchronous callable"):
            RecordingBatcher(max_batch_cost=1, item_cost=callback)


@pytest.mark.asyncio
@pytest.mark.parametrize("cost", [True, False, -1, float("inf"), float("nan"), "2", None, 1j, 10**1000])
async def test_invalid_cost_rejects_only_submission_before_admission(cost):
    callback = Mock(side_effect=[cost, 1])
    batcher = RecordingBatcher(max_batch_cost=2, item_cost=callback, max_queue_time=0)
    try:
        with pytest.raises(ValueError, match="item_cost result must be a finite, nonnegative real number"):
            await batcher.process("invalid")
        assert not batcher._pending_futures
        assert batcher._queued_count() == 0
        assert batcher._current_task is None
        assert await batcher.process("valid") == "valid"
        assert batcher.batches == [["valid"]]
    finally:
        await cleanup(batcher, [])


@pytest.mark.asyncio
async def test_accidentally_async_callable_result_is_closed_and_rejected():
    class AsyncCost:
        async def __call__(self, item):
            return 1

    batcher = RecordingBatcher(max_batch_cost=2, item_cost=AsyncCost())
    with pytest.raises(ValueError, match="item_cost result"):
        await batcher.process("invalid")
    assert not batcher._pending_futures
    assert batcher._current_task is None


@pytest.mark.asyncio
async def test_oversized_item_and_callback_error_do_not_poison_accepted_neighbors():
    error = LookupError("cannot estimate")

    def cost(item):
        if item == "error":
            raise error
        return len(item)

    batcher = RecordingBatcher(max_batch_cost=4, item_cost=cost, max_queue_time=0)
    try:
        result = await asyncio.gather(
            *(batcher.process(item) for item in ("ab", "large", "error", "cd")), return_exceptions=True
        )
        assert result[0] == "ab" and result[3] == "cd"
        assert isinstance(result[1], ValueError)
        assert str(result[1]) == "item_cost result exceeds max_batch_cost"
        assert result[2] is error
        assert batcher.batches == [["ab", "cd"]]
    finally:
        await cleanup(batcher, [])


@pytest.mark.asyncio
@pytest.mark.parametrize("concurrency", [1, 2, -1])
@pytest.mark.parametrize(
    "count, costs, expected",
    [
        (-1, [4, 4, 2, 5, 1], [[4], [4, 2], [5, 1]]),
        (2, [1, 1, 1, 1, 1], [[1, 1], [1, 1], [1]]),
        (1, [0, 3, 0], [[0], [3], [0]]),
        (2, [0, 0, 0], [[0, 0], [0]]),
        (-1, [0, 0, 0], [[0, 0, 0]]),
        (-1, [Fraction(1, 3)] * 3, [[Fraction(1, 3)] * 3]),
    ],
)
async def test_count_and_cost_limits_preserve_fifo_without_losing_carryover(
    concurrency, count, costs, expected
):
    callback = Mock(side_effect=lambda item: item)
    batcher = RecordingBatcher(
        max_batch_size=count,
        max_batch_cost=6,
        item_cost=callback,
        max_queue_time=0,
        concurrency=concurrency,
    )
    try:
        assert await asyncio.gather(*(batcher.process(cost) for cost in costs)) == costs
        assert batcher.batches == expected
        assert callback.call_count == len(costs)
        assert all(sum(batch) <= 6 for batch in batcher.batches)
        assert batcher._next_batch_item is None
    finally:
        await cleanup(batcher, [])


@pytest.mark.asyncio
async def test_exact_budget_dispatches_immediately_and_floating_sum_stays_under_limit():
    batcher = RecordingBatcher(max_batch_cost=0.3, item_cost=lambda item: item, max_queue_time=60)
    tasks = [asyncio.create_task(batcher.process(value)) for value in (0.1, 0.2, 0.1)]
    try:
        # 0.1 + 0.2 exceeds 0.3 in floating arithmetic, so they cannot share a batch.
        await wait_until(lambda: len(batcher.batches) == 2)
        assert batcher.batches == [[0.1], [0.2]]
        await batcher.stop(timeout=2)
        assert await asyncio.gather(*tasks) == [0.1, 0.2, 0.1]
        assert all(sum(batch) <= 0.3 for batch in batcher.batches)
    finally:
        await cleanup(batcher, tasks)

    exact = RecordingBatcher(max_batch_cost=3, item_cost=lambda item: item, max_queue_time=60)
    try:
        assert await asyncio.wait_for(exact.process(3), 2) == 3
    finally:
        await cleanup(exact, [])


@pytest.mark.asyncio
async def test_cost_is_snapshotted_once_even_if_item_changes_while_retained():
    callback = Mock(side_effect=lambda item: item["cost"])
    items = [{"cost": value} for value in (4, 4, 2)]
    batcher = GatedBatcher(max_batch_cost=6, item_cost=callback, max_queue_time=0)
    tasks = [asyncio.create_task(batcher.process(item)) for item in items]
    try:
        await asyncio.wait_for(batcher.started.wait(), 2)
        assert batcher._next_batch_item.item is items[1]
        items[1]["cost"] = 100
        batcher.release.set()
        assert await asyncio.wait_for(asyncio.gather(*tasks), 2) == items
        assert batcher.batches == [[items[0]], items[1:]]
        assert callback.call_count == 3
    finally:
        await cleanup(batcher, tasks)


@pytest.mark.asyncio
async def test_retained_request_keeps_its_original_collection_deadline():
    batcher = GatedBatcher(max_batch_cost=6, item_cost=lambda item: item, max_queue_time=60)
    tasks = [asyncio.create_task(batcher.process(value)) for value in (4, 3)]
    try:
        await asyncio.wait_for(batcher.started.wait(), 2)
        retained = batcher._next_batch_item
        assert retained.item == 3
        # Simulate the original enqueue deadline expiring while a processor was busy.
        batcher._next_batch_item = retained._replace(enqueued_at=asyncio.get_running_loop().time() - 61)
        batcher.release.set()
        assert await asyncio.wait_for(asyncio.gather(*tasks), 2) == [4, 3]
        assert batcher.batches == [[4], [3]]
    finally:
        await cleanup(batcher, tasks)


@pytest.mark.asyncio
async def test_retained_request_counts_toward_bounded_queue_capacity():
    batcher = GatedBatcher(max_batch_cost=6, item_cost=lambda item: item, max_queue_time=0, max_queue_size=2)
    tasks = [asyncio.create_task(batcher.process(value)) for value in (4, 3)]
    try:
        await asyncio.wait_for(batcher.started.wait(), 2)
        assert batcher._next_batch_item.item == 3
        tasks.append(asyncio.create_task(batcher.process(2)))
        await wait_until(lambda: batcher._queued_count() == 2)
        assert batcher._queue.qsize() == 1
        with pytest.raises(QueueFullException):
            await batcher.process(1)
        batcher.release.set()
        assert await asyncio.wait_for(asyncio.gather(*tasks), 2) == [4, 3, 2]
        assert batcher.batches == [[4], [3, 2]]
    finally:
        await cleanup(batcher, tasks)


@pytest.mark.asyncio
async def test_graceful_shutdown_drains_retained_item_and_restart_has_no_stale_cost():
    batcher = GatedBatcher(max_batch_cost=6, item_cost=lambda item: item, max_queue_time=60)
    tasks = [asyncio.create_task(batcher.process(value)) for value in (4, 3)]
    try:
        await asyncio.wait_for(batcher.started.wait(), 2)
        assert batcher._next_batch_item.item == 3
        shutdown = asyncio.create_task(batcher.stop(timeout=2))
        tasks.append(shutdown)
        await wait_until(batcher._stop.is_set)
        batcher.release.set()
        assert await asyncio.gather(*tasks) == [4, 3, None]
        assert batcher.batches == [[4], [3]]
        assert await asyncio.wait_for(batcher.process(6), 2) == 6
        assert batcher.batches == [[4], [3], [6]]
    finally:
        await cleanup(batcher, tasks)


@pytest.mark.asyncio
@pytest.mark.parametrize("force", [False, True])
async def test_cancelled_retained_request_is_drained_or_cleared_without_losing_later_items(force):
    batcher = GatedBatcher(max_batch_cost=6, item_cost=lambda item: item, max_queue_time=0)
    tasks = [asyncio.create_task(batcher.process(value)) for value in (4, 3, 2)]
    try:
        await asyncio.wait_for(batcher.started.wait(), 2)
        tasks[1].cancel()
        if force:
            await batcher.stop(force=True, timeout=2)
            results = await asyncio.gather(*tasks, return_exceptions=True)
            assert all(isinstance(result, asyncio.CancelledError) for result in results)
            assert batcher.batches == [[4]]
        else:
            batcher.release.set()
            results = await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), 2)
            assert results[0] == 4 and results[2] == 2
            assert isinstance(results[1], asyncio.CancelledError)
            assert batcher.batches == [[4], [3, 2]]
            await batcher.stop(timeout=2)
        assert batcher._next_batch_item is None
        assert batcher._queued_count() == 0
        batcher.release.set()
        assert await asyncio.wait_for(batcher.process(6), 2) == 6
    finally:
        await cleanup(batcher, tasks)


@pytest.mark.asyncio
async def test_direct_dispatcher_cancellation_clears_carryover_before_restart():
    batcher = GatedBatcher(max_batch_cost=6, item_cost=lambda item: item, max_queue_time=0)
    tasks = [asyncio.create_task(batcher.process(value)) for value in (4, 3, 2)]
    try:
        await asyncio.wait_for(batcher.started.wait(), 2)
        assert batcher._next_batch_item.item == 3
        batcher._current_task.cancel()
        await asyncio.gather(batcher._current_task, return_exceptions=True)
        results = await asyncio.gather(*tasks, return_exceptions=True)
        assert all(isinstance(result, asyncio.CancelledError) for result in results)
        assert batcher._next_batch_item is None
        assert batcher._queued_count() == 0
        batcher.release.set()
        assert await asyncio.wait_for(batcher.process(6), 2) == 6
        assert batcher.batches == [[4], [6]]
    finally:
        await cleanup(batcher, tasks)


@pytest.mark.asyncio
@pytest.mark.parametrize("concurrency", [2, -1])
async def test_force_stop_cancels_retained_item_after_it_moves_into_partial_collection(concurrency):
    batcher = GatedBatcher(
        max_batch_cost=6, item_cost=lambda item: item, max_queue_time=60, concurrency=concurrency
    )
    tasks = [asyncio.create_task(batcher.process(value)) for value in (4, 3)]
    try:
        await asyncio.wait_for(batcher.started.wait(), 2)
        await wait_until(lambda: batcher._queued_count() == 0)
        assert batcher.batches == [[4]]
        await batcher.stop(force=True, timeout=2)
        results = await asyncio.gather(*tasks, return_exceptions=True)
        assert all(isinstance(result, asyncio.CancelledError) for result in results)
        assert not batcher._pending_futures
        batcher.release.set()
        assert await asyncio.wait_for(batcher.process(6), 2) == 6
        assert batcher.batches == [[4], [6]]
    finally:
        await cleanup(batcher, tasks)
