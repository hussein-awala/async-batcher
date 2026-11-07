from __future__ import annotations

import asyncio
from collections import Counter

import pytest
from async_batcher import AsyncBatcher, QueueFullException

from tests.core_helpers import GatedBatcher, wait_until


async def retained_head(harness):
    costs = {item: item for item in range(1, 6)}
    evaluated = []

    def item_cost(item):
        evaluated.append(item)
        return costs[item]

    batcher = harness.create(
        GatedBatcher,
        max_batch_cost=5,
        item_cost=item_cost,
        max_queue_size=1,
        max_queue_time=60,
    )
    first = harness.submit(batcher, 4)[0]
    # The collector has taken 4 and is waiting to fill its partial batch.
    await wait_until(lambda: bool(batcher._pending_futures) and batcher._queue.empty())
    second = harness.submit(batcher, 3)[0]
    assert await asyncio.wait_for(batcher.started.get(), 2) == [4]
    assert batcher._queue.empty()
    assert batcher._queued_count() == 1
    return batcher, first, second, costs, evaluated


@pytest.mark.asyncio
async def test_retained_head_blocks_admission_until_consumed_and_cost_is_cached(harness):
    batcher, first, second, costs, evaluated = await retained_head(harness)
    with pytest.raises(QueueFullException):
        await batcher.process(1)
    waiting = harness.spawn(batcher.process(2, wait_for_space=True))
    await wait_until(lambda: 2 in evaluated)
    assert not waiting.done()
    assert len(batcher._pending_futures) == 2
    assert batcher._queue.empty()
    # Waiting must not re-evaluate the previously calculated cost estimate.
    costs[2] = 99
    batcher.release.set()
    assert await asyncio.wait_for(asyncio.gather(first, second, waiting), 2) == [8, 6, 4]
    assert batcher.batches == [[4], [3, 2]]
    assert Counter(evaluated)[2] == 1
    await batcher.stop()
    assert batcher._queued_count() == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("force", [False, True])
async def test_shutdown_clears_retained_capacity_and_rejects_waiting_producer(harness, force):
    batcher, first, second, _, evaluated = await retained_head(harness)
    waiting = harness.spawn(batcher.process(2, wait_for_space=True))
    await wait_until(lambda: 2 in evaluated)
    stopping = harness.spawn(batcher.stop(force=force))
    with pytest.raises(RuntimeError, match="stopping"):
        await asyncio.wait_for(waiting, 2)
    batcher.release.set()
    await asyncio.wait_for(stopping, 2)
    results = await asyncio.gather(first, second, return_exceptions=True)
    if force:
        assert all(isinstance(result, asyncio.CancelledError) for result in results)
    else:
        assert results == [8, 6]
    assert batcher._queued_count() == 0
    assert batcher._next_batch_item is None
    assert not batcher._pending_futures
    assert await asyncio.wait_for(batcher.process(5, wait_for_space=True), 2) == 10
    assert all(2 not in batch for batch in batcher.batches)


@pytest.mark.asyncio
@pytest.mark.parametrize("ordered", [False, True])
async def test_stream_from_callable_combines_count_cost_and_queue_limits(ordered):
    batches = []

    async def processor(batch):
        batches.append(batch)
        await asyncio.sleep(0)
        return batch

    items = [4, 3, 2, 1, 4, 0, 1, 3, 2]
    async with AsyncBatcher.from_callable(
        processor,
        max_batch_size=3,
        max_batch_cost=5,
        item_cost=lambda item: item,
        max_queue_size=2,
        max_queue_time=0,
        concurrency=2,
    ) as batcher:
        async with asyncio.timeout(2):
            results = [item async for item in batcher.map(items, max_pending=4, ordered=ordered)]
    assert results == items if ordered else Counter(results) == Counter(items)
    assert all(len(batch) <= 3 and sum(batch) <= 5 for batch in batches)
    assert not await batcher.is_running()
    assert not batcher._pending_futures
