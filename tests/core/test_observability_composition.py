from __future__ import annotations

import asyncio
from collections import Counter

import pytest
from async_batcher import AsyncBatcher, QueueFullException

from tests.core_helpers import GatedBatcher, wait_until


@pytest.mark.asyncio
@pytest.mark.parametrize("force", [False, True])
async def test_retained_cost_head_and_admission_outcomes_are_counted_once(harness, force):
    events = []
    batcher = harness.create(
        GatedBatcher,
        max_batch_cost=5,
        item_cost=lambda item: item,
        max_queue_size=1,
        max_queue_time=60,
        on_batch=events.append,
    )
    first = harness.submit(batcher, 4)[0]
    await wait_until(lambda: batcher.stats().accepted == 1 and batcher._queue.empty())
    second = harness.submit(batcher, 3)[0]
    assert await batcher.started.get() == [4]
    assert batcher._queue.empty()
    stats = batcher.stats()
    assert (stats.queued_items, stats.pending_items, stats.in_flight_items, stats.active_batches) == (
        1,
        2,
        1,
        1,
    )
    with pytest.raises(QueueFullException):
        await batcher.process(1)
    with pytest.raises(TimeoutError):
        await batcher.process(1, wait_for_space=True, enqueue_timeout=0)
    with pytest.raises(ValueError, match="exceeds"):
        await batcher.process(99, wait_for_space=True)
    assert batcher.stats().rejected == 2

    waiting = harness.spawn(batcher.process(1, wait_for_space=True))
    await wait_until(lambda: bool(batcher._queue_space_available._waiters))
    assert batcher.stats().accepted == 2
    waiting.cancel()
    await asyncio.gather(waiting, return_exceptions=True)
    assert (batcher.stats().rejected, batcher.stats().cancelled, batcher.stats().pending_items) == (2, 0, 2)

    interrupted = harness.spawn(batcher.process(1, wait_for_space=True))
    await wait_until(lambda: bool(batcher._queue_space_available._waiters))
    stopping = harness.spawn(batcher.stop(force=force))
    with pytest.raises(RuntimeError, match="stopping"):
        await interrupted
    batcher.release.set()
    await stopping
    await asyncio.gather(first, second, return_exceptions=True)
    final = batcher.stats()
    assert (final.accepted, final.rejected) == (2, 3)
    assert (final.queued_items, final.pending_items, final.active_batches, final.in_flight_items) == (
        0,
        0,
        0,
        0,
    )
    assert (final.succeeded, final.failed, final.cancelled) == ((0, 0, 2) if force else (2, 0, 0))
    assert sum(event.size for event in events) == (1 if force else 2)
    assert final.batches_dispatched == final.batches_completed == len(events)


@pytest.mark.asyncio
@pytest.mark.parametrize("ordered", [False, True])
async def test_async_source_callable_map_cost_and_events_compose(ordered):
    batches = []
    events = []
    items = [4, 3, 2, 1, 4, 0, 1, 3, 2]

    async def source():
        for item in items:
            await asyncio.sleep(0)
            yield item

    async def processor(batch):
        batches.append(batch)
        await asyncio.sleep(0)
        return [item * 2 for item in batch]

    async with AsyncBatcher.from_callable(
        processor,
        max_batch_size=3,
        max_batch_cost=5,
        item_cost=lambda item: item,
        max_queue_size=2,
        max_queue_time=0,
        concurrency=2,
        on_batch=events.append,
    ) as batcher:
        async with asyncio.timeout(2):
            results = [result async for result in batcher.map(source(), max_pending=4, ordered=ordered)]
    expected = [item * 2 for item in items]
    assert results == expected if ordered else Counter(results) == Counter(expected)
    assert all(len(batch) <= 3 and sum(batch) <= 5 for batch in batches)
    assert sum(event.size for event in events) == sum(map(len, batches)) == len(items)
    assert all(event.succeeded == event.size and event.failed == event.cancelled == 0 for event in events)
    final = batcher.stats()
    assert final.accepted == final.succeeded == len(items)
    assert final.rejected == final.failed == final.cancelled == 0
    assert final.batches_dispatched == final.batches_completed == len(events)
    assert (final.pending_items, final.queued_items, final.in_flight_items, final.active_batches) == (
        0,
        0,
        0,
        0,
    )
