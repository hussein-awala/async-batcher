from __future__ import annotations

import asyncio

import pytest
from async_batcher import AsyncBatcher

from tests.core_helpers import GatedBatcher, wait_until


@pytest.mark.asyncio
@pytest.mark.parametrize("discard", [False, True])
async def test_queued_cancellation_is_opt_in_and_does_not_interrupt_active_work(harness, discard):
    events = []
    batcher = harness.create(
        GatedBatcher, max_batch_size=1, discard_cancelled=discard, on_batch=events.append
    )
    running = harness.submit(batcher, 1)[0]
    await batcher.started.get()
    cancelled, kept = harness.submit(batcher, 2, 3)
    await wait_until(lambda: batcher.stats().accepted == 3)
    running.cancel()
    cancelled.cancel()
    await asyncio.gather(running, cancelled, return_exceptions=True)
    batcher.release.set()
    await asyncio.wait_for(batcher.flush(), 1)
    assert await kept == 6
    assert batcher.batches == ([[1], [3]] if discard else [[1], [2], [3]])
    assert sum(event.size for event in events) == (2 if discard else 3)
    assert batcher.stats().accepted == 3 and batcher.stats().cancelled == 2
    assert not batcher._work_sequences


@pytest.mark.asyncio
@pytest.mark.parametrize("cancel_all", [False, True])
async def test_cancelled_partial_batch_is_filtered_before_dispatch_and_flush_finishes(harness, cancel_all):
    events = []
    batcher = harness.create(
        max_batch_size=8, max_queue_time=3600, discard_cancelled=True, on_batch=events.append
    )
    first, second = harness.submit(batcher, 1, 2)
    await wait_until(lambda: batcher.stats().accepted == 2 and batcher.stats().queued_items == 0)
    first.cancel()
    if cancel_all:
        second.cancel()
    await asyncio.wait_for(batcher.flush(), 1)
    results = await asyncio.gather(first, second, return_exceptions=True)
    assert isinstance(results[0], asyncio.CancelledError)
    if cancel_all:
        assert isinstance(results[1], asyncio.CancelledError)
        assert not batcher.batches and not events
    else:
        assert results[1] == 4 and batcher.batches == [[2]]
        assert events[0].size == 1 and events[0].cancelled == 0
    # No semaphore leak when the entire collected batch was discarded.
    later = harness.submit(batcher, 3)[0]
    await wait_until(lambda: batcher.stats().accepted == 3)
    await asyncio.wait_for(batcher.flush(), 1)
    assert await later == 6


@pytest.mark.asyncio
async def test_cancelled_cost_carryover_frees_capacity_and_does_not_consume_budget(harness):
    batcher = harness.create(
        GatedBatcher,
        max_batch_cost=4,
        item_cost=lambda item: item,
        max_queue_size=2,
        max_queue_time=3600,
        discard_cancelled=True,
    )
    first, cancelled = harness.submit(batcher, 3, 2)
    await batcher.started.get()
    assert batcher._next_batch_item.item == 2
    cancelled.cancel()
    later = harness.submit(batcher, 1)[0]
    await wait_until(lambda: batcher.stats().accepted == 3)
    batcher.release.set()
    await asyncio.wait_for(batcher.flush(), 1)
    assert await asyncio.gather(first, later) == [6, 2]
    await asyncio.gather(cancelled, return_exceptions=True)
    assert batcher.batches == [[3], [1]]
    assert not batcher._queue_is_full() and not batcher._work_sequences


@pytest.mark.asyncio
async def test_skipped_flush_boundary_does_not_collect_later_request(harness):
    batcher = harness.create(GatedBatcher, max_batch_size=2, max_queue_time=3600, discard_cancelled=True)
    first = harness.submit(batcher, 0, 1)
    await batcher.started.get()
    keep, cancelled = harness.submit(batcher, 2, 3)
    await wait_until(lambda: batcher.stats().accepted == 4)
    flushing = harness.spawn(batcher.flush())
    await wait_until(lambda: bool(batcher._flush_targets))
    cancelled.cancel()
    later = harness.submit(batcher, 4)[0]
    await wait_until(lambda: batcher.stats().accepted == 5)
    batcher.release.set()
    await asyncio.wait_for(flushing, 1)
    assert await asyncio.gather(*first, keep) == [0, 2, 4]
    assert batcher.batches == [[0, 1], [2]]
    assert not later.done()
    await batcher.stop()
    assert await later == 8
    await asyncio.gather(cancelled, return_exceptions=True)


@pytest.mark.asyncio
async def test_cancelled_only_queue_releases_waiting_admission_without_empty_call(harness):
    batcher = harness.create(GatedBatcher, max_batch_size=1, max_queue_size=1, discard_cancelled=True)
    harness.submit(batcher, 1)
    await batcher.started.get()
    cancelled = harness.submit(batcher, 2)[0]
    await wait_until(lambda: batcher.stats().accepted == 2)
    waiting = harness.spawn(batcher.process(3, wait_for_space=True))
    await wait_until(lambda: batcher.stats().admission_waiters == 1)
    cancelled.cancel()
    batcher.release.set()
    assert await asyncio.wait_for(waiting, 1) == 6
    await batcher.flush()
    assert batcher.batches == [[1], [3]]
    await asyncio.gather(cancelled, return_exceptions=True)


@pytest.mark.parametrize("value", [0, 1, None, "yes"])
def test_discard_policy_requires_boolean(value):
    with pytest.raises(TypeError, match="discard_cancelled"):
        AsyncBatcher.from_callable(lambda batch: batch, discard_cancelled=value)
