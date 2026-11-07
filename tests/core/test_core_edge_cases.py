from __future__ import annotations

import asyncio

import pytest
from async_batcher.batcher import AsyncBatcher
from async_batcher.exceptions import QueueFullException

from tests.core_helpers import GatedBatcher, wait_until


@pytest.mark.asyncio
@pytest.mark.parametrize("synchronous", [False, True])
async def test_processor_cancellation_releases_capacity_for_later_batches(harness, synchronous):
    def process(batch):
        if batch == [1, 2]:
            raise asyncio.CancelledError("upstream operation cancelled")
        return [item * 2 for item in batch]

    class CancelledBatcher(AsyncBatcher):
        if synchronous:

            def process_batch(self, batch):
                return process(batch)
        else:

            async def process_batch(self, batch):
                return process(batch)

    batcher = harness.create(CancelledBatcher, max_batch_size=2, concurrency=1, max_queue_time=0)
    requests = harness.submit(batcher, 1, 2, 3, 4)
    results = await asyncio.wait_for(asyncio.gather(*requests, return_exceptions=True), timeout=2)
    assert all(isinstance(result, asyncio.CancelledError) for result in results[:2])
    assert all(request.cancelled() for request in requests[:2])
    assert results[2:] == [6, 8]
    assert await batcher.is_running()
    await asyncio.wait_for(batcher.stop(), timeout=2)
    assert not await batcher.is_running()


@pytest.mark.asyncio
async def test_force_stop_timeout_keeps_cleanup_alive_and_allows_reuse_afterwards(harness):
    started = asyncio.Event()
    cleanup_started = asyncio.Event()
    release_cleanup = asyncio.Event()
    cleanup_finished = asyncio.Event()

    class CleanupBatcher(AsyncBatcher):
        async def process_batch(self, batch):
            if batch == [1]:
                started.set()
                try:
                    await asyncio.Future()
                finally:
                    cleanup_started.set()
                    await release_cleanup.wait()
                    cleanup_finished.set()
            return [item * 2 for item in batch]

    batcher = harness.create(CleanupBatcher, max_batch_size=1)
    request = harness.submit(batcher, 1)[0]
    try:
        await asyncio.wait_for(started.wait(), timeout=2)
        with pytest.raises(TimeoutError):
            await batcher.stop(force=True, timeout=0.01)
        await asyncio.wait_for(cleanup_started.wait(), timeout=2)
        with pytest.raises(asyncio.CancelledError):
            await request
        assert not cleanup_finished.is_set()
        with pytest.raises(RuntimeError, match="stopping"):
            await batcher.process(2)
        assert await batcher.is_running()
    finally:
        release_cleanup.set()
    await asyncio.wait_for(batcher.stop(), timeout=2)
    assert cleanup_finished.is_set()
    assert not await batcher.is_running()
    assert await asyncio.wait_for(batcher.process(3), timeout=2) == 6


@pytest.mark.asyncio
async def test_queue_rejection_and_queued_cancellation_do_not_corrupt_later_results(harness):
    batcher = harness.create(GatedBatcher, max_batch_size=1, concurrency=1, max_queue_size=2)
    active = harness.submit(batcher, 1)[0]
    await asyncio.wait_for(batcher.started.get(), timeout=2)
    cancelled, peer = harness.submit(batcher, 2, 3)
    await wait_until(lambda: batcher._queue.qsize() == 2)
    with pytest.raises(QueueFullException):
        await batcher.process(4)
    cancelled.cancel()
    with pytest.raises(asyncio.CancelledError):
        await cancelled
    batcher.release.set()
    assert await asyncio.wait_for(asyncio.gather(active, peer), timeout=2) == [2, 6]
    assert await asyncio.wait_for(batcher.process(4), timeout=2) == 8
    await asyncio.wait_for(batcher.stop(), timeout=2)
    # Accepted cancelled work may still execute, but rejection must not enqueue a duplicate.
    assert [item for batch in batcher.batches for item in batch if item != 2] == [1, 3, 4]


@pytest.mark.asyncio
@pytest.mark.parametrize("option", ["sleep_time", "buffering_time"])
async def test_ignored_legacy_timing_options_warn_without_delaying_work(harness, option):
    with pytest.warns(UserWarning, match=f"'{option}' has no effect"):
        batcher = harness.create(max_batch_size=1, **{option: 60})
    assert await asyncio.wait_for(batcher.process(3), timeout=2) == 6


@pytest.mark.asyncio
async def test_legacy_batch_size_alias_still_controls_batch_size(harness):
    with pytest.warns(UserWarning, match="'batch_size' argument is deprecated"):
        batcher = harness.create(max_batch_size=10, batch_size=1, max_queue_time=60)
    assert await asyncio.wait_for(asyncio.gather(*harness.submit(batcher, 1, 2)), timeout=2) == [2, 4]
    assert batcher.batches == [[1], [2]]
