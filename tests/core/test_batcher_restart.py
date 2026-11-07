from __future__ import annotations

import asyncio

import pytest
from async_batcher.batcher import AsyncBatcher

from tests.core_helpers import GatedBatcher


@pytest.mark.asyncio
@pytest.mark.parametrize("force", [False, True])
@pytest.mark.parametrize("concurrency", [1, 2, -1])
async def test_repeated_restart_preserves_results_and_concurrency(harness, force, concurrency):
    batcher = harness.create(GatedBatcher, max_batch_size=2, max_queue_time=0, concurrency=concurrency)
    for cycle in range(3):
        batcher.release.clear()
        values = list(range(cycle * 12, (cycle + 1) * 12))
        requests = harness.submit(batcher, *values)
        expected_active = concurrency if concurrency > 0 else 6
        for _ in range(expected_active):
            await asyncio.wait_for(batcher.started.get(), timeout=1)
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(batcher.started.get(), timeout=0.01)
        batcher.release.set()
        assert await asyncio.wait_for(asyncio.gather(*requests), timeout=1) == [x * 2 for x in values]
        await asyncio.wait_for(batcher.stop(force=force), timeout=1)
        assert batcher.max_active == expected_active
        assert not batcher._running_batches
        assert not await batcher.is_running()
        while not batcher.started.empty():
            batcher.started.get_nowait()


@pytest.mark.asyncio
async def test_restart_after_force_stop_does_not_replay_cancelled_items(harness):
    batcher = harness.create(GatedBatcher, max_batch_size=2, max_queue_time=0, concurrency=1)
    old_requests = harness.submit(batcher, *range(6))
    await asyncio.wait_for(batcher.started.get(), timeout=1)
    await asyncio.wait_for(batcher.stop(force=True), timeout=1)
    results = await asyncio.gather(*old_requests, return_exceptions=True)
    assert all(isinstance(result, asyncio.CancelledError) for result in results)

    batcher.release.set()
    new_requests = harness.submit(batcher, 10, 11, 12)
    assert await asyncio.wait_for(asyncio.gather(*new_requests), timeout=1) == [20, 22, 24]
    assert batcher.batches == [[10, 11], [12]]


@pytest.mark.asyncio
@pytest.mark.parametrize("timeout", [False, True])
async def test_restart_rejected_until_graceful_shutdown_finishes(harness, timeout):
    batcher = harness.create(GatedBatcher, max_batch_size=1)
    request = harness.submit(batcher, 1)[0]
    await asyncio.wait_for(batcher.started.get(), timeout=1)
    if timeout:
        with pytest.raises(asyncio.TimeoutError):
            await batcher.stop(timeout=0.01)
        stopping = harness.spawn(batcher.stop())
    else:
        stopping = harness.spawn(batcher.stop())
        await asyncio.sleep(0)
    dispatcher = batcher._current_task
    try:
        with pytest.raises(RuntimeError, match="stopping"):
            await batcher.process(2)
        assert batcher._current_task is dispatcher
        assert not stopping.done()
    finally:
        batcher.release.set()
    await asyncio.wait_for(stopping, timeout=1)
    assert await request == 2
    assert await asyncio.wait_for(batcher.process(3), timeout=1) == 6
    assert batcher.batches == [[1], [3]]


@pytest.mark.asyncio
async def test_restart_rejected_until_forced_cleanup_finishes(harness):
    started = asyncio.Event()
    cleanup_started = asyncio.Event()
    finish_cleanup = asyncio.Event()

    class CleanupBatcher(AsyncBatcher):
        async def process_batch(self, batch):
            if batch == [1]:
                started.set()
                try:
                    await asyncio.Event().wait()
                finally:
                    cleanup_started.set()
                    await finish_cleanup.wait()
            return [item * 2 for item in batch]

    batcher = harness.create(CleanupBatcher, max_batch_size=1)
    request = harness.submit(batcher, 1)[0]
    await asyncio.wait_for(started.wait(), timeout=1)
    stopping = harness.spawn(batcher.stop(force=True))
    await asyncio.wait_for(cleanup_started.wait(), timeout=1)
    try:
        with pytest.raises(RuntimeError, match="stopping"):
            await batcher.process(2)
        assert not stopping.done()
    finally:
        finish_cleanup.set()
    await asyncio.wait_for(stopping, timeout=1)
    with pytest.raises(asyncio.CancelledError):
        await request
    assert await asyncio.wait_for(batcher.process(3), timeout=1) == 6
