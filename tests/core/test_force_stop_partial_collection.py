from __future__ import annotations

import asyncio

import pytest
from async_batcher.batcher import AsyncBatcher


@pytest.mark.asyncio
@pytest.mark.parametrize("concurrency", [1, 2, -1])
async def test_force_stop_cancels_active_worker_while_collecting_next_batch(concurrency):
    started = asyncio.Event()
    release_processing = asyncio.Event()
    cleanup_started = asyncio.Event()
    release_cleanup = asyncio.Event()
    cleanup_finished = asyncio.Event()

    class GatedBatcher(AsyncBatcher):
        async def process_batch(self, batch):
            started.set()
            try:
                await release_processing.wait()
            finally:
                cleanup_started.set()
                await release_cleanup.wait()
                cleanup_finished.set()
            return batch

    batcher = GatedBatcher(max_batch_size=2, max_queue_time=60, concurrency=concurrency)
    requests = [asyncio.create_task(batcher.process(item)) for item in (1, 2)]
    stopping = []
    try:
        await asyncio.wait_for(started.wait(), timeout=1)
        requests.append(asyncio.create_task(batcher.process(3)))
        await asyncio.sleep(0)

        async def wait_for_collection():
            while len(batcher._pending_futures) != 3 or (concurrency != 1 and not batcher._queue.empty()):
                await asyncio.sleep(0)

        await asyncio.wait_for(wait_for_collection(), timeout=1)
        await asyncio.sleep(0)
        stopping.append(asyncio.create_task(batcher.stop(force=True)))
        await asyncio.wait_for(cleanup_started.wait(), timeout=1)
        stopping.append(asyncio.create_task(batcher.stop(force=True)))
        await asyncio.sleep(0)
        assert not cleanup_finished.is_set()
        assert all(not task.done() for task in stopping)

        release_cleanup.set()
        await asyncio.wait_for(asyncio.gather(*stopping), timeout=1)
        results = await asyncio.wait_for(asyncio.gather(*requests, return_exceptions=True), timeout=1)
        assert all(isinstance(result, asyncio.CancelledError) for result in results)
        assert cleanup_finished.is_set()
        assert not batcher._running_batches
        assert not await batcher.is_running()
    finally:
        release_processing.set()
        release_cleanup.set()
        await asyncio.wait_for(batcher.stop(force=True), timeout=1)
        await asyncio.wait_for(asyncio.gather(*stopping, *requests, return_exceptions=True), timeout=1)
