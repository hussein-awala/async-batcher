from __future__ import annotations

import asyncio

import pytest
from async_batcher.batcher import AsyncBatcher


@pytest.mark.asyncio
@pytest.mark.parametrize("force", [False, True])
async def test_stop_preserves_cleanup_after_public_dispatcher_is_cancelled(force):
    started = asyncio.Event()
    cleanup_started = asyncio.Event()
    release_cleanup = asyncio.Event()
    cleanup_finished = asyncio.Event()

    class GatedBatcher(AsyncBatcher):
        async def process_batch(self, batch):
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                cleanup_started.set()
                await release_cleanup.wait()
                cleanup_finished.set()

    batcher = GatedBatcher(max_batch_size=1)
    dispatcher = asyncio.create_task(batcher.run())
    await asyncio.sleep(0)
    request = asyncio.create_task(batcher.process(1))
    stopping = None
    try:
        await asyncio.wait_for(started.wait(), timeout=1)
        dispatcher.cancel()
        await asyncio.wait_for(cleanup_started.wait(), timeout=1)
        stopping = asyncio.create_task(batcher.stop(force=force))
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(asyncio.shield(stopping), timeout=0.02)
        assert not cleanup_finished.is_set()

        release_cleanup.set()
        await asyncio.wait_for(stopping, timeout=1)
        assert cleanup_finished.is_set()
        with pytest.raises(asyncio.CancelledError):
            await request
        assert not batcher._running_batches
        assert not await batcher.is_running()
    finally:
        release_cleanup.set()
        await asyncio.wait_for(batcher.stop(force=True), timeout=1)
        tasks = [dispatcher, request]
        if stopping is not None:
            tasks.append(stopping)
        await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), timeout=1)
