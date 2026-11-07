from __future__ import annotations

import asyncio
import gc

import pytest

from tests.core_helpers import RecordingBatcher


@pytest.mark.asyncio
async def test_cancelled_public_run_before_dispatcher_start_allows_clean_restart():
    batcher = RecordingBatcher(max_batch_size=1)
    running = asyncio.create_task(batcher.run())
    try:
        await asyncio.sleep(0)
        assert batcher._current_task is not None
        assert not await batcher.is_running()
        running.cancel()
        with pytest.raises(asyncio.CancelledError):
            await running
        assert batcher.stats().state == "stopped"
        assert await asyncio.wait_for(batcher.process(21), 1) == 42
        await asyncio.wait_for(batcher.stop(), 1)
        assert batcher.stats().pending_items == batcher.stats().queued_items == 0
    finally:
        await batcher.stop(force=True)
        await asyncio.gather(running, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("discard", [False, True])
async def test_cancelled_auto_start_caller_preserves_dispatcher_ownership(discard):
    batcher = RecordingBatcher(max_batch_size=1, discard_cancelled=discard)
    request = asyncio.create_task(batcher.process(21))
    try:
        await asyncio.sleep(0)
        assert batcher._current_task is not None
        assert not await batcher.is_running()
        request.cancel()
        with pytest.raises(asyncio.CancelledError):
            await request
        await asyncio.wait_for(batcher.flush(), 1)
        await asyncio.wait_for(batcher.stop(), 1)
        assert batcher.batches == ([] if discard else [[21]])
        assert batcher.stats().cancelled == 1
        assert batcher.stats().pending_items == batcher.stats().queued_items == 0
    finally:
        await batcher.stop(force=True)
        await asyncio.gather(request, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("entrypoint", ["process", "run"])
async def test_refused_dispatcher_creation_does_not_accept_or_replay_work(entrypoint, recwarn):
    loop = asyncio.get_running_loop()
    previous = loop.get_task_factory()
    batcher = RecordingBatcher(max_batch_size=1)

    def refuse_dispatcher(loop, coroutine, **kwargs):
        if coroutine.__name__ == "_run":
            raise RuntimeError("dispatcher creation refused")
        return asyncio.Task(coroutine, loop=loop, **kwargs)

    try:
        loop.set_task_factory(refuse_dispatcher)
        with pytest.raises(RuntimeError, match="dispatcher creation refused"):
            await (batcher.process(7) if entrypoint == "process" else batcher.run())
        loop.set_task_factory(previous)
        gc.collect()
        assert not [warning for warning in recwarn if issubclass(warning.category, RuntimeWarning)]
        assert batcher.stats().accepted == batcher.stats().pending_items == batcher.stats().queued_items == 0
        await asyncio.wait_for(batcher.stop(), 1)
        assert await asyncio.wait_for(batcher.process(8), 1) == 16
        assert batcher.batches == [[8]]
    finally:
        loop.set_task_factory(previous)
        await batcher.stop(force=True)
