from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from contextvars import ContextVar

import pytest
from async_batcher.batcher import AsyncBatcher

_test_value = ContextVar("executor_test_value", default="default")


@pytest.mark.asyncio
@pytest.mark.parametrize("force", [False, True])
async def test_synchronous_processor_cannot_wait_for_its_own_shutdown(harness, force):
    loop = asyncio.get_running_loop()

    class SelfStoppingBatcher(AsyncBatcher):
        def process_batch(self, batch):
            stopping = asyncio.run_coroutine_threadsafe(self.stop(force=force), loop)
            # Bound the regression's worker wait so a failure cannot hang teardown.
            stopping.result(timeout=2)
            return batch

    batcher = harness.create(SelfStoppingBatcher, max_batch_size=1)
    with pytest.raises(RuntimeError, match="stop.*process_batch"):
        await asyncio.wait_for(batcher.process(1), timeout=3)
    assert not batcher._stop.is_set()


@pytest.mark.asyncio
async def test_processor_context_is_copied_without_leaking_to_caller_or_executor(harness):
    class ContextBatcher(AsyncBatcher):
        def process_batch(self, batch):
            inherited = _test_value.get()
            _test_value.set("processor change")
            return [inherited] * len(batch)

    token = _test_value.set("caller value")
    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            batcher = harness.create(ContextBatcher, max_batch_size=1, executor=executor)
            for item in (1, 2):
                assert await asyncio.wait_for(batcher.process(item), timeout=1) == "caller value"
            assert _test_value.get() == "caller value"
            assert await asyncio.get_running_loop().run_in_executor(executor, _test_value.get) == "default"
            await batcher.stop()
    finally:
        _test_value.reset(token)
