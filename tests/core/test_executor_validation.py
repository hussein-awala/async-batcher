from __future__ import annotations

import asyncio
from concurrent import futures

import pytest
from async_batcher.batcher import AsyncBatcher

from tests.core_helpers import RecordingBatcher


@pytest.mark.parametrize("executor_name", ["ProcessPoolExecutor", "InterpreterPoolExecutor"])
def test_isolated_executor_is_rejected_before_submitting_work(executor_name, monkeypatch):
    executor_type = getattr(futures, executor_name, None)
    if executor_type is None:
        pytest.skip("InterpreterPoolExecutor requires Python 3.14+")

    def unexpected_submit(*args, **kwargs):
        pytest.fail("Invalid executors must be rejected before work is submitted")

    with executor_type(max_workers=1) as executor:
        monkeypatch.setattr(executor, "submit", unexpected_submit)
        with pytest.raises(TypeError, match=executor_name + ".*same interpreter"):
            RecordingBatcher(executor=executor)


@pytest.mark.asyncio
async def test_custom_same_interpreter_executor_remains_supported(harness):
    class CustomExecutor(futures.Executor):
        def __init__(self):
            self.pool = futures.ThreadPoolExecutor(max_workers=1)

        def submit(self, fn, /, *args, **kwargs):
            return self.pool.submit(fn, *args, **kwargs)

        def shutdown(self, wait=True, *, cancel_futures=False):
            self.pool.shutdown(wait=wait, cancel_futures=cancel_futures)

    class SyncBatcher(AsyncBatcher):
        def process_batch(self, batch):
            return [value * 2 for value in batch]

    with CustomExecutor() as executor:
        batcher = harness.create(SyncBatcher, max_batch_size=1, executor=executor)
        assert await asyncio.wait_for(batcher.process(3), timeout=1) == 6
        await batcher.stop()
