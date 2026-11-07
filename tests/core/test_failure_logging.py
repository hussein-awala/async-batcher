from __future__ import annotations

import asyncio
import logging

import pytest
from async_batcher import AsyncBatcher, GroupedBatcher

from tests.core_helpers import wait_until

SECRET = "synthetic-private-value"


@pytest.mark.asyncio
@pytest.mark.parametrize("details", [False, True])
async def test_processor_log_policy_keeps_caller_exception(caplog, details):
    original = RuntimeError(SECRET)
    original.__cause__ = ValueError("private chained cause")

    async def fail(batch):
        raise original

    async with AsyncBatcher.from_callable(fail, max_batch_size=1, log_exceptions=details) as batcher:
        with caplog.at_level(logging.ERROR, logger="async_batcher.batcher"):
            with pytest.raises(RuntimeError) as caught:
                await batcher.process(1)
    assert caught.value is original
    assert "Error processing batch" in caplog.text
    assert (SECRET in caplog.text) is details
    assert ("private chained cause" in caplog.text) is details
    assert bool(caplog.records[-1].exc_info) is details


@pytest.mark.asyncio
@pytest.mark.parametrize("details", [False, True])
async def test_observer_failure_log_policy_does_not_change_result(caplog, details):
    def observe(event):
        raise ValueError(SECRET)

    async with AsyncBatcher.from_callable(
        lambda batch: batch, max_batch_size=1, on_batch=observe, log_exceptions=details
    ) as batcher:
        with caplog.at_level(logging.ERROR, logger="async_batcher.batcher"):
            assert await batcher.process(7) == 7
            await batcher.stop()
    assert "Error in on_batch callback" in caplog.text
    assert (SECRET in caplog.text) is details


@pytest.mark.asyncio
@pytest.mark.parametrize("details", [False, True])
async def test_group_cleanup_failure_uses_manager_log_policy(caplog, details):
    class Child(AsyncBatcher):
        fail = True

        async def process_batch(self, batch):
            return batch

        async def stop(self, *args, **kwargs):
            if self.fail:
                raise RuntimeError(SECRET)
            await super().stop(*args, **kwargs)

    child = Child(max_batch_size=1, log_exceptions=False)
    manager = GroupedBatcher(
        lambda key: child, group_key=lambda item: item, idle_timeout=0, log_exceptions=details
    )
    try:
        with caplog.at_level(logging.ERROR, logger="async_batcher.grouped"):
            assert await manager.process(1) == 1
            await wait_until(lambda: "Error stopping an idle batch group" in caplog.text)
        assert (SECRET in caplog.text) is details
    finally:
        child.fail = False
        await asyncio.wait_for(manager.stop(), 1)


@pytest.mark.parametrize("value", [None, 0, "no"])
def test_invalid_logging_policy_is_rejected(value):
    with pytest.raises(TypeError, match="log_exceptions"):
        AsyncBatcher.from_callable(lambda batch: batch, log_exceptions=value)
    with pytest.raises(TypeError, match="log_exceptions"):
        GroupedBatcher(lambda key: None, group_key=lambda item: item, log_exceptions=value)
