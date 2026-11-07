from __future__ import annotations

import asyncio

import pytest

from tests.core_helpers import wait_until


@pytest.mark.asyncio
async def test_manual_run_owns_dispatcher_and_rejects_duplicates(harness):
    batcher = harness.create(max_batch_size=2, max_queue_time=0)
    running = harness.spawn(batcher.run())
    await wait_until(lambda: batcher._is_running.is_set())
    with pytest.raises(RuntimeError):
        await asyncio.wait_for(batcher.run(), timeout=0.2)

    requests = harness.submit(batcher, *range(8))
    assert await asyncio.wait_for(asyncio.gather(*requests), timeout=1) == list(range(0, 16, 2))
    assert batcher._current_task is running
    await asyncio.wait_for(batcher.stop(), timeout=2)
    await running
