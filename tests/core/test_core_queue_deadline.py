from __future__ import annotations

import asyncio

import pytest


@pytest.mark.asyncio
async def test_later_arrival_does_not_restart_queue_deadline(harness):
    batcher = harness.create(max_batch_size=10, max_queue_time=0.3)
    first = harness.submit(batcher, 1)[0]
    await asyncio.sleep(0.2)
    second = harness.submit(batcher, 2)[0]

    assert await asyncio.wait_for(asyncio.shield(first), timeout=0.2) == 2
    assert await asyncio.wait_for(second, timeout=1) == 4
    assert batcher.batches == [[1, 2]]
