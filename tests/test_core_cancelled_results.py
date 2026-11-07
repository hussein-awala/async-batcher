from __future__ import annotations

import asyncio

import pytest

from tests.core_helpers import GatedBatcher


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [False, True])
async def test_cancelled_request_does_not_strand_batch_peers(harness, failure):
    error = ValueError("batch failed") if failure else None
    batcher = harness.create(GatedBatcher, max_batch_size=2, error=error)
    cancelled, peer = harness.submit(batcher, 1, 2)
    await asyncio.wait_for(batcher.started.get(), timeout=1)
    cancelled.cancel()
    with pytest.raises(asyncio.CancelledError):
        await cancelled
    batcher.release.set()

    if failure:
        with pytest.raises(ValueError, match="batch failed"):
            await asyncio.wait_for(peer, timeout=1)
    else:
        assert await asyncio.wait_for(peer, timeout=1) == 4
    await asyncio.wait_for(batcher.stop(), timeout=2)
    assert not batcher._running_batches
