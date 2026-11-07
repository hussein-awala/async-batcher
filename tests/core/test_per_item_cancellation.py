from __future__ import annotations

import asyncio

import pytest
from async_batcher.batcher import AsyncBatcher


@pytest.mark.asyncio
@pytest.mark.parametrize("cancelled_item", [1, 2])
async def test_gather_cancellation_cancels_only_its_corresponding_request(harness, cancelled_item):
    class GatherBatcher(AsyncBatcher):
        async def process_batch(self, batch):
            async def process_item(item):
                if item == cancelled_item:
                    raise asyncio.CancelledError("item cancelled")
                return item * 2

            return await asyncio.gather(*(process_item(item) for item in batch), return_exceptions=True)

    batcher = harness.create(GatherBatcher, max_batch_size=3, max_queue_time=0)
    requests = harness.submit(batcher, 1, 2, 3)
    outcomes = await asyncio.wait_for(asyncio.gather(*requests, return_exceptions=True), timeout=1)
    for item, request, outcome in zip((1, 2, 3), requests, outcomes, strict=True):
        if item == cancelled_item:
            assert isinstance(outcome, asyncio.CancelledError)
            assert request.cancelled()
        else:
            assert not request.cancelled()
            assert outcome == item * 2
    await batcher.stop()
    assert not batcher._pending_futures
    assert await asyncio.wait_for(batcher.process(4), timeout=1) == 8


@pytest.mark.asyncio
async def test_synchronous_per_item_cancellation_preserves_reason_and_peer_result(harness):
    class SyncBatcher(AsyncBatcher):
        def process_batch(self, batch):
            return [asyncio.CancelledError("item cancelled"), 4]

    batcher = harness.create(SyncBatcher, max_batch_size=2)
    cancelled, peer = harness.submit(batcher, 1, 2)
    with pytest.raises(asyncio.CancelledError, match="item cancelled"):
        await asyncio.wait_for(cancelled, timeout=1)
    assert cancelled.cancelled()
    assert await asyncio.wait_for(peer, timeout=1) == 4
    await batcher.stop()
    assert not batcher._pending_futures
