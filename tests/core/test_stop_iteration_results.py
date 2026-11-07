from __future__ import annotations

import asyncio

import pytest
from async_batcher.batcher import AsyncBatcher


@pytest.mark.asyncio
@pytest.mark.parametrize("processor_kind", ["sync", "async"])
@pytest.mark.parametrize("failure_kind", ["raised", "returned"])
async def test_stop_iteration_does_not_strand_requests_or_fail_batch_peers(
    harness, processor_kind, failure_kind
):
    error = StopIteration("iterator exhausted")

    class SyncBatcher(AsyncBatcher):
        def process_batch(self, batch):
            return self.results(batch)

        def results(self, batch):
            if 1 not in batch:
                return [item * 2 for item in batch]
            if failure_kind == "raised":
                raise error
            return [error, 4]

    class AsyncProcessor(SyncBatcher):
        async def process_batch(self, batch):
            return self.results(batch)

    processor = SyncBatcher if processor_kind == "sync" else AsyncProcessor
    batcher = harness.create(processor, max_batch_size=2, max_queue_time=0)
    requests = harness.submit(batcher, 1, 2)
    results = await asyncio.wait_for(asyncio.gather(*requests, return_exceptions=True), timeout=1)

    assert isinstance(results[0], RuntimeError)
    assert results[0].__cause__ is error
    if failure_kind == "raised":
        assert results[1] is results[0]
    else:
        assert results[1] == 4
    await asyncio.wait_for(batcher.stop(), timeout=1)
    assert not batcher._running_batches
    assert await asyncio.wait_for(batcher.process(3), timeout=1) == 6
