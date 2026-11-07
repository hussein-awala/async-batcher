from __future__ import annotations

import asyncio

import pytest
from async_batcher.batcher import AsyncBatcher


@pytest.mark.asyncio
@pytest.mark.parametrize("force", [False, True])
@pytest.mark.parametrize(
    "eager",
    [
        False,
        pytest.param(
            True,
            marks=pytest.mark.skipif(
                not hasattr(asyncio, "eager_task_factory"), reason="Requires Python 3.12+"
            ),
        ),
    ],
)
async def test_processor_cannot_wait_for_its_own_shutdown(harness, force, eager):
    class SelfStoppingBatcher(AsyncBatcher):
        async def process_batch(self, batch):
            await self.stop(force=force)
            return batch

    loop = asyncio.get_running_loop()
    previous_factory = loop.get_task_factory()
    if eager:
        loop.set_task_factory(asyncio.eager_task_factory)
    try:
        batcher = harness.create(SelfStoppingBatcher, max_batch_size=1)
        with pytest.raises(RuntimeError):
            await asyncio.wait_for(batcher.process(1), timeout=1)
    finally:
        loop.set_task_factory(previous_factory)
