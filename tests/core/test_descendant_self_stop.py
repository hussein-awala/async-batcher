from __future__ import annotations

import asyncio

import pytest
from async_batcher.batcher import AsyncBatcher


@pytest.mark.asyncio
@pytest.mark.parametrize("force", [False, True])
@pytest.mark.parametrize("schedule", ["task", "gather"])
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
async def test_processor_cannot_await_stop_through_child_task(harness, force, schedule, eager):
    class SelfStoppingBatcher(AsyncBatcher):
        async def process_batch(self, batch):
            if schedule == "task":
                await asyncio.create_task(self.stop(force=force))
            else:
                await asyncio.gather(self.stop(force=force))
            return batch

    loop = asyncio.get_running_loop()
    previous_factory = loop.get_task_factory()
    if eager:
        loop.set_task_factory(asyncio.eager_task_factory)
    try:
        batcher = harness.create(SelfStoppingBatcher, max_batch_size=1)
        with pytest.raises(RuntimeError, match="stop.*process_batch"):
            await asyncio.wait_for(batcher.process(1), timeout=1)
        assert not batcher._stop.is_set()
    finally:
        loop.set_task_factory(previous_factory)


@pytest.mark.asyncio
async def test_processor_child_can_stop_after_its_processor_finishes(harness):
    allow_stop = asyncio.Event()
    children = []

    class DeferredStoppingBatcher(AsyncBatcher):
        async def process_batch(self, batch):
            async def stop_later():
                await allow_stop.wait()
                await self.stop()

            children.append(harness.spawn(stop_later()))
            return batch

    batcher = harness.create(DeferredStoppingBatcher, max_batch_size=1)
    assert await asyncio.wait_for(batcher.process(1), timeout=1) == 1
    allow_stop.set()
    await asyncio.wait_for(children[0], timeout=1)
    assert not await batcher.is_running()


@pytest.mark.asyncio
async def test_processor_child_can_stop_an_unrelated_batcher(harness):
    other = harness.create(max_batch_size=1)
    assert await other.process(1) == 2

    class OtherStoppingBatcher(AsyncBatcher):
        async def process_batch(self, batch):
            await asyncio.create_task(other.stop())
            return batch

    batcher = harness.create(OtherStoppingBatcher, max_batch_size=1)
    assert await asyncio.wait_for(batcher.process(2), timeout=1) == 2
    assert not await other.is_running()
