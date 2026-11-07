from __future__ import annotations

import asyncio

import pytest_asyncio
from async_batcher.batcher import AsyncBatcher


class RecordingBatcher(AsyncBatcher[int, int]):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.batches = []

    async def process_batch(self, batch):
        self.batches.append(batch)
        return [item * 2 for item in batch]


class GatedBatcher(RecordingBatcher):
    def __init__(self, *, error=None, **kwargs):
        super().__init__(**kwargs)
        self.started = asyncio.Queue()
        self.release = asyncio.Event()
        self.error = error
        self.active = 0
        self.max_active = 0

    async def process_batch(self, batch):
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        self.started.put_nowait(batch)
        try:
            await self.release.wait()
            if self.error is not None:
                raise self.error
            return await super().process_batch(batch)
        finally:
            self.active -= 1


class BatcherHarness:
    def __init__(self):
        self.batchers = []
        self.tasks = []

    def create(self, cls=RecordingBatcher, **kwargs):
        batcher = cls(**kwargs)
        self.batchers.append(batcher)
        return batcher

    def spawn(self, coroutine):
        task = asyncio.create_task(coroutine)
        self.tasks.append(task)
        return task

    def submit(self, batcher, *items):
        return [self.spawn(batcher.process(item)) for item in items]

    async def close(self):
        tasks = list(self.tasks)
        for batcher in self.batchers:
            tasks.extend(batcher._running_batches.values())
            if batcher._current_task is not None:
                tasks.append(batcher._current_task)
            await asyncio.wait_for(batcher.stop(force=True), timeout=1)
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), timeout=1)


@pytest_asyncio.fixture
async def harness():
    instance = BatcherHarness()
    yield instance
    await instance.close()


async def wait_until(predicate):
    async def wait():
        while not predicate():
            await asyncio.sleep(0)

    await asyncio.wait_for(wait(), timeout=1)
