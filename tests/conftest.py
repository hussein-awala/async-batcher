from __future__ import annotations

import asyncio
from unittest import mock

import pytest_asyncio
from async_batcher.batcher import AsyncBatcher

pytest_plugins = ["tests.core_helpers"]


class MockAsyncBatcher(AsyncBatcher):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.mock_batch_processor = mock.AsyncMock(side_effect=lambda batch: [i * 2 for i in batch])

    async def process_batch(self, *args, **kwargs):
        return await self.mock_batch_processor(*args, **kwargs)


class SlowAsyncBatcher(MockAsyncBatcher):
    def __init__(self, sleep_time: float = 1, **kwargs):
        super().__init__(**kwargs)
        self.sleep_time = sleep_time

    async def process_batch(self, *args, **kwargs):
        await asyncio.sleep(self.sleep_time)
        return await super().process_batch(*args, **kwargs)


@pytest_asyncio.fixture
async def mock_async_batcher():
    batcher = MockAsyncBatcher(
        max_batch_size=10,
        max_queue_time=0.01,
    )
    yield batcher
    await batcher.stop()
    batcher.mock_batch_processor.reset_mock()
