from __future__ import annotations

import asyncio
from unittest import mock

import pytest
from async_batcher.batcher import AsyncBatcher


def pytest_runtest_setup(item):
    def _has_marker(item, marker_name: str) -> bool:
        return len(list(item.iter_markers(name=marker_name))) > 0

    markexpr = item.config.getoption("markexpr")
    if markexpr == "":
        if _has_marker(item=item, marker_name="integration"):
            pytest.skip("skipping integration tests")


class MockAsyncBatcher(AsyncBatcher):
    mock_batch_processor = mock.AsyncMock(side_effect=lambda batch: [i * 2 for i in batch])

    async def process_batch(self, *args, **kwargs):
        return await self.mock_batch_processor(*args, **kwargs)


class SlowAsyncBatcher(MockAsyncBatcher):
    def __init__(self, sleep_time: float = 1, **kwargs):
        super().__init__(**kwargs)
        self.sleep_time = sleep_time

    async def process_batch(self, *args, **kwargs):
        await asyncio.sleep(self.sleep_time)
        return await super().process_batch(*args, **kwargs)


@pytest.fixture(scope="function")
def mock_async_batcher():
    batcher = MockAsyncBatcher(
        max_batch_size=10,
        max_queue_time=0.01,
    )
    yield batcher
    asyncio.get_event_loop().run_until_complete(batcher.stop())
    batcher.mock_batch_processor.reset_mock()
