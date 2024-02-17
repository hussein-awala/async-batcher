from __future__ import annotations

from unittest import mock

import pytest
from async_batcher.batcher import AsyncBatcher


class MockAsyncBatcher(AsyncBatcher):
    mock_batch_processor = mock.AsyncMock(side_effect=lambda batch: [i * 2 for i in batch])

    async def process_batch(self, *args, **kwargs):
        return await self.mock_batch_processor(*args, **kwargs)


@pytest.fixture(scope="function")
def mock_async_batcher():
    batcher = MockAsyncBatcher(
        batch_size=10,
        sleep_time=0.01,
        buffering_time=0.2,
    )
    batcher.start()
    yield batcher
    batcher.stop()
    batcher.mock_batch_processor.reset_mock()
