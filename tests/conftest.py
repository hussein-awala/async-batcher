from __future__ import annotations

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
