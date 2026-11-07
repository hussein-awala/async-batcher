from __future__ import annotations

import asyncio

import pytest

from tests.conftest import MockAsyncBatcher, SlowAsyncBatcher


@pytest.mark.asyncio
async def test_mock_processors_keep_configuration_and_calls_per_instance():
    first = MockAsyncBatcher(max_batch_size=1)
    second = SlowAsyncBatcher(sleep_time=0, max_batch_size=1)
    first.mock_batch_processor.side_effect = lambda batch: [item + 1 for item in batch]
    try:
        assert await asyncio.gather(first.process(3), second.process(4)) == [4, 8]
        first.mock_batch_processor.assert_awaited_once_with(batch=[3])
        second.mock_batch_processor.assert_awaited_once_with(batch=[4])

        first.mock_batch_processor.reset_mock()
        second.mock_batch_processor.assert_awaited_once_with(batch=[4])
    finally:
        await asyncio.gather(first.stop(), second.stop())
