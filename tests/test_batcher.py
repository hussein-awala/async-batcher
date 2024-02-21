from __future__ import annotations

import asyncio

import pytest

from tests.conftest import MockAsyncBatcher


class CallsMaker:
    """Calls the batcher with a range of items after a sleep time.

    This class is used to simulate multiple calls to the batcher with a range of items
    without blocking the main thread.
    """

    result = None

    def __init__(self, batcher, sleep_time, start_range, end_range):
        super().__init__()
        self.batcher = batcher
        self.sleep_time = sleep_time
        self.start_range = start_range
        self.end_range = end_range

    async def arun(self):
        await asyncio.sleep(self.sleep_time)
        result = await asyncio.gather(
            *[self.batcher.process(item=i) for i in range(self.start_range, self.end_range)]
        )
        self.result = result


@pytest.mark.asyncio
async def test_process_batch(mock_async_batcher):
    result = await asyncio.gather(*[mock_async_batcher.process(item=i) for i in range(10)])

    assert mock_async_batcher.mock_batch_processor.call_count == 1
    assert mock_async_batcher.mock_batch_processor.mock_calls[0].kwargs["batch"] == list(range(10))
    assert result == [i * 2 for i in range(10)]


@pytest.mark.asyncio
async def test_process_batch_with_bigger_buffer(mock_async_batcher):
    result = await asyncio.gather(*[mock_async_batcher.process(item=i) for i in range(25)])

    assert mock_async_batcher.mock_batch_processor.call_count == 3
    assert mock_async_batcher.mock_batch_processor.mock_calls[0].kwargs["batch"] == list(range(10))
    assert mock_async_batcher.mock_batch_processor.mock_calls[1].kwargs["batch"] == list(range(10, 20))
    assert mock_async_batcher.mock_batch_processor.mock_calls[2].kwargs["batch"] == list(range(20, 25))
    assert result == [i * 2 for i in range(25)]


@pytest.mark.asyncio
async def test_process_batch_with_short_buffering_time():
    batcher = MockAsyncBatcher(
        max_batch_size=10,
        max_queue_time=0.2,
    )

    calls_maker1 = CallsMaker(batcher, 0, 0, 5)
    calls_maker2 = CallsMaker(batcher, 0.2, 5, 20)
    calls_maker3 = CallsMaker(batcher, 0.4, 20, 30)
    await asyncio.gather(calls_maker1.arun(), calls_maker2.arun(), calls_maker3.arun())

    assert batcher.mock_batch_processor.call_count == 4
    # the first range of size 5 should be processed in a single batch
    assert batcher.mock_batch_processor.mock_calls[0].kwargs["batch"] == list(range(5))
    # the second range of size 15 should be processed in 2 batches
    assert batcher.mock_batch_processor.mock_calls[1].kwargs["batch"] == list(range(5, 15))
    # the second part of the second range should be processed with 5 items from the third range
    assert batcher.mock_batch_processor.mock_calls[2].kwargs["batch"] == list(range(15, 25))
    # the last 5 items should be processed in a single batch
    assert batcher.mock_batch_processor.mock_calls[3].kwargs["batch"] == list(range(25, 30))
    # the results should be correct regardless the number of needed batches
    assert calls_maker1.result == [i * 2 for i in range(5)]
    assert calls_maker2.result == [i * 2 for i in range(5, 20)]
    assert calls_maker3.result == [i * 2 for i in range(20, 30)]
