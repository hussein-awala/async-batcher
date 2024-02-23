from __future__ import annotations

import asyncio

import pytest

from tests.conftest import MockAsyncBatcher, SlowAsyncBatcher


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
    calls_maker2 = CallsMaker(batcher, 0.25, 5, 20)
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


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "concurrency, min_execution_time, max_execution_time",
    [
        # batch 1: from 0 to 1
        # batch 2: from 1 to 2
        # batch 3: from 2 to 3
        # batch 4: from 3.2 to 4.2 (< max_batch_size, extra 0.2)
        (1, 4.2, 4.4),
        # batch 1: from 0 to 1
        # batch 2: from 0.25 to 1.25
        # batch 3: from 1 to 2
        # batch 4: from 1.45 to 2.45 (< max_batch_size, extra 0.2)
        (2, 2.45, 2.65),
        # batch 1: from 0 to 1
        # batch 2: from 0.25 to 1.25
        # batch 3: from 0.4 to 1.4
        # batch 4: from 1.2 to 2.2 (< max_batch_size, extra 0.2)
        (3, 2.2, 2.4),
        # batch 1: from 0 to 1
        # batch 2: from 0.25 to 1.25
        # batch 3: from 0.4 to 1.4
        # batch 4: from 0.6 to 1.6 (< max_batch_size, extra 0.2)
        (-1, 1.6, 1.8),
    ],
)
async def test_concurrent_process_batch(concurrency, min_execution_time, max_execution_time):
    batcher = SlowAsyncBatcher(
        sleep_time=1,
        max_batch_size=10,
        max_queue_time=0.2,
        concurrency=concurrency,
    )
    batcher.mock_batch_processor.reset_mock()
    started_at = asyncio.get_event_loop().time()
    calls_maker1 = CallsMaker(batcher, 0, 0, 5)
    calls_maker2 = CallsMaker(batcher, 0.25, 5, 20)
    calls_maker3 = CallsMaker(batcher, 0.4, 20, 30)
    await asyncio.gather(calls_maker1.arun(), calls_maker2.arun(), calls_maker3.arun())
    ended_at = asyncio.get_event_loop().time()

    # the last batch should be processed after:
    # 1 (batch processing time) + 0.4 (sleep time) seconds = 1.4 seconds at most
    # we add 0.4 seconds to the expected time to account for the sleep time
    assert min_execution_time < ended_at - started_at
    assert ended_at - started_at < max_execution_time

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
    batcher.mock_batch_processor.reset_mock()


@pytest.mark.asyncio
async def test_stop_batcher(mock_async_batcher):
    await asyncio.gather(*[mock_async_batcher.process(item=i) for i in range(10)])

    assert await mock_async_batcher.is_running()
    await mock_async_batcher.stop()
    assert not await mock_async_batcher.is_running()
    with pytest.raises(RuntimeError):
        await mock_async_batcher.process(item=0)


@pytest.mark.asyncio
async def test_force_stop_batcher():
    batcher = SlowAsyncBatcher(
        sleep_time=1,
        max_batch_size=10,
        max_queue_time=0.2,
        concurrency=1,
    )
    batcher.mock_batch_processor.reset_mock()
    await asyncio.gather(*[batcher.process(item=i) for i in range(10)])
    assert await batcher.is_running()
    await batcher.stop(force=True)
    assert batcher._current_task.cancelled() or batcher._current_task.cancelling()
    for task in batcher._running_batches:
        assert task.cancelled() or task.cancelling()
    batcher.mock_batch_processor.reset_mock()
