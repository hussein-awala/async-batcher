from __future__ import annotations

import asyncio

import pytest
from async_batcher.exceptions import QueueFullException

from tests.conftest import MockAsyncBatcher, SlowAsyncBatcher
from tests.core_helpers import GatedBatcher


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

    async def process_and_catch_exception(self, item):
        try:
            return await self.batcher.process(item=item)
        except Exception as e:
            return e

    async def arun(self):
        await asyncio.sleep(self.sleep_time)
        result = await asyncio.gather(
            *[self.process_and_catch_exception(item=i) for i in range(self.start_range, self.end_range)]
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
    await batcher.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("concurrency", [1, 2, 3, -1])
async def test_concurrent_process_batch(harness, concurrency):
    batcher = harness.create(GatedBatcher, max_batch_size=5, max_queue_time=60, concurrency=concurrency)
    expected_batches = [list(range(start, start + 5)) for start in range(0, 20, 5)]
    active_limit = len(expected_batches) if concurrency == -1 else concurrency
    requests = harness.submit(batcher, *range(20))

    async with asyncio.timeout(3):
        # Full batches can start together, but none can finish before this gate opens.
        first_wave = [await batcher.started.get() for _ in range(active_limit)]
        assert first_wave == expected_batches[:active_limit]
        assert batcher.active == active_limit
        assert all(not request.done() for request in requests)
        batcher.release.set()
        assert await asyncio.gather(*requests) == [item * 2 for item in range(20)]
        await batcher.stop()

    # Check the peak across every wave, including work scheduled after the gate opens.
    assert batcher.max_active == active_limit
    assert batcher.active == 0
    assert batcher.batches == expected_batches


@pytest.mark.asyncio
async def test_stop_batcher(mock_async_batcher):
    await asyncio.gather(*[mock_async_batcher.process(item=i) for i in range(10)])

    assert await mock_async_batcher.is_running()
    await mock_async_batcher.stop()
    assert not await mock_async_batcher.is_running()
    assert await mock_async_batcher.process(item=3) == 6


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
    for task in batcher._running_batches.values():
        assert task.cancelled() or task.cancelling()
    batcher.mock_batch_processor.reset_mock()


@pytest.mark.asyncio
async def test_max_queue_size():
    batcher = SlowAsyncBatcher(
        sleep_time=1,
        max_batch_size=10,
        max_queue_time=0.2,
        concurrency=1,
        max_queue_size=15,
    )
    batcher.mock_batch_processor.reset_mock()
    calls_maker1 = CallsMaker(batcher, 0, 0, 10)
    calls_maker2 = CallsMaker(batcher, 0.25, 10, 20)
    calls_maker3 = CallsMaker(batcher, 0.4, 20, 30)
    await asyncio.gather(calls_maker1.arun(), calls_maker2.arun(), calls_maker3.arun())
    assert batcher.mock_batch_processor.call_count == 3
    assert calls_maker1.result == [i * 2 for i in range(10)]
    assert calls_maker2.result == [i * 2 for i in range(10, 20)]
    assert calls_maker3.result[:5] == [i * 2 for i in range(20, 25)]
    assert all(isinstance(e, QueueFullException) for e in calls_maker3.result[5:])
    batcher.mock_batch_processor.reset_mock()
    await batcher.stop()
