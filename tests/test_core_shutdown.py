from __future__ import annotations

import asyncio
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest
from async_batcher.batcher import AsyncBatcher

from tests.core_helpers import GatedBatcher, wait_until


@pytest.mark.asyncio
async def test_graceful_stop_waits_for_active_batch(harness):
    batcher = harness.create(GatedBatcher, max_batch_size=2, max_queue_time=0)
    requests = harness.submit(batcher, 1, 2)
    await asyncio.wait_for(batcher.started.get(), timeout=1)
    stopping = harness.spawn(batcher.stop())
    await asyncio.sleep(0.02)
    assert not stopping.done()
    batcher.release.set()

    await asyncio.wait_for(stopping, timeout=1)
    assert all(task.done() for task in requests)
    assert await asyncio.gather(*requests) == [2, 4]
    assert not await batcher.is_running()
    assert not batcher._running_batches


@pytest.mark.asyncio
async def test_graceful_stop_flushes_queued_partial_batch(harness):
    batcher = harness.create(GatedBatcher, max_batch_size=2, max_queue_time=60, concurrency=1)
    requests = harness.submit(batcher, *range(5))
    await asyncio.wait_for(batcher.started.get(), timeout=1)
    stopping = harness.spawn(batcher.stop())
    await asyncio.sleep(0)
    batcher.release.set()

    await asyncio.wait_for(stopping, timeout=1)
    assert await asyncio.gather(*requests) == [0, 2, 4, 6, 8]
    assert batcher.batches == [[0, 1], [2, 3], [4]]


@pytest.mark.asyncio
async def test_stop_timeout_preserves_in_flight_processing(harness):
    batcher = harness.create(GatedBatcher, max_batch_size=2)
    requests = harness.submit(batcher, 1, 2)
    await asyncio.wait_for(batcher.started.get(), timeout=1)

    with pytest.raises(asyncio.TimeoutError):
        await batcher.stop(timeout=0.02)
    assert not batcher._current_task.cancelled()
    assert all(not task.done() for task in requests)
    batcher.release.set()
    await asyncio.wait_for(batcher.stop(), timeout=1)
    assert await asyncio.gather(*requests) == [2, 4]
    assert not await batcher.is_running()


@pytest.mark.asyncio
async def test_cancelled_stop_waiter_preserves_processing(harness):
    batcher = harness.create(GatedBatcher, max_batch_size=2)
    requests = harness.submit(batcher, 1, 2)
    await asyncio.wait_for(batcher.started.get(), timeout=1)
    stopping = harness.spawn(batcher.stop())
    await asyncio.sleep(0)
    stopping.cancel()
    with pytest.raises(asyncio.CancelledError):
        await stopping
    batcher.release.set()

    await asyncio.wait_for(batcher.stop(), timeout=1)
    assert await asyncio.gather(*requests) == [2, 4]
    assert not await batcher.is_running()


@pytest.mark.asyncio
async def test_force_stop_cancels_active_and_queued_requests(harness):
    batcher = harness.create(GatedBatcher, max_batch_size=2, concurrency=1)
    requests = harness.submit(batcher, *range(5))
    await asyncio.wait_for(batcher.started.get(), timeout=1)

    await asyncio.wait_for(batcher.stop(force=True), timeout=1)
    results = await asyncio.wait_for(asyncio.gather(*requests, return_exceptions=True), timeout=1)
    assert all(isinstance(result, asyncio.CancelledError) for result in results)
    assert not await batcher.is_running()
    assert not batcher._running_batches
    await batcher.stop(force=True)
    await batcher.stop()


@pytest.mark.asyncio
async def test_force_stop_cancels_partially_collected_batch(harness):
    batcher = harness.create(max_batch_size=10, max_queue_time=60)
    request = harness.submit(batcher, 1)[0]
    await asyncio.sleep(0)
    await wait_until(lambda: batcher._queue.empty())

    await asyncio.wait_for(batcher.stop(force=True), timeout=1)
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(request, timeout=1)
    assert batcher.batches == []
    assert not await batcher.is_running()


@pytest.mark.asyncio
@pytest.mark.parametrize("force", [False, True])
async def test_stop_before_start_allows_later_processing(harness, force):
    batcher = harness.create()
    await batcher.stop(force=force)
    assert not await batcher.is_running()
    assert await asyncio.wait_for(batcher.process(1), timeout=1) == 2


@pytest.mark.asyncio
async def test_idle_stop_does_not_wait_for_polling_interval(harness):
    batcher = harness.create(max_batch_size=2, max_queue_time=0)
    assert await batcher.process(1) == 2
    await asyncio.sleep(0)
    await asyncio.wait_for(batcher.stop(), timeout=0.2)
    assert not await batcher.is_running()


@pytest.mark.asyncio
@pytest.mark.parametrize("concurrency", [1, 2, -1])
async def test_concurrency_limit_is_enforced(harness, concurrency):
    batcher = harness.create(GatedBatcher, max_batch_size=2, max_queue_time=0, concurrency=concurrency)
    requests = harness.submit(batcher, *range(12))
    expected_active = concurrency if concurrency > 0 else 6
    for _ in range(expected_active):
        await asyncio.wait_for(batcher.started.get(), timeout=1)
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(batcher.started.get(), timeout=0.02)
    assert batcher.max_active == expected_active
    batcher.release.set()

    assert await asyncio.wait_for(asyncio.gather(*requests), timeout=1) == list(range(0, 24, 2))
    assert batcher.max_active == expected_active
    assert len(batcher.batches) == 6


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["none", "per_item_error", "batch_error", "wrong_length"])
async def test_batch_result_contract(harness, outcome):
    error = ValueError("processor failed")

    class ResultBatcher(AsyncBatcher):
        async def process_batch(self, batch):
            if outcome == "none":
                return None
            if outcome == "per_item_error":
                return [error, 4]
            if outcome == "batch_error":
                raise error
            return [2]

    batcher = harness.create(ResultBatcher, max_batch_size=2)
    requests = harness.submit(batcher, 1, 2)
    results = await asyncio.wait_for(asyncio.gather(*requests, return_exceptions=True), timeout=1)
    if outcome == "none":
        assert results == [None, None]
    elif outcome == "per_item_error":
        assert results == [error, 4]
    elif outcome == "batch_error":
        assert results == [error, error]
    else:
        assert all(isinstance(result, ValueError) for result in results)
        assert all("Expected to get 2 results, but got 1" in str(result) for result in results)
    await asyncio.wait_for(batcher.stop(), timeout=1)
    assert not batcher._running_batches


@pytest.mark.asyncio
async def test_synchronous_processor_uses_supplied_executor(harness):
    class SyncBatcher(AsyncBatcher):
        def process_batch(self, batch):
            return [(item * 2, threading.current_thread().name) for item in batch]

    with ThreadPoolExecutor(max_workers=1, thread_name_prefix="batcher-test") as executor:
        batcher = harness.create(SyncBatcher, max_batch_size=2, executor=executor)
        requests = harness.submit(batcher, 1, 2)
        results = await asyncio.wait_for(asyncio.gather(*requests), timeout=1)
        assert [value for value, _ in results] == [2, 4]
        assert all(name.startswith("batcher-test") for _, name in results)
        await asyncio.wait_for(batcher.stop(), timeout=1)


@pytest.mark.asyncio
async def test_force_stop_does_not_start_a_scheduled_processor(harness):
    batcher = harness.create(max_batch_size=1)
    request = harness.submit(batcher, 1)[0]

    async def stop_after_dispatch():
        # Let the dispatcher schedule its worker, then stop before that worker's first step.
        await asyncio.sleep(0)
        await batcher.stop(force=True)

    await asyncio.wait_for(harness.spawn(stop_after_dispatch()), timeout=1)
    with pytest.raises(asyncio.CancelledError):
        await request
    assert batcher.batches == []


@pytest.mark.asyncio
@pytest.mark.parametrize("timeout_first", [False, True])
async def test_force_stop_waits_for_processor_cancellation_cleanup(harness, timeout_first):
    started = asyncio.Event()
    cleanup_started = asyncio.Event()
    allow_cleanup = asyncio.Event()
    cleanup_finished = asyncio.Event()

    class CleanupBatcher(AsyncBatcher):
        async def process_batch(self, batch):
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                cleanup_started.set()
                await allow_cleanup.wait()
                cleanup_finished.set()

    batcher = harness.create(CleanupBatcher, max_batch_size=1)
    request = harness.submit(batcher, 1)[0]
    await asyncio.wait_for(started.wait(), timeout=1)
    if timeout_first:
        with pytest.raises(asyncio.TimeoutError):
            await batcher.stop(timeout=0.01)
    stopping = harness.spawn(batcher.stop(force=True))
    await asyncio.wait_for(cleanup_started.wait(), timeout=1)
    also_stopping = harness.spawn(batcher.stop(force=True))
    await asyncio.sleep(0.01)
    assert not stopping.done()
    assert not also_stopping.done()
    allow_cleanup.set()

    await asyncio.wait_for(asyncio.gather(stopping, also_stopping), timeout=1)
    assert cleanup_finished.is_set()
    with pytest.raises(asyncio.CancelledError):
        await request
    assert not batcher._running_batches


@pytest.mark.asyncio
@pytest.mark.skipif(not hasattr(asyncio, "eager_task_factory"), reason="Requires Python 3.12+")
async def test_eager_task_factory_preserves_worker_tracking(harness):
    loop = asyncio.get_running_loop()
    previous_factory = loop.get_task_factory()
    loop.set_task_factory(asyncio.eager_task_factory)
    try:
        batcher = harness.create(GatedBatcher, max_batch_size=2)
        requests = harness.submit(batcher, 1, 2)
        await asyncio.wait_for(batcher.started.get(), timeout=1)
        await asyncio.wait_for(batcher.stop(force=True), timeout=1)
        results = await asyncio.wait_for(asyncio.gather(*requests, return_exceptions=True), timeout=1)
        assert all(isinstance(result, asyncio.CancelledError) for result in results)
        assert not batcher._running_batches
        assert not await batcher.is_running()
    finally:
        loop.set_task_factory(previous_factory)
