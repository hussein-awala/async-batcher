from __future__ import annotations

import asyncio

import pytest
from async_batcher.exceptions import QueueFullException

from tests.core_helpers import GatedBatcher, wait_until


async def fill_queue(harness):
    batcher = harness.create(GatedBatcher, max_batch_size=1, max_queue_size=1, max_queue_time=0)
    active = harness.submit(batcher, 1)[0]
    await asyncio.wait_for(batcher.started.get(), 2)
    queued = harness.submit(batcher, 2)[0]
    await wait_until(batcher._queue.full)
    return batcher, active, queued


@pytest.mark.asyncio
async def test_waiting_admission_keeps_default_immediate_rejection(harness):
    batcher, active, queued = await fill_queue(harness)
    with pytest.raises(QueueFullException):
        await batcher.process(99)
    waiting = harness.spawn(batcher.process(3, wait_for_space=True))
    await asyncio.sleep(0)
    assert not waiting.done()
    assert len(batcher._pending_futures) == 2
    batcher.release.set()
    assert await asyncio.wait_for(asyncio.gather(active, queued, waiting), 2) == [2, 4, 6]
    assert batcher.batches == [[1], [2], [3]]


@pytest.mark.asyncio
@pytest.mark.parametrize("timeout", [0, 0.01])
async def test_admission_timeout_never_queues_the_item(harness, timeout):
    batcher, active, queued = await fill_queue(harness)
    with pytest.raises(TimeoutError):
        await batcher.process(99, wait_for_space=True, enqueue_timeout=timeout)
    assert len(batcher._pending_futures) == 2
    batcher.release.set()
    assert await asyncio.wait_for(asyncio.gather(active, queued), 2) == [2, 4]
    await batcher.stop()
    assert batcher.batches == [[1], [2]]


@pytest.mark.asyncio
async def test_admission_timeout_does_not_limit_result_wait(harness):
    batcher = harness.create(GatedBatcher, max_batch_size=1, max_queue_size=1)
    request = harness.spawn(batcher.process(1, wait_for_space=True, enqueue_timeout=0))
    await asyncio.wait_for(batcher.started.get(), 2)
    assert not request.done()
    batcher.release.set()
    assert await asyncio.wait_for(request, 2) == 2


@pytest.mark.asyncio
async def test_cancelling_waiter_does_not_consume_capacity_or_cancel_peers(harness):
    batcher, active, queued = await fill_queue(harness)
    cancelled = harness.spawn(batcher.process(99, wait_for_space=True))
    waiting = harness.spawn(batcher.process(3, wait_for_space=True))
    await asyncio.sleep(0)
    cancelled.cancel()
    with pytest.raises(asyncio.CancelledError):
        await cancelled
    batcher.release.set()
    assert await asyncio.wait_for(asyncio.gather(active, queued, waiting), 2) == [2, 4, 6]
    assert batcher.batches == [[1], [2], [3]]


@pytest.mark.asyncio
@pytest.mark.parametrize("force", [False, True])
async def test_shutdown_rejects_unaccepted_waiters_and_allows_fresh_restart(harness, force):
    batcher, active, queued = await fill_queue(harness)
    waiting = [harness.spawn(batcher.process(i, wait_for_space=True)) for i in range(3, 8)]
    await asyncio.sleep(0)
    stopping = harness.spawn(batcher.stop(force=force))
    outcomes = await asyncio.wait_for(asyncio.gather(*waiting, return_exceptions=True), 2)
    assert all(isinstance(error, RuntimeError) and "stopping" in str(error) for error in outcomes)
    batcher.release.set()
    await asyncio.wait_for(stopping, 2)
    accepted = await asyncio.gather(active, queued, return_exceptions=True)
    assert accepted == [2, 4] if not force else all(isinstance(x, asyncio.CancelledError) for x in accepted)
    assert await asyncio.wait_for(batcher.process(8, wait_for_space=True), 2) == 16
    assert all(item not in range(3, 8) for batch in batcher.batches for item in batch)


@pytest.mark.asyncio
async def test_dispatcher_cancellation_wakes_waiting_producers(harness):
    batcher, active, queued = await fill_queue(harness)
    waiting = harness.spawn(batcher.process(3, wait_for_space=True))
    await asyncio.sleep(0)
    batcher._current_task.cancel()
    with pytest.raises(RuntimeError, match="stopping"):
        await asyncio.wait_for(waiting, 2)
    await batcher.stop()
    assert all(
        isinstance(x, asyncio.CancelledError)
        for x in await asyncio.gather(active, queued, return_exceptions=True)
    )


@pytest.mark.asyncio
async def test_waiter_cannot_cross_shutdown_generation_even_if_woken_after_restart(harness):
    batcher, active, queued = await fill_queue(harness)
    entered = asyncio.Event()
    resume = asyncio.Event()
    original_wait = batcher._queue_space_available.wait

    async def delayed_wake():
        entered.set()
        await original_wait()
        await resume.wait()

    batcher._queue_space_available.wait = delayed_wake
    waiting = harness.spawn(batcher.process(99, wait_for_space=True))
    await asyncio.wait_for(entered.wait(), 2)
    await batcher.stop(force=True)
    batcher.release.set()
    assert await asyncio.wait_for(batcher.process(3), 2) == 6
    resume.set()
    with pytest.raises(RuntimeError, match="stopping"):
        await asyncio.wait_for(waiting, 2)
    await asyncio.gather(active, queued, return_exceptions=True)
    assert all(99 not in batch for batch in batcher.batches)


@pytest.mark.asyncio
async def test_many_waiters_survive_cancellation_and_repeated_restart(harness):
    batcher = harness.create(max_batch_size=4, max_queue_size=2, max_queue_time=0)
    for cycle in range(3):
        requests = [harness.spawn(batcher.process(i, wait_for_space=True)) for i in range(40)]
        for index in range(cycle, len(requests), 5):
            requests[index].cancel()
        results = await asyncio.wait_for(asyncio.gather(*requests, return_exceptions=True), 2)
        for index, result in enumerate(results):
            assert isinstance(result, asyncio.CancelledError) or result == index * 2
        await batcher.stop()
        assert not batcher._pending_futures
        assert not batcher._running_batches
        assert await batcher.process(40, wait_for_space=True) == 80


@pytest.mark.asyncio
@pytest.mark.parametrize("timeout", [-1, float("nan"), float("inf"), True, "1", 10**1000])
async def test_invalid_admission_timeout_fails_before_binding_or_accepting(harness, timeout):
    batcher = harness.create()
    with pytest.raises(ValueError, match="enqueue_timeout"):
        await batcher.process(1, wait_for_space=True, enqueue_timeout=timeout)
    assert batcher._loop is None
    assert not batcher._pending_futures


@pytest.mark.asyncio
async def test_timeout_requires_waiting_and_waiting_flag_is_boolean(harness):
    batcher = harness.create()
    with pytest.raises(ValueError, match="requires wait_for_space"):
        await batcher.process(1, enqueue_timeout=1)
    with pytest.raises(TypeError, match="boolean"):
        await batcher.process(1, wait_for_space=1)
