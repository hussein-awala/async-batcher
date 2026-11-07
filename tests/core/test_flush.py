from __future__ import annotations

import asyncio
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest
from async_batcher import AsyncBatcher

from tests.core_helpers import GatedBatcher, wait_until


@pytest.mark.asyncio
@pytest.mark.parametrize("eager", [False, True])
async def test_flush_dispatches_partial_batch_and_keeps_intake_open(harness, eager):
    if eager and not hasattr(asyncio, "eager_task_factory"):
        pytest.skip("Eager tasks require Python 3.12+")
    loop = asyncio.get_running_loop()
    factory = loop.get_task_factory()
    if eager:
        loop.set_task_factory(asyncio.eager_task_factory)
    try:
        batcher = harness.create(max_batch_size=10, max_queue_time=3600)
        first = harness.submit(batcher, 1, 2)
        await wait_until(lambda: batcher.stats().accepted == 2 and batcher.stats().queued_items == 0)
        await asyncio.wait_for(batcher.flush(), 1)
        assert await asyncio.gather(*first) == [2, 4]
        assert batcher.batches == [[1, 2]]
        assert batcher.stats().state == "running"
        later = harness.submit(batcher, 3)[0]
        await wait_until(lambda: batcher.stats().accepted == 3)
        await asyncio.wait_for(batcher.flush(), 1)
        assert await later == 6
        assert not batcher._work_sequences and not batcher._flush_targets
    finally:
        loop.set_task_factory(factory)


@pytest.mark.asyncio
async def test_flush_excludes_later_acceptance_and_waiting_producer(harness):
    later_gate = asyncio.Event()

    class LaterGated(GatedBatcher):
        async def process_batch(self, batch):
            if batch == [3]:
                await later_gate.wait()
            return await super().process_batch(batch)

    batcher = harness.create(LaterGated, max_batch_size=1, max_queue_size=1)
    harness.submit(batcher, 1)
    assert await batcher.started.get() == [1]
    harness.submit(batcher, 2)
    await wait_until(lambda: batcher.stats().accepted == 2)
    waiting = harness.spawn(batcher.process(3, wait_for_space=True))
    await wait_until(lambda: batcher.stats().admission_waiters == 1)
    flushing = harness.spawn(batcher.flush())
    await wait_until(lambda: bool(batcher._flush_targets))
    batcher.release.set()
    assert await batcher.started.get() == [2]
    await asyncio.wait_for(flushing, 1)
    assert not waiting.done()
    later_gate.set()
    assert await waiting == 6


@pytest.mark.asyncio
async def test_partial_collection_boundary_splits_later_items(harness):
    batcher = harness.create(GatedBatcher, max_batch_size=10, max_queue_time=3600)
    harness.submit(batcher, 1)
    await wait_until(lambda: batcher.stats().accepted == 1 and batcher.stats().queued_items == 0)
    flushing = harness.spawn(batcher.flush())
    # Let flush register its boundary, then add work before the collector resumes.
    await wait_until(lambda: bool(batcher._flush_targets))
    later = harness.submit(batcher, 2)[0]
    assert await asyncio.wait_for(batcher.started.get(), 1) == [1]
    batcher.release.set()
    await asyncio.wait_for(flushing, 1)
    assert not later.done()
    await batcher.stop()
    assert await later == 4
    assert batcher.batches == [[1], [2]]


@pytest.mark.asyncio
async def test_multiple_flush_boundaries_and_cost_carryover(harness):
    batcher = harness.create(GatedBatcher, max_queue_time=3600, max_batch_cost=3, item_cost=lambda _: 2)
    requests = harness.submit(batcher, 1, 2)
    await batcher.started.get()
    await wait_until(lambda: batcher.stats().accepted == 2)
    first = harness.spawn(batcher.flush())
    await wait_until(lambda: bool(batcher._flush_targets))
    requests += harness.submit(batcher, 3)
    await wait_until(lambda: batcher.stats().accepted == 3)
    second = harness.spawn(batcher.flush())
    same = harness.spawn(batcher.flush())
    await wait_until(lambda: sum(batcher._flush_targets.values()) == 3)
    batcher.release.set()
    await asyncio.wait_for(asyncio.gather(first, second, same), 1)
    assert await asyncio.gather(*requests) == [2, 4, 6]
    assert batcher.batches == [[1], [2], [3]]
    assert not batcher._flush_targets and not batcher._work_sequences


@pytest.mark.asyncio
@pytest.mark.parametrize("eager", [False, True])
@pytest.mark.parametrize("cancel_oldest", [False, True])
async def test_concurrent_flush_waits_for_out_of_order_boundary_but_not_later_work(
    harness, eager, cancel_oldest
):
    if eager and not hasattr(asyncio, "eager_task_factory"):
        pytest.skip("Eager tasks require Python 3.12+")
    loop = asyncio.get_running_loop()
    previous = loop.get_task_factory()
    gates = {item: asyncio.Event() for item in (1, 2, 3)}
    started = asyncio.Queue()
    events = []

    class IndependentBatches(AsyncBatcher):
        async def process_batch(self, batch):
            started.put_nowait(batch[0])
            await gates[batch[0]].wait()
            return batch

    if eager:
        loop.set_task_factory(asyncio.eager_task_factory)
    batcher = harness.create(IndependentBatches, max_batch_size=1, concurrency=3, on_batch=events.append)
    try:
        first, second = harness.submit(batcher, 1, 2)
        assert {await started.get(), await started.get()} == {1, 2}
        flushing = harness.spawn(batcher.flush())
        await wait_until(lambda: bool(batcher._flush_targets))
        later = harness.submit(batcher, 3)[0]
        assert await started.get() == 3
        if cancel_oldest:
            first.cancel()
            await asyncio.gather(first, return_exceptions=True)
        gates[2].set()
        assert await second == 2
        await wait_until(lambda: len(events) == 1)
        assert not flushing.done()
        gates[1].set()
        await asyncio.wait_for(flushing, 1)
        assert [event.batch_id for event in events] == [2, 1]
        assert events[-1].cancelled == int(cancel_oldest)
        assert not later.done()
        assert batcher.stats().active_batches == 1
        if not cancel_oldest:
            assert await first == 1
        gates[3].set()
        assert await later == 3
    finally:
        for gate in gates.values():
            gate.set()
        loop.set_task_factory(previous)


@pytest.mark.asyncio
async def test_flush_waits_for_cancelled_request_backend_and_completion_hook(harness):
    events = []
    batcher = harness.create(GatedBatcher, max_batch_size=1, on_batch=events.append)
    request = harness.submit(batcher, 1)[0]
    await batcher.started.get()
    request.cancel()
    await asyncio.gather(request, return_exceptions=True)
    flushing = harness.spawn(batcher.flush())
    await wait_until(lambda: bool(batcher._flush_targets))
    assert batcher.stats().pending_items == 0 and not flushing.done()
    batcher.release.set()
    await asyncio.wait_for(flushing, 1)
    assert len(events) == 1 and events[0].cancelled == 1
    assert batcher.stats().active_batches == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("ending", ["timeout", "cancel"])
async def test_abandoned_flush_wait_does_not_cancel_work(harness, ending):
    batcher = harness.create(GatedBatcher, max_batch_size=1)
    request = harness.submit(batcher, 1)[0]
    await batcher.started.get()
    flushing = harness.spawn(batcher.flush(timeout=0.02 if ending == "timeout" else None))
    await wait_until(lambda: bool(batcher._flush_targets))
    if ending == "cancel":
        flushing.cancel()
    with pytest.raises(TimeoutError if ending == "timeout" else asyncio.CancelledError):
        await flushing
    assert not batcher._flush_targets and not request.done()
    batcher.release.set()
    await batcher.flush()
    assert await request == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("force", [False, True])
async def test_flush_during_shutdown_and_restart(harness, force):
    batcher = harness.create(GatedBatcher, max_batch_size=1)
    request = harness.submit(batcher, 1)[0]
    await batcher.started.get()
    flushing = harness.spawn(batcher.flush())
    await wait_until(lambda: bool(batcher._flush_targets))
    if force:
        await batcher.stop(force=True)
        with pytest.raises(RuntimeError, match="interrupted by forced shutdown"):
            await flushing
        await asyncio.gather(request, return_exceptions=True)
    else:
        stopping = harness.spawn(batcher.stop())
        batcher.release.set()
        await asyncio.gather(flushing, stopping)
        assert await request == 2
    batcher.release.set()
    restarted = harness.submit(batcher, 2)[0]
    await wait_until(lambda: batcher.stats().accepted == 2)
    await batcher.flush()
    assert await restarted == 4


@pytest.mark.asyncio
async def test_item_errors_do_not_make_flush_claim_or_require_success(harness):
    error = ValueError("failed")
    batcher = harness.create(GatedBatcher, max_batch_size=1, error=error)
    request = harness.submit(batcher, 1)[0]
    await batcher.started.get()
    batcher.release.set()
    await batcher.flush()
    with pytest.raises(ValueError) as caught:
        await request
    assert caught.value is error
    assert batcher.stats().failed == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("descendant", [False, True])
async def test_processor_cannot_flush_its_own_batcher(harness, descendant):
    class SelfFlush(AsyncBatcher):
        async def process_batch(self, batch):
            if descendant:
                await asyncio.create_task(self.flush())
            else:
                await self.flush()
            return batch

    batcher = harness.create(SelfFlush, max_batch_size=1)
    with pytest.raises(RuntimeError, match=r"flush\(\).*outside the processor"):
        await asyncio.wait_for(batcher.process(1), 1)


@pytest.mark.asyncio
async def test_force_interrupts_flush_without_claiming_native_thread_finished(harness):
    started = threading.Event()
    release = threading.Event()
    finished = threading.Event()

    def processor(batch):
        started.set()
        release.wait(5)
        finished.set()
        return batch

    with ThreadPoolExecutor(max_workers=1) as executor:
        batcher = AsyncBatcher.from_callable(processor, executor=executor, max_batch_size=1)
        harness.batchers.append(batcher)
        request = harness.submit(batcher, 1)[0]
        try:
            await wait_until(started.is_set)
            flushing = harness.spawn(batcher.flush())
            await wait_until(lambda: bool(batcher._flush_targets))
            await batcher.stop(force=True)
            with pytest.raises(RuntimeError, match="forced shutdown"):
                await flushing
            assert not finished.is_set()
            await asyncio.gather(request, return_exceptions=True)
        finally:
            release.set()
    assert finished.is_set()


@pytest.mark.asyncio
async def test_empty_flush_binds_loop_without_starting_dispatcher(harness):
    batcher = harness.create()
    await batcher.flush()
    assert batcher.stats().state == "idle"
    assert batcher._current_task is None
    with pytest.raises(RuntimeError, match="same event loop"):
        await asyncio.to_thread(lambda: asyncio.run(batcher.flush()))


@pytest.mark.asyncio
@pytest.mark.parametrize("timeout", [-1, True, float("nan"), float("inf"), "1"])
async def test_invalid_flush_timeout_is_rejected(harness, timeout):
    with pytest.raises(ValueError, match="timeout"):
        await harness.create().flush(timeout)


@pytest.mark.asyncio
async def test_flush_started_during_forced_cleanup_rejects_until_cleanup_finishes(harness):
    cleanup_started = asyncio.Event()
    cleanup_release = asyncio.Event()

    class SlowCleanup(GatedBatcher):
        async def process_batch(self, batch):
            try:
                return await super().process_batch(batch)
            finally:
                cleanup_started.set()
                await cleanup_release.wait()

    batcher = harness.create(SlowCleanup, max_batch_size=1)
    request = harness.submit(batcher, 1)[0]
    await batcher.started.get()
    stopping = harness.spawn(batcher.stop(force=True))
    try:
        await asyncio.wait_for(cleanup_started.wait(), 1)
        with pytest.raises(RuntimeError, match="forced shutdown"):
            await batcher.flush()
        assert not stopping.done()
    finally:
        cleanup_release.set()
    await stopping
    await asyncio.gather(request, return_exceptions=True)
    await batcher.flush()
