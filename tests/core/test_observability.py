from __future__ import annotations

import asyncio
import gc
import inspect
from dataclasses import FrozenInstanceError, asdict
from functools import partial
from unittest.mock import patch

import pytest
from async_batcher import AsyncBatcher, BatcherStats, BatchEvent, QueueFullException

from tests.core_helpers import GatedBatcher, RecordingBatcher, wait_until


def assert_balanced(stats):
    assert stats.accepted == stats.pending_items + stats.succeeded + stats.failed + stats.cancelled
    assert stats.batches_dispatched == stats.active_batches + stats.batches_completed


def test_new_snapshot_is_immutable_and_does_not_bind_a_loop():
    batcher = RecordingBatcher()
    stats = batcher.stats()
    assert isinstance(stats, BatcherStats)
    assert set(asdict(stats).values()) == {0}
    assert batcher._loop is None
    with pytest.raises(FrozenInstanceError):
        stats.accepted = 1


@pytest.mark.asyncio
async def test_queue_pending_and_in_flight_are_distinct_and_cancellation_is_immediate(harness):
    events = []
    batcher = harness.create(GatedBatcher, max_batch_size=1, max_queue_size=1, on_batch=events.append)
    first = harness.submit(batcher, 1)[0]
    await batcher.started.get()
    queued = harness.submit(batcher, 2)[0]
    await wait_until(lambda: batcher.stats().queued_items == 1)
    before = batcher.stats()
    assert (before.accepted, before.pending_items, before.in_flight_items, before.active_batches) == (
        2,
        2,
        1,
        1,
    )
    with pytest.raises(QueueFullException):
        await batcher.process(3)
    queued.cancel()
    # Task.cancel immediately cancels its awaited future; its done callback has
    # not run yet. Snapshot counts must already reflect the terminal outcome.
    after = batcher.stats()
    assert (after.queued_items, after.pending_items, after.cancelled, after.rejected) == (1, 1, 1, 1)
    assert before.cancelled == 0
    assert_balanced(after)
    batcher.release.set()
    assert await first == 2
    await asyncio.gather(queued, return_exceptions=True)
    await batcher.stop()
    final = batcher.stats()
    assert (final.queued_items, final.pending_items, final.in_flight_items, final.active_batches) == (
        0,
        0,
        0,
        0,
    )
    assert (final.accepted, final.succeeded, final.cancelled, final.batches_completed) == (2, 1, 1, 2)
    assert [event.cancelled for event in events] == [0, 1]
    assert_balanced(final)


@pytest.mark.asyncio
async def test_partial_collection_is_pending_but_not_queued_or_dispatched(harness):
    events = []
    batcher = harness.create(max_batch_size=4, max_queue_time=3600, on_batch=events.append)
    requests = harness.submit(batcher, 1, 2)
    await wait_until(lambda: batcher.stats().accepted == 2 and batcher.stats().queued_items == 0)
    stats = batcher.stats()
    assert (stats.pending_items, stats.in_flight_items, stats.active_batches) == (2, 0, 0)
    await batcher.stop(force=True)
    await asyncio.gather(*requests, return_exceptions=True)
    final = batcher.stats()
    assert (final.accepted, final.cancelled, final.batches_dispatched) == (2, 2, 0)
    assert not events
    assert_balanced(final)


@pytest.mark.asyncio
async def test_mixed_outcomes_and_hook_snapshot_after_cleanup(harness):
    class MixedBatcher(AsyncBatcher):
        async def process_batch(self, batch):
            return [10, ValueError("failure"), asyncio.CancelledError(), StopIteration()]

    events = []
    snapshots = []

    class Hook:
        def __call__(self, event):
            events.append(event)
            snapshots.append(batcher.stats())

    batcher = harness.create(MixedBatcher, max_batch_size=4, on_batch=Hook())
    results = await asyncio.gather(*harness.submit(batcher, 1, 2, 3, 4), return_exceptions=True)
    assert results[0] == 10
    assert isinstance(results[1], ValueError)
    assert isinstance(results[2], asyncio.CancelledError)
    assert isinstance(results[3], RuntimeError)
    await batcher.stop()
    event = events[0]
    assert isinstance(event, BatchEvent)
    assert (event.batch_id, event.size, event.succeeded, event.failed, event.cancelled) == (1, 4, 1, 2, 1)
    assert event.queue_wait_seconds >= 0
    assert event.processing_seconds >= 0
    assert set(asdict(event)) == {
        "batch_id",
        "size",
        "queue_wait_seconds",
        "processing_seconds",
        "succeeded",
        "failed",
        "cancelled",
    }
    with pytest.raises(FrozenInstanceError):
        event.size = 99
    stats = snapshots[0]
    assert (stats.pending_items, stats.active_batches, stats.in_flight_items) == (0, 0, 0)
    assert (stats.accepted, stats.succeeded, stats.failed, stats.cancelled) == (4, 1, 2, 1)
    assert_balanced(stats)


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["error", "malformed", "cancelled", "none", "sync"])
async def test_whole_batch_outcomes_and_processing_duration(harness, mode):
    class OutcomeBatcher(AsyncBatcher):
        async def process_batch(self, batch):
            if mode == "error":
                raise ValueError("batch failed")
            if mode == "malformed":
                return []
            if mode == "cancelled":
                raise asyncio.CancelledError
            return None

    class SyncBatcher(AsyncBatcher):
        def process_batch(self, batch):
            return None

    events = []
    batcher = harness.create(
        SyncBatcher if mode == "sync" else OutcomeBatcher, max_batch_size=2, on_batch=events.append
    )
    await asyncio.gather(*harness.submit(batcher, 1, 2), return_exceptions=True)
    await batcher.stop()
    expected = (
        (0, 2, 0) if mode in {"error", "malformed"} else (0, 0, 2) if mode == "cancelled" else (2, 0, 0)
    )
    event = events[0]
    assert (event.succeeded, event.failed, event.cancelled) == expected
    assert event.processing_seconds is not None
    stats = batcher.stats()
    assert (stats.succeeded, stats.failed, stats.cancelled) == expected
    assert_balanced(stats)


@pytest.mark.asyncio
async def test_worker_cancelled_before_start_has_event_without_processing_duration(harness):
    create_task = asyncio.create_task

    def cancel_worker(coro, **kwargs):
        task = create_task(coro, **kwargs)
        if coro.__name__ == "_batch_run":
            task.cancel()
        return task

    events = []
    batcher = harness.create(max_batch_size=1, on_batch=events.append)
    with patch("asyncio.create_task", side_effect=cancel_worker):
        result = await asyncio.gather(batcher.process(1), return_exceptions=True)
    assert isinstance(result[0], asyncio.CancelledError)
    await batcher.stop()
    assert len(events) == 1
    assert events[0].processing_seconds is None
    assert events[0].queue_wait_seconds >= 0
    assert (events[0].size, events[0].cancelled) == (1, 1)
    assert_balanced(batcher.stats())


@pytest.mark.asyncio
async def test_shutdown_rejection_and_restart_keep_lifetime_totals(harness):
    events = []
    batcher = harness.create(GatedBatcher, max_batch_size=1, on_batch=events.append)
    request = harness.submit(batcher, 1)[0]
    await batcher.started.get()
    with pytest.raises(TimeoutError):
        await batcher.stop(timeout=0)
    with pytest.raises(RuntimeError, match="stopping"):
        await batcher.process(2)
    assert batcher.stats().rejected == 1
    batcher.release.set()
    assert await request == 2
    await batcher.stop()
    assert await batcher.process(3) == 6
    await batcher.stop()
    assert (batcher.stats().accepted, batcher.stats().succeeded, batcher.stats().batches_completed) == (
        2,
        2,
        2,
    )
    assert [event.batch_id for event in events] == [1, 2]
    assert_balanced(batcher.stats())


@pytest.mark.asyncio
async def test_force_stop_counts_caller_cancellation_not_late_backend_success(harness):
    class ResistantBatcher(AsyncBatcher):
        async def process_batch(self, batch):
            started.set()
            try:
                await release.wait()
            except asyncio.CancelledError:
                await release.wait()
            return batch

    started = asyncio.Event()
    release = asyncio.Event()
    events = []
    batcher = harness.create(ResistantBatcher, max_batch_size=1, on_batch=events.append)
    request = harness.submit(batcher, 1)[0]
    await started.wait()
    stopping = harness.spawn(batcher.stop(force=True))
    await wait_until(lambda: batcher.stats().cancelled == 1)
    stats = batcher.stats()
    assert (stats.pending_items, stats.in_flight_items, stats.active_batches) == (0, 1, 1)
    release.set()
    await stopping
    await asyncio.gather(request, return_exceptions=True)
    assert (events[0].succeeded, events[0].cancelled) == (0, 1)
    assert_balanced(batcher.stats())


@pytest.mark.asyncio
async def test_foreign_loop_rejection_does_not_change_stats(harness):
    batcher = harness.create()
    await batcher.stop()
    before = batcher.stats()

    def foreign_loop():
        with pytest.raises(RuntimeError, match="same event loop"):
            asyncio.run(batcher.process(1))

    await asyncio.to_thread(foreign_loop)
    assert batcher.stats() == before


@pytest.mark.asyncio
async def test_failed_worker_creation_does_not_leave_active_stats_or_unawaited_coroutine(recwarn):
    create_task = asyncio.create_task

    def fail_worker(coro, **kwargs):
        if coro.__name__ == "_batch_run":
            raise RuntimeError("task factory failed")
        return create_task(coro, **kwargs)

    batcher = RecordingBatcher(max_batch_size=1)
    with patch("asyncio.create_task", side_effect=fail_worker):
        result = await asyncio.gather(batcher.process(1), return_exceptions=True)
    assert isinstance(result[0], asyncio.CancelledError)
    with pytest.raises(RuntimeError, match="task factory failed"):
        await batcher.stop()
    stats = batcher.stats()
    assert (stats.active_batches, stats.in_flight_items, stats.batches_dispatched) == (0, 0, 0)
    assert (stats.accepted, stats.cancelled) == (1, 1)
    gc.collect()
    assert not [warning for warning in recwarn if issubclass(warning.category, RuntimeWarning)]
    assert_balanced(stats)


@pytest.mark.asyncio
@pytest.mark.skipif(not hasattr(asyncio, "eager_task_factory"), reason="Requires Python 3.12+")
async def test_snapshot_during_eager_processor_includes_its_dispatched_batch():
    snapshots = []

    class SnapshotBatcher(AsyncBatcher):
        async def process_batch(self, batch):
            snapshots.append(self.stats())
            return batch

    loop = asyncio.get_running_loop()
    original = loop.get_task_factory()
    batcher = SnapshotBatcher(max_batch_size=1)
    loop.set_task_factory(asyncio.eager_task_factory)
    try:
        assert await batcher.process(1) == 1
        await batcher.stop()
    finally:
        loop.set_task_factory(original)
        await batcher.stop(force=True)
    stats = snapshots[0]
    assert (stats.pending_items, stats.active_batches, stats.in_flight_items) == (1, 1, 1)
    assert_balanced(stats)


@pytest.mark.asyncio
async def test_base_exception_from_hook_happens_after_batch_cleanup(harness):
    class ObserverAbort(BaseException):
        pass

    def callback(event):
        raise ObserverAbort()

    loop = asyncio.get_running_loop()
    original = loop.get_exception_handler()
    errors = []
    loop.set_exception_handler(lambda _loop, context: errors.append(context["exception"]))
    batcher = harness.create(max_batch_size=1, on_batch=callback)
    try:
        assert await batcher.process(1) == 2
        await batcher.stop()
    finally:
        loop.set_exception_handler(original)
    assert len(errors) == 1
    assert isinstance(errors[0], ObserverAbort)
    stats = batcher.stats()
    assert (stats.active_batches, stats.in_flight_items, stats.pending_items) == (0, 0, 0)
    assert stats.succeeded == 1
    assert_balanced(stats)


async def async_hook(event):
    return None


async def async_generator_hook(event):
    yield event


class AsyncHook:
    async def __call__(self, event):
        return None


class AsyncGeneratorHook:
    async def __call__(self, event):
        yield event


@pytest.mark.parametrize(
    "hook", [42, async_hook, partial(async_hook), AsyncHook(), async_generator_hook, AsyncGeneratorHook()]
)
def test_async_or_noncallable_hooks_are_rejected(hook):
    with pytest.raises(TypeError, match="synchronous callable"):
        RecordingBatcher(on_batch=hook)


@pytest.mark.asyncio
async def test_hook_exception_is_logged_and_does_not_change_results_or_restart(harness, caplog):
    def callback(event):
        raise ValueError("observer failed")

    batcher = harness.create(max_batch_size=1, on_batch=callback)
    for item in (1, 2):
        assert await batcher.process(item) == item * 2
        await batcher.stop()
    assert batcher.stats().succeeded == 2
    assert len([record for record in caplog.records if record.message == "Error in on_batch callback"]) == 2


@pytest.mark.parametrize("error", [KeyboardInterrupt(), SystemExit(), asyncio.CancelledError()])
def test_hook_base_exceptions_are_not_swallowed(error):
    def callback(event):
        raise error

    batcher = RecordingBatcher(on_batch=callback)
    with pytest.raises(type(error)):
        batcher._notify_batch(BatchEvent(1, 1, 0, 0, 1, 0, 0))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "kind", ["coroutine", "custom", "iterator", "future", "failed_future", "cancelled_future", "task"]
)
async def test_unexpected_awaitables_are_cleaned_up_and_logged(harness, caplog, recwarn, kind):
    returned = []

    class CustomAwaitable:
        def __init__(self, coroutine):
            self.coroutine = coroutine

        def __await__(self):
            return self.coroutine.__await__()

    class IteratorAwaitable:
        def __await__(self):
            return iter(())

    def callback(event):
        if kind in {"coroutine", "custom"}:
            value = async_hook(event)
            returned.append(value)
            return value if kind == "coroutine" else CustomAwaitable(value)
        if kind == "iterator":
            return IteratorAwaitable()
        if kind == "task":
            value = asyncio.create_task(async_hook(event))
        else:
            value = asyncio.get_running_loop().create_future()
            if kind == "failed_future":
                value.set_exception(ValueError("unused hook future"))
            elif kind == "cancelled_future":
                value.cancel()
        returned.append(value)
        return value

    batcher = harness.create(max_batch_size=1, on_batch=callback)
    assert await batcher.process(1) == 2
    await batcher.stop()
    await asyncio.sleep(0)
    if kind in {"coroutine", "custom"}:
        assert inspect.getcoroutinestate(returned[0]) == inspect.CORO_CLOSED
    elif kind == "future":
        assert not returned[0].done()
        returned[0].set_result(None)
        await asyncio.sleep(0)
    elif kind == "task":
        assert await returned[0] is None
    assert "on_batch must not return an awaitable" in caplog.text
    returned.clear()
    gc.collect()
    assert not [warning for warning in recwarn if issubclass(warning.category, RuntimeWarning)]
    assert_balanced(batcher.stats())


@pytest.mark.asyncio
async def test_returned_borrowed_request_task_is_not_cancelled(harness, caplog):
    owner = harness.create(GatedBatcher, max_batch_size=1)
    borrowed = harness.submit(owner, 7)[0]
    await owner.started.get()
    observer = harness.create(max_batch_size=1, on_batch=lambda _event: borrowed)
    assert await observer.process(1) == 2
    await observer.stop()
    assert not borrowed.done()
    owner.release.set()
    assert await borrowed == 14
    await owner.stop()
    assert owner.stats().succeeded == 1
    assert owner.stats().cancelled == 0
    assert "on_batch must not return an awaitable" in caplog.text
