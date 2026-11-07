from __future__ import annotations

import asyncio
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest
from async_batcher import AsyncBatcher, BatchEvent
from examples.recipes.operations import (
    BackendDeadlineExceeded,
    BoundedExporter,
    Intake,
    IntakeClosed,
    ResponseDeadlineExceeded,
    async_groups,
    backend_call,
    thread_groups,
)

from tests.core_helpers import wait_until


@pytest.mark.asyncio
async def test_response_deadline_does_not_claim_backend_work_was_retracted():
    started, release = asyncio.Event(), asyncio.Event()
    completed = []

    async def process(items):
        started.set()
        await release.wait()
        completed.extend(items)
        return items

    batcher = AsyncBatcher.from_callable(process, max_batch_size=1)
    intake = Intake(batcher)
    request = asyncio.create_task(intake.process("accepted", response_timeout=0.01))
    try:
        await asyncio.wait_for(started.wait(), 2)
        with pytest.raises(ResponseDeadlineExceeded):
            await request
        assert batcher.stats().in_flight_items == 1
        release.set()
        assert await intake.shutdown(grace_timeout=2, force_timeout=2) is False
        assert completed == ["accepted"]
        with pytest.raises(IntakeClosed):
            await intake.process("too late")
    finally:
        release.set()
        await batcher.stop(force=True, timeout=2)
        await asyncio.gather(request, return_exceptions=True)


@pytest.mark.asyncio
async def test_backend_timeout_is_not_misclassified_as_a_response_deadline():
    failure = TimeoutError("backend raised its own timeout")

    async def process(items):
        raise failure

    async with AsyncBatcher.from_callable(process, max_batch_size=1) as batcher:
        with pytest.raises(TimeoutError) as caught:
            await Intake(batcher).process(1, response_timeout=2)
        assert caught.value is failure


@pytest.mark.asyncio
async def test_backend_deadline_cancels_its_cooperative_call_and_is_distinct():
    cleaned = asyncio.Event()

    async def backend(items):
        try:
            await asyncio.Event().wait()
        finally:
            cleaned.set()

    async def process(items):
        return await backend_call(backend, items, timeout=0.01)

    async with AsyncBatcher.from_callable(process, max_batch_size=1) as batcher:
        with pytest.raises(BackendDeadlineExceeded):
            await Intake(batcher).process(1, response_timeout=2)
        assert cleaned.is_set()


@pytest.mark.asyncio
async def test_intake_stays_closed_when_grace_and_forced_cleanup_budgets_expire():
    started, cleaning, release_cleanup = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def process(items):
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            cleaning.set()
            await release_cleanup.wait()

    batcher = AsyncBatcher.from_callable(process, max_batch_size=1)
    intake = Intake(batcher)
    request = asyncio.create_task(intake.process(1))
    try:
        await asyncio.wait_for(started.wait(), 2)
        with pytest.raises(TimeoutError):
            await intake.shutdown(grace_timeout=0.01, force_timeout=0.01)
        assert cleaning.is_set()
        with pytest.raises(IntakeClosed):
            await intake.process(2)
        release_cleanup.set()
        await batcher.stop(force=True, timeout=2)
        assert request.cancelled()
        with pytest.raises(IntakeClosed):
            await intake.process(3)
    finally:
        release_cleanup.set()
        await batcher.stop(force=True, timeout=2)
        await asyncio.gather(request, return_exceptions=True)


@pytest.mark.asyncio
async def test_multiple_groups_share_one_async_backend_concurrency_budget():
    release = asyncio.Event()
    active = peak = 0

    async def process(key, items):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        try:
            assert all(item[0] == key for item in items)
            await release.wait()
            return [item[1] * 2 for item in items]
        finally:
            active -= 1

    async with async_groups(
        process, group_key=lambda item: item[0], backend_slots=asyncio.Semaphore(2), batch_size=1
    ) as groups:
        requests = [asyncio.create_task(groups.process((str(index), index))) for index in range(6)]
        try:
            await wait_until(lambda: active == 2)
            assert groups.group_count == 6
            assert peak == 2
            release.set()
            assert await asyncio.wait_for(asyncio.gather(*requests), 2) == [0, 2, 4, 6, 8, 10]
            assert peak == 2
        finally:
            release.set()
            for request in requests:
                request.cancel()
            await asyncio.gather(*requests, return_exceptions=True)


def test_forced_group_shutdown_does_not_own_or_join_a_borrowed_thread():
    started, release, finished = threading.Event(), threading.Event(), threading.Event()
    client_closed = False
    threads = []

    class TrackedPool(ThreadPoolExecutor):
        submitted = 0

        def submit(self, fn, /, *args, **kwargs):
            self.submitted += 1
            return super().submit(fn, *args, **kwargs)

    def process(key, items):
        threads.append(threading.current_thread().name)
        started.set()
        assert release.wait(5), "test did not release the worker"
        assert not client_closed
        if len(threads) == 2:
            finished.set()
        return items

    async def run(executor):
        groups = thread_groups(process, group_key=lambda item: item, executor=executor, batch_size=1)
        requests = [asyncio.create_task(groups.process(tenant)) for tenant in ("tenant-a", "tenant-b")]
        try:
            assert await asyncio.to_thread(started.wait, 2)
            await wait_until(lambda: executor.submitted == 2)
            assert len(threads) == 1  # The second group shares the occupied worker slot.
            await groups.stop(force=True, timeout=2)
            await asyncio.gather(*requests, return_exceptions=True)
            assert all(request.cancelled() for request in requests)
            assert not finished.is_set()
            # A stopped manager has not closed the caller's pool.
            assert executor.submit(lambda: "still open") is not None
        finally:
            release.set()
            await groups.stop(force=True, timeout=2)
            await asyncio.gather(*requests, return_exceptions=True)

    with TrackedPool(max_workers=1, thread_name_prefix="caller-owned") as executor:
        asyncio.run(run(executor))
    client_closed = True
    assert finished.is_set()
    assert all(name.startswith("caller-owned") for name in threads)


@pytest.mark.asyncio
async def test_slow_telemetry_drops_overflow_without_stalling_batch_results():
    started, release = asyncio.Event(), asyncio.Event()
    exported = []

    async def send(event):
        started.set()
        await release.wait()
        exported.append(event)

    async with BoundedExporter(send, max_events=1) as exporter:
        try:
            async with AsyncBatcher.from_callable(
                lambda items: items, max_batch_size=1, on_batch=exporter.observe
            ) as batcher:
                assert await batcher.process("private-first") == "private-first"
                await asyncio.wait_for(started.wait(), 2)
                assert await batcher.process("private-second") == "private-second"
                assert await batcher.process("private-third") == "private-third"
                await wait_until(lambda: exporter.dropped == 1)
                assert exporter.queued_events == 1
                assert exporter.dropped == 1
                assert batcher.stats().succeeded == 3
        finally:
            release.set()
    assert exporter.sent == 2 and exporter.failed == 0
    assert all(isinstance(event, BatchEvent) for event in exported)
    assert all("private" not in repr(event) for event in exported)


@pytest.mark.asyncio
async def test_exporter_counts_errors_and_continues_with_later_events():
    calls = []

    async def send(event):
        calls.append(event)
        if len(calls) == 1:
            raise OSError("telemetry unavailable")

    async with BoundedExporter(send) as exporter:
        async with AsyncBatcher.from_callable(
            lambda items: items, max_batch_size=1, on_batch=exporter.observe
        ) as batcher:
            await batcher.process(1)
            await batcher.process(2)
    assert exporter.failed == 1 and exporter.sent == 1 and exporter.dropped == 0


@pytest.mark.asyncio
async def test_exporter_shutdown_counts_unsent_events_and_cleans_up_its_task():
    started = asyncio.Event()

    async def send(event):
        started.set()
        await asyncio.Event().wait()

    event = BatchEvent(1, 1, 0, 0, 1, 0, 0)
    before = asyncio.all_tasks()
    async with BoundedExporter(send, max_events=1, send_timeout=2, drain_timeout=0.01) as exporter:
        exporter.observe(event)
        await asyncio.wait_for(started.wait(), 2)
        exporter.observe(event)
        exporter.observe(event)
    assert exporter.dropped == 3 and exporter.sent == 0 and exporter.queued_events == 0
    assert asyncio.all_tasks() == before


def test_exporter_rejects_an_unbounded_queue_or_deadline():
    with pytest.raises(ValueError):
        BoundedExporter(None, max_events=0)
    with pytest.raises(ValueError):
        BoundedExporter(None, send_timeout=None)
    with pytest.raises(ValueError):
        BoundedExporter(None, drain_timeout=float("inf"))
