from __future__ import annotations

import asyncio
from unittest.mock import patch

import pytest
from async_batcher import BatcherStats

from tests.core_helpers import GatedBatcher, wait_until


def test_old_snapshot_construction_keeps_new_field_defaults():
    stats = BatcherStats(*([0] * 11))
    assert stats.admission_waiters == 0
    assert stats.oldest_pending_seconds is None
    assert stats.state == "idle"


@pytest.mark.asyncio
async def test_age_tracks_oldest_unresolved_result_not_cancelled_backend_work(harness):
    batcher = harness.create(GatedBatcher, max_batch_size=1)
    first = harness.submit(batcher, 1)[0]
    await batcher.started.get()
    accepted_at = next(iter(batcher._pending_since.values()))
    with patch.object(batcher._loop, "time", return_value=accepted_at + 7):
        before = batcher.stats()
        assert before.oldest_pending_seconds == pytest.approx(7)
    first.cancel()
    # The future is terminal immediately, before bookkeeping callbacks execute.
    after = batcher.stats()
    assert after.oldest_pending_seconds is None
    assert after.pending_items == 0 and after.in_flight_items == 1
    assert before.oldest_pending_seconds == pytest.approx(7)
    await asyncio.gather(first, return_exceptions=True)
    second = harness.submit(batcher, 2)[0]
    await wait_until(lambda: batcher.stats().pending_items == 1)
    assert batcher.stats().oldest_pending_seconds >= 0
    batcher.release.set()
    assert await second == 4
    await batcher.stop()
    assert not batcher._pending_since
    assert batcher.stats().oldest_pending_seconds is None


@pytest.mark.asyncio
@pytest.mark.parametrize("ending", ["admitted", "cancelled", "timeout", "shutdown"])
async def test_waiting_producers_leave_gauge_on_every_admission_exit(harness, ending):
    batcher = harness.create(GatedBatcher, max_batch_size=1, max_queue_size=1)
    harness.submit(batcher, 1)
    await batcher.started.get()
    harness.submit(batcher, 2)
    await wait_until(lambda: batcher.stats().queued_items == 1)
    waiting = harness.spawn(
        batcher.process(3, wait_for_space=True, enqueue_timeout=0.05 if ending == "timeout" else None)
    )
    await wait_until(lambda: batcher.stats().admission_waiters == 1)
    stats = batcher.stats()
    assert stats.accepted == stats.pending_items == 2
    if ending == "admitted":
        batcher.release.set()
        assert await waiting == 6
    elif ending == "cancelled":
        waiting.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiting
    elif ending == "timeout":
        with pytest.raises(TimeoutError):
            await waiting
    else:
        await batcher.stop(force=True)
        with pytest.raises(RuntimeError, match="stopping"):
            await waiting
    assert batcher.stats().admission_waiters == 0
    assert batcher.stats().accepted == (3 if ending == "admitted" else 2)
    assert batcher.stats().rejected == (1 if ending in {"timeout", "shutdown"} else 0)


@pytest.mark.asyncio
async def test_partial_batch_age_and_states_cover_stop_restart_and_callback(harness):
    snapshots = []
    batcher = harness.create(
        max_batch_size=4, max_queue_time=3600, on_batch=lambda _: snapshots.append(batcher.stats())
    )
    assert batcher.stats().state == "idle"
    request = harness.submit(batcher, 1)[0]
    await wait_until(lambda: batcher.stats().pending_items == 1 and batcher.stats().queued_items == 0)
    snapshot = batcher.stats()
    assert snapshot.state == "running"
    assert snapshot.oldest_pending_seconds >= 0
    stopping = harness.spawn(batcher.stop())
    await wait_until(lambda: batcher._stop.is_set())
    assert batcher.stats().state == "stopping"
    await stopping
    assert await request == 2
    assert batcher.stats().state == "stopped"
    assert snapshots[0].oldest_pending_seconds is None
    assert snapshots[0].pending_items == 0
    restarted = harness.submit(batcher, 2)[0]
    await wait_until(lambda: batcher.stats().state == "running")
    await batcher.stop()
    assert await restarted == 4
    assert batcher.stats().state == "stopped"


@pytest.mark.asyncio
async def test_empty_stop_and_explicit_run_report_lifecycle_without_requests(harness):
    batcher = harness.create()
    await batcher.stop()
    assert batcher.stats().state == "stopped"
    # A fresh explicit dispatcher is running even before a request arrives.
    other = harness.create()
    running = harness.spawn(other.run())
    await wait_until(lambda: other.stats().state == "running")
    assert other.stats().oldest_pending_seconds is None
    await other.stop()
    await running
    assert other.stats().state == "stopped"
