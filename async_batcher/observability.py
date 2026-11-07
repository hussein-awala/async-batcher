"""Payload-free operational snapshots and batch completion events."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class BatcherStats:
    """One immutable view of gauges and lifetime totals for a batcher.

    Terminal totals describe accepted request futures, not backend side effects.
    Totals survive stop/restart. Active batches include completion cleanup, and
    in-flight items can include requests whose callers already cancelled.
    """

    queued_items: int
    pending_items: int
    in_flight_items: int
    active_batches: int
    accepted: int
    rejected: int
    succeeded: int
    failed: int
    cancelled: int
    batches_dispatched: int
    batches_completed: int


@dataclass(frozen=True, slots=True)
class BatchEvent:
    """Completion of one dispatched batch, without items or exception objects.

    queue_wait_seconds is the oldest item's residence before processing starts,
    or before completion if the worker never starts. processing_seconds is None
    for a worker cancelled before it starts; otherwise it includes processor
    execution and result validation, but excludes event callback execution.
    Outcome counts describe request futures, including prior caller cancellation.
    """

    batch_id: int
    size: int
    queue_wait_seconds: float
    processing_seconds: float | None
    succeeded: int
    failed: int
    cancelled: int


@dataclass(slots=True)
class _BatchTiming:
    batch_id: int
    started_at: float | None = None
    finished_at: float | None = None
