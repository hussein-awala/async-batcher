"""Keyword types shared by the public constructor and callable factory."""

from __future__ import annotations

from typing import TYPE_CHECKING, Generic, TypedDict, TypeVar

if TYPE_CHECKING:
    from collections.abc import Callable
    from concurrent.futures import Executor
    from contextvars import Context
    from numbers import Real

    from async_batcher.observability import BatchEvent

T = TypeVar("T")


class LegacyOptions(TypedDict, total=False):
    batch_size: int
    sleep_time: object
    buffering_time: object


class BatcherOptions(LegacyOptions, Generic[T], total=False):
    max_batch_size: int
    max_queue_time: float
    concurrency: int
    max_queue_size: int
    max_batch_cost: float | Real | None
    item_cost: Callable[[T], float | Real] | None
    executor: Executor | None
    on_batch: Callable[[BatchEvent], None] | None
    processor_context: Context | None
    log_exceptions: bool
