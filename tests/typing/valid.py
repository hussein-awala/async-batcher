"""Strict consumer checks against an installed wheel; never executed as tests."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, AsyncIterator, Awaitable, Iterator
from contextvars import Context
from fractions import Fraction
from typing import assert_type

from async_batcher import AsyncBatcher, BatcherStats, BatchEvent, GroupedBatcher, GroupLimitError, MapOutcome


class AsyncDouble(AsyncBatcher[int, int]):
    async def process_batch(self, batch: list[int]) -> list[int]:
        return [item * 2 for item in batch]


class SyncText(AsyncBatcher[int, str]):
    def process_batch(self, batch: list[int]) -> list[str]:
        return [str(item) for item in batch]


class MixedDouble(AsyncBatcher[int, int]):
    async def process_batch(self, batch: list[int]) -> list[int | ValueError | asyncio.CancelledError]:
        return [item * 2 if item >= 0 else ValueError("negative") for item in batch]


def render(items: list[int]) -> list[str]:
    return [str(item) for item in items]


async def async_render(items: list[int]) -> list[str]:
    return render(items)


def wrapped(items: list[int]) -> Awaitable[list[str]]:
    return async_render(items)


class AsyncRenderer:
    async def __call__(self, items: list[int]) -> list[str]:
        return render(items)


def discard(items: list[int]) -> None:
    return None


async def async_discard(items: list[int]) -> None:
    return None


def optional(items: list[int]) -> list[str] | None:
    return render(items) if items else None


async def async_optional(items: list[int]) -> list[str] | None:
    return optional(items)


def per_item_errors(items: list[int]) -> list[str | ValueError]:
    return [str(item) if item >= 0 else ValueError("negative") for item in items]


async def async_per_item_errors(items: list[int]) -> list[str | Exception | asyncio.CancelledError]:
    return [str(item) if item >= 0 else asyncio.CancelledError() for item in items]


class SizedResults:
    def __init__(self, items: list[str]) -> None:
        self.items = items

    def __len__(self) -> int:
        return len(self.items)

    def __iter__(self) -> Iterator[str]:
        return iter(self.items)


def sized_results(items: list[int]) -> SizedResults:
    return SizedResults(render(items))


def on_batch(event: BatchEvent) -> None:
    assert_type(event.batch_id, int)
    assert_type(event.size, int)
    assert_type(event.queue_wait_seconds, float)
    assert_type(event.processing_seconds, float | None)
    assert_type(event.succeeded, int)


def numeric_cost(item: int) -> int:
    return item


def fractional_cost(item: int) -> Fraction:
    return Fraction(item, 2)


sync_batcher = AsyncBatcher.from_callable(
    render, max_batch_size=16, on_batch=on_batch, processor_context=Context()
)
async_batcher = AsyncBatcher.from_callable(async_render)
wrapped_batcher = AsyncBatcher.from_callable(wrapped)
object_batcher = AsyncBatcher.from_callable(AsyncRenderer())
assert_type(sync_batcher, AsyncBatcher[int, str])
assert_type(async_batcher, AsyncBatcher[int, str])
assert_type(wrapped_batcher, AsyncBatcher[int, str])
assert_type(object_batcher, AsyncBatcher[int, str])
assert_type(AsyncBatcher.from_callable(discard), AsyncBatcher[int, None])
assert_type(AsyncBatcher.from_callable(async_discard), AsyncBatcher[int, None])
assert_type(AsyncBatcher.from_callable(optional), AsyncBatcher[int, str | None])
assert_type(AsyncBatcher.from_callable(async_optional), AsyncBatcher[int, str | None])
assert_type(AsyncBatcher.from_callable(per_item_errors), AsyncBatcher[int, str])
assert_type(AsyncBatcher.from_callable(async_per_item_errors), AsyncBatcher[int, str])
assert_type(AsyncBatcher.from_callable(sized_results), AsyncBatcher[int, str])

integer_cost = AsyncDouble(max_batch_cost=5, item_cost=numeric_cost, on_batch=on_batch)
# Obsolete timing options are ignored at runtime, regardless of their values.
legacy = AsyncDouble(batch_size=2, sleep_time=None, buffering_time="ignored")
legacy_factory = AsyncBatcher.from_callable(render, sleep_time=None, buffering_time=object())
assert_type(legacy_factory, AsyncBatcher[int, str])
fraction_cost = AsyncBatcher.from_callable(render, max_batch_cost=Fraction(5), item_cost=fractional_cost)
assert_type(fraction_cost, AsyncBatcher[int, str])
wide_result: AsyncBatcher[int, object] = sync_batcher


def key(item: int) -> str:
    return str(item % 2)


def factory(group: str) -> AsyncBatcher[int, str]:
    return AsyncBatcher.from_callable(render)


grouped = GroupedBatcher(factory, group_key=key, max_groups=4)
assert_type(grouped, GroupedBatcher[str, int, str])
assert_type(grouped.group_count, int)
error: Exception = GroupLimitError("full")


async def source() -> AsyncIterator[int]:
    yield 1


async def consume() -> None:
    assert_type(await sync_batcher.process(1, wait_for_space=True, enqueue_timeout=0.1), str)
    assert_type(await integer_cost.process(1), int)
    assert_type(await MixedDouble().process(1), int)
    assert_type(sync_batcher.map([1, 2]), AsyncGenerator[str, None])
    assert_type(sync_batcher.map(source(), max_pending=4, ordered=False), AsyncGenerator[str, None])
    async with integer_cost as entered:
        assert_type(entered, AsyncDouble)
        assert_type(await entered.is_running(), bool)
    async with grouped as manager:
        assert_type(manager, GroupedBatcher[str, int, str])
        assert_type(await manager.process(1), str)
    stats = sync_batcher.stats()
    assert_type(stats, BatcherStats)
    assert_type(stats.pending_items, int)
    assert_type(stats.accepted, int)
    assert_type(stats.admission_waiters, int)
    assert_type(stats.oldest_pending_seconds, float | None)
    assert_type(await sync_batcher.stop(), None)
    assert_type(await sync_batcher.run(), None)


async def consume_outcomes() -> None:
    assert_type(sync_batcher.map_outcomes([1, 2]), AsyncGenerator[MapOutcome[int, str], None])
    async for outcome in sync_batcher.map_outcomes(source(), ordered=False):
        assert_type(outcome.index, int)
        assert_type(outcome.item, int)
        assert_type(outcome.result, str | None)
        assert_type(outcome.error, Exception | None)
        assert_type(outcome.succeeded, bool)


async def grouped_streams() -> None:
    assert_type(await grouped.process(1, wait_for_space=True, enqueue_timeout=0.1), str)
    assert_type(grouped.map([1]), AsyncGenerator[str, None])
    assert_type(grouped.map_outcomes(source()), AsyncGenerator[MapOutcome[int, str], None])
