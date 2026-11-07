"""Every marked line must fail with exactly the expected diagnostic code."""

from __future__ import annotations

from typing import TYPE_CHECKING

from async_batcher import AsyncBatcher, BatchEvent, CoalescingBatcher, GroupedBatcher

if TYPE_CHECKING:
    from collections.abc import AsyncIterator


class Double(AsyncBatcher[int, int]):
    async def process_batch(self, batch: list[int]) -> list[int]:
        return batch


def render(items: list[int]) -> list[str]:
    return [str(item) for item in items]


def wrong_hook(event: str) -> None:
    return None


def wrong_cost(item: str) -> int:
    return len(item)


async def async_hook(event: BatchEvent) -> None:
    return None


batcher = AsyncBatcher.from_callable(render)
Double(max_batch_size="ten")  # error: arg-type
Double(log_exceptions="no")  # error: arg-type
Double(processor_context={})  # error: arg-type
Double(on_batch=wrong_hook)  # error: arg-type
Double(item_cost=wrong_cost)  # error: arg-type
Double(on_batch=async_hook)  # error: arg-type
AsyncBatcher.from_callable(render, typo=True)  # error: call-overload, call-overload
AsyncBatcher.from_callable(render, max_batch_cost="five")  # error: call-overload
AsyncBatcher.from_callable(render, on_batch=wrong_hook)  # error: arg-type
AsyncBatcher.from_callable(render, item_cost=wrong_cost)  # error: misc
batcher.stats().accepted = 2  # error: misc
unknown = batcher.stats().unknown  # error: attr-defined
event = BatchEvent(1, 1, 0, None, 1, 0, 0)
event.size = 4  # error: misc


def key(item: int) -> str:
    return str(item)


def factory(group: str) -> AsyncBatcher[int, str]:
    return AsyncBatcher.from_callable(render)


grouped: GroupedBatcher[str, int, str] = GroupedBatcher(factory, group_key=key)
wrong_group: GroupedBatcher[int, int, str] = grouped  # error: assignment
wrong_input: AsyncBatcher[object, str] = batcher  # error: assignment


async def strings() -> AsyncIterator[str]:
    yield "bad"


async def consume() -> None:
    await batcher.flush(timeout="bad")  # error: arg-type
    await batcher.process("bad")  # error: arg-type
    await batcher.process(1, enqueue_timeout="soon")  # error: arg-type
    batcher.map(["bad"])  # error: list-item
    batcher.map(strings())  # error: arg-type
    batcher.map([1], max_pending="many")  # error: arg-type
    _result: int = await batcher.process(1)  # error: assignment
    await grouped.process("bad")  # error: arg-type
    _grouped_result: int = await grouped.process(1)  # error: assignment


async def invalid_outcomes() -> None:
    batcher.map_outcomes(["bad"])  # error: list-item
    batcher.map_outcomes(strings())  # error: arg-type
    async for outcome in batcher.map_outcomes([1]):
        outcome.index = 1  # error: misc
        _wrong: str = outcome.item  # error: assignment


async def invalid_grouped_streams() -> None:
    grouped.map(["bad"])  # error: list-item
    grouped.map_outcomes(strings())  # error: arg-type
    await grouped.process(1, enqueue_timeout="soon")  # error: arg-type


GroupedBatcher(factory, group_key=key, max_pending_items="many")  # error: arg-type
grouped.pending_items = 9  # error: misc


coalesced = CoalescingBatcher(batcher, key=key)
CoalescingBatcher(batcher, key=key, max_keys="many")  # error: arg-type
coalesced.waiting_callers = 9  # error: misc


async def invalid_coalescing() -> None:
    await coalesced.process("bad")  # error: arg-type
    _wrong: int = await coalesced.process(1)  # error: assignment
