from __future__ import annotations

import inspect
from typing import TYPE_CHECKING, TypeVar

from async_batcher.batcher import AsyncBatcher

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Sequence

T = TypeVar("T")
S = TypeVar("S")


class _AsyncCallableBatcher(AsyncBatcher[T, S]):
    def __init__(self, processor: Callable[[list[T]], Awaitable[Sequence[S] | None]], **kwargs):
        super().__init__(**kwargs)
        self._processor = processor

    async def process_batch(self, batch: list[T]) -> Sequence[S] | None:
        return await self._processor(batch)


class _SyncCallableBatcher(AsyncBatcher[T, S]):
    def __init__(self, processor: Callable[[list[T]], Sequence[S] | None], **kwargs):
        super().__init__(**kwargs)
        self._processor = processor

    def process_batch(self, batch: list[T]) -> Sequence[S] | None:
        results = self._processor(batch)
        if inspect.isawaitable(results):
            if inspect.iscoroutine(results):
                results.close()
            raise TypeError("A regular processor must return results directly; use async def to await work")
        return results


def create_batcher(processor, **kwargs):
    if not callable(processor):
        raise TypeError("processor must be callable")
    if inspect.isasyncgenfunction(processor) or inspect.isasyncgenfunction(processor.__call__):
        raise TypeError("processor must return an ordered result collection, not an async generator")
    if inspect.iscoroutinefunction(processor) or inspect.iscoroutinefunction(processor.__call__):
        return _AsyncCallableBatcher(processor, **kwargs)
    return _SyncCallableBatcher(processor, **kwargs)
