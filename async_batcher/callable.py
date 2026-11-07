from __future__ import annotations

import asyncio
import inspect
from contextvars import copy_context
from typing import TYPE_CHECKING, TypeVar

from async_batcher.batcher import AsyncBatcher, _run_sync_batch

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
    def __init__(
        self, processor: Callable[[list[T]], Sequence[S] | Awaitable[Sequence[S] | None] | None], **kwargs
    ):
        super().__init__(**kwargs)
        self._processor = processor

    async def process_batch(self, batch: list[T]) -> Sequence[S] | None:
        loop = asyncio.get_running_loop()
        results = await loop.run_in_executor(
            self.executor, copy_context().run, _run_sync_batch, self._processor, batch
        )
        # Decorators and regular callable objects can return awaitables without
        # being recognized as coroutine functions. Await them on the event loop.
        if inspect.isawaitable(results):
            return await results
        return results


def create_batcher(processor, **kwargs):
    if not callable(processor):
        raise TypeError("processor must be callable")
    if inspect.isasyncgenfunction(processor) or inspect.isasyncgenfunction(processor.__call__):
        raise TypeError("processor must return an ordered result collection, not an async generator")
    if inspect.iscoroutinefunction(processor) or inspect.iscoroutinefunction(processor.__call__):
        return _AsyncCallableBatcher(processor, **kwargs)
    return _SyncCallableBatcher(processor, **kwargs)
