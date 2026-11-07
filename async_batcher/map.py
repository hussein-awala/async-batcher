from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import AsyncIterable
from typing import TYPE_CHECKING, TypeVar

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Iterable

    from async_batcher.batcher import AsyncBatcher

T = TypeVar("T")
S = TypeVar("S")


def map_items(
    batcher: AsyncBatcher[T, S],
    source: Iterable[T] | AsyncIterable[T],
    *,
    max_pending: int,
    ordered: bool,
) -> AsyncGenerator[S, None]:
    if isinstance(max_pending, bool) or not isinstance(max_pending, int) or max_pending < 1:
        raise ValueError("max_pending must be a positive integer")
    if not isinstance(ordered, bool):
        raise TypeError("ordered must be a boolean")
    return _map_items(batcher, source, max_pending=max_pending, ordered=ordered)


async def _map_items(batcher, source, *, max_pending, ordered):
    asynchronous = isinstance(source, AsyncIterable)
    iterator = aiter(source) if asynchronous else iter(source)
    exhausted = StopAsyncIteration if asynchronous else StopIteration
    capacity = asyncio.Semaphore(max_pending)
    changed = asyncio.Event()
    pending = {}
    completed_order = deque()
    closing = False

    def completed(task):
        if not ordered:
            completed_order.append(task)
        changed.set()

    async def feed():
        try:
            while True:
                # A slot covers source consumption, admission, processing, and
                # any completed result still waiting to be yielded.
                await capacity.acquire()
                try:
                    item = await anext(iterator) if asynchronous else next(iterator)
                except exhausted:
                    break
                if closing:
                    break
                task = asyncio.create_task(batcher.process(item, wait_for_space=True))
                pending[task] = None
                task.add_done_callback(completed)
                changed.set()
        finally:
            changed.set()

    feeder = asyncio.create_task(feed())
    try:
        while True:
            changed.clear()
            if feeder.done():
                feeder.result()
            # Fail promptly even when a preceding ordered result or the source
            # is blocked. Cleanup below retrieves all other task exceptions.
            for task in pending:
                if task.done() and (task.cancelled() or task.exception() is not None):
                    task.result()
            ready = (
                next(iter(pending), None)
                if ordered
                else completed_order.popleft()
                if completed_order
                else None
            )
            if ready is not None and ready.done():
                del pending[ready]
                capacity.release()
                yield ready.result()
            elif feeder.done() and not pending:
                break
            else:
                await changed.wait()
    finally:
        closing = True
        feeder.cancel()
        for task in pending:
            task.cancel()
        try:
            await asyncio.gather(feeder, *pending, return_exceptions=True)
        finally:
            close = getattr(iterator, "aclose" if asynchronous else "close", None)
            if close is not None:
                if asynchronous:
                    await close()
                else:
                    close()
