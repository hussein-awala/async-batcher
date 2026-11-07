from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import AsyncIterable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Generic, TypeVar

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Awaitable, Callable, Iterable

T = TypeVar("T")
S = TypeVar("S")
T_co = TypeVar("T_co", covariant=True)
S_co = TypeVar("S_co", covariant=True)


@dataclass(frozen=True)
class MapOutcome(Generic[T_co, S_co]):
    """One indexed input and its result or ordinary request failure.

    ``item`` is the original reference, not a copy. A successful ``None`` result
    has ``error is None`` and ``succeeded is True``.
    """

    index: int
    item: T_co
    result: S_co | None = None
    error: Exception | None = None

    @property
    def succeeded(self) -> bool:
        return self.error is None


def map_items(
    process: Callable[..., Awaitable[S]],
    source: Iterable[T] | AsyncIterable[T],
    *,
    max_pending: int,
    ordered: bool,
) -> AsyncGenerator[S, None]:
    async def submit(index: int, item: T) -> S:
        return await process(item, wait_for_space=True)

    return _validated_map(submit, source, max_pending=max_pending, ordered=ordered)


def map_outcomes(
    process: Callable[..., Awaitable[S]],
    source: Iterable[T] | AsyncIterable[T],
    *,
    max_pending: int,
    ordered: bool,
) -> AsyncGenerator[MapOutcome[T, S], None]:
    async def submit(index: int, item: T) -> MapOutcome[T, S]:
        try:
            result = await process(item, wait_for_space=True)
        except Exception as error:
            return MapOutcome(index, item, error=error)
        return MapOutcome(index, item, result=result)

    return _validated_map(submit, source, max_pending=max_pending, ordered=ordered)


def _validated_map(
    submit: Callable[[int, T], Awaitable[S]],
    source: Iterable[T] | AsyncIterable[T],
    *,
    max_pending: int,
    ordered: bool,
) -> AsyncGenerator[S, None]:
    if isinstance(max_pending, bool) or not isinstance(max_pending, int) or max_pending < 1:
        raise ValueError("max_pending must be a positive integer")
    if not isinstance(ordered, bool):
        raise TypeError("ordered must be a boolean")
    return _map_items(submit, source, max_pending=max_pending, ordered=ordered)


async def _map_items(submit, source, *, max_pending, ordered):
    asynchronous = isinstance(source, AsyncIterable)
    iterator = aiter(source) if asynchronous else iter(source)
    exhausted = StopAsyncIteration if asynchronous else StopIteration
    capacity = asyncio.Semaphore(max_pending)
    changed = asyncio.Event()
    pending = {}
    completed_order = deque()
    closing = False
    failure = None

    async def request(index, item):
        nonlocal failure
        try:
            return await submit(index, item)
        except BaseException as error:
            # Publish before task completion: an eager/ready consumer must not
            # wait for a scheduled done callback to observe an item failure.
            if failure is None:
                failure = error
            raise

    def completed(task):
        nonlocal failure
        try:
            error = task.exception()
        except asyncio.CancelledError as error:
            # A task cancelled before its first step never enters request().
            if failure is None:
                failure = error
        else:
            if failure is None and error is not None:
                failure = error
        if not ordered:
            completed_order.append(task)
        changed.set()

    async def feed():
        index = 0
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
                task = asyncio.create_task(request(index, item))
                index += 1
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
            # Completion publishes failure once, independent of window size.
            if failure is not None:
                raise failure
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
