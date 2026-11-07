from __future__ import annotations

import asyncio
import inspect
from dataclasses import dataclass
from typing import TYPE_CHECKING, Generic, Self, TypeVar

from async_batcher.batcher import AsyncBatcher, _processing_ancestors
from async_batcher.exceptions import QueueFullException

if TYPE_CHECKING:
    from collections.abc import Callable, Hashable
    from types import TracebackType

K = TypeVar("K", bound="Hashable")
T = TypeVar("T")
S = TypeVar("S")


@dataclass(eq=False)
class _Request(Generic[S]):
    future: asyncio.Future[S]
    task: asyncio.Task[S] | None = None


class CoalescingBatcher(Generic[K, T, S]):
    """Share bounded in-flight requests by explicit key, without caching results.

    The first input for a key is submitted to a borrowed AsyncBatcher. Callers
    joining that key receive the same result object or exception. Cancellation
    detaches only that caller; the request remains owned until it completes.
    """

    def __init__(
        self,
        batcher: AsyncBatcher[T, S],
        *,
        key: Callable[[T], K],
        max_keys: int = 128,
        max_waiters: int = 1024,
    ) -> None:
        if not isinstance(batcher, AsyncBatcher):
            raise TypeError("batcher must be an AsyncBatcher instance")
        if not callable(key):
            raise TypeError("key must be callable")
        if inspect.iscoroutinefunction(key) or inspect.iscoroutinefunction(key.__call__):
            raise TypeError("key must be synchronous")
        for name, value in (("max_keys", max_keys), ("max_waiters", max_waiters)):
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise ValueError(f"{name} must be a positive integer")
        self._batcher = batcher
        self._key = key
        self.max_keys = max_keys
        self.max_waiters = max_waiters
        self._requests: dict[K, _Request[S]] = {}
        self._waiters = 0
        self._loop: asyncio.AbstractEventLoop | None = None
        self._closed = False
        self._entered = False
        self._shutdown: asyncio.Task[None] | None = None

    @property
    def in_flight_keys(self) -> int:
        return len(self._requests)

    @property
    def waiting_callers(self) -> int:
        return self._waiters

    def _get_loop(self) -> asyncio.AbstractEventLoop:
        loop = asyncio.get_running_loop()
        if self._loop is None:
            self._loop = loop
        elif loop is not self._loop:
            raise RuntimeError("CoalescingBatcher must be used from the same event loop")
        return loop

    def _check_open(self) -> None:
        if self._closed:
            raise RuntimeError("CoalescingBatcher is closed")

    def _complete(self, key: K, request: _Request[S], task: asyncio.Task[S]) -> None:
        if request.future.done():
            return
        if task.cancelled():
            request.future.cancel()
        elif (error := task.exception()) is not None:
            request.future.set_exception(error)
        else:
            request.future.set_result(task.result())
        if self._requests.get(key) is request:
            del self._requests[key]

    async def process(self, item: T) -> S:
        """Join an in-flight key or submit its first item; reject full wrapper bounds."""
        loop = self._get_loop()
        self._check_open()
        if self._waiters >= self.max_waiters:
            raise QueueFullException("CoalescingBatcher waiting-caller limit reached")
        self._waiters += 1
        try:
            key = self._key(item)
            if inspect.isawaitable(key):
                if inspect.iscoroutine(key):
                    key.close()
                raise TypeError("key must return a hashable key directly")
            hash(key)
            request = self._requests.get(key)
            if request is not None and request.task is not None and request.task.done():
                self._complete(key, request, request.task)
                request = None
            if request is None:
                if len(self._requests) >= self.max_keys:
                    raise QueueFullException("CoalescingBatcher in-flight key limit reached")
                request = _Request(loop.create_future())
                # Observe abandoned errors without taking ownership of callers.
                request.future.add_done_callback(lambda done: None if done.cancelled() else done.exception())
                self._requests[key] = request
                coroutine = self._batcher.process(item)
                try:
                    task = asyncio.create_task(coroutine)
                except BaseException:
                    coroutine.close()
                    del self._requests[key]
                    request.future.cancel()
                    raise
                request.task = task
                task.add_done_callback(lambda done: self._complete(key, request, done))
                if task.done():
                    self._complete(key, request, task)
            return await asyncio.shield(request.future)
        finally:
            self._waiters -= 1

    async def _drain(self) -> None:
        requests = tuple(self._requests.items())
        tasks = [request.task for _, request in requests if request.task is not None]
        await asyncio.gather(*tasks, return_exceptions=True)
        for key, request in requests:
            if request.task is not None:
                self._complete(key, request, request.task)

    async def aclose(self) -> None:
        """Permanently close this wrapper and drain requests; never stop its borrowed batcher."""
        self._get_loop()
        if asyncio.current_task() in self._batcher._processing_tasks or any(
            ancestor() in self._batcher._processing_tasks for ancestor in _processing_ancestors.get()
        ):
            raise RuntimeError("aclose() cannot be awaited from a coalesced processor")
        self._closed = True
        if self._shutdown is None:
            self._shutdown = asyncio.create_task(self._drain())
        await asyncio.shield(self._shutdown)

    async def __aenter__(self) -> Self:
        self._get_loop()
        self._check_open()
        if self._entered:
            raise RuntimeError("CoalescingBatcher context is already entered")
        self._entered = True
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        try:
            await self.aclose()
        finally:
            self._entered = False
