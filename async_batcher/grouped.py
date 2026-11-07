from __future__ import annotations

import asyncio
import inspect
import logging
import math
from dataclasses import dataclass, field
from numbers import Real
from typing import TYPE_CHECKING, Generic, Self, TypeVar

from async_batcher.batcher import AsyncBatcher, _processing_ancestors
from async_batcher.exceptions import AsyncBatchException, QueueFullException
from async_batcher.map import MapOutcome, map_items, map_outcomes

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, AsyncIterable, Callable, Hashable, Iterable
    from types import TracebackType

K = TypeVar("K", bound="Hashable")
T = TypeVar("T")
S = TypeVar("S")


class GroupLimitError(AsyncBatchException):
    """All configured group slots are busy processing accepted work."""


@dataclass(eq=False)
class _Group(Generic[K, T, S]):
    key: K
    batcher: AsyncBatcher[T, S]
    pending: set[asyncio.Task[S]] = field(default_factory=set)
    idle_since: float = 0
    timer: asyncio.TimerHandle | None = None
    closing: bool = False
    close_task: asyncio.Task[None] | None = None
    force_task: asyncio.Task[None] | None = None


class GroupedBatcher(Generic[K, T, S]):
    """Route compatible items to bounded, independently drained child batchers.

    The synchronous factory receives a group key and must return a fresh unused
    AsyncBatcher. Child limits apply per group; max_groups bounds the total number
    of child instances. Idle children are stopped before their slots are reused.
    """

    logger = logging.getLogger(__name__)

    def __init__(
        self,
        factory: Callable[[K], AsyncBatcher[T, S]],
        *,
        group_key: Callable[[T], K],
        max_groups: int = 128,
        max_pending_items: int | None = None,
        idle_timeout: float = 60,
        log_exceptions: bool = True,
    ) -> None:
        if not callable(factory) or not callable(group_key):
            raise TypeError("factory and group_key must be callable")
        if any(
            inspect.iscoroutinefunction(fn) or inspect.iscoroutinefunction(fn.__call__)
            for fn in (factory, group_key)
        ):
            raise TypeError("factory and group_key must be synchronous")
        if not isinstance(log_exceptions, bool):
            raise TypeError("log_exceptions must be a boolean")
        self._log_exceptions = log_exceptions
        if isinstance(max_groups, bool) or not isinstance(max_groups, int) or max_groups < 1:
            raise ValueError("max_groups must be a positive integer")
        if max_pending_items is not None and (
            isinstance(max_pending_items, bool)
            or not isinstance(max_pending_items, int)
            or max_pending_items < 1
        ):
            raise ValueError("max_pending_items must be a positive integer or None")
        try:
            valid_timeout = (
                not isinstance(idle_timeout, bool)
                and isinstance(idle_timeout, Real)
                and math.isfinite(idle_timeout)
                and idle_timeout >= 0
            )
        except OverflowError:
            valid_timeout = False
        if not valid_timeout:
            raise ValueError("idle_timeout must be a finite, nonnegative number")
        self._factory = factory
        self._group_key = group_key
        self.max_groups = max_groups
        self.max_pending_items = max_pending_items
        self._pending_items = 0
        self.idle_timeout = idle_timeout
        self._groups: dict[K, _Group[K, T, S]] = {}
        self._loop: asyncio.AbstractEventLoop | None = None
        self._closed = False
        self._entered = False
        self._shutdown: asyncio.Task[None] | None = None

    @property
    def group_count(self) -> int:
        """Number of allocated groups, including those still stopping."""
        return len(self._groups)

    @property
    def pending_items(self) -> int:
        """Owned submissions, including group cleanup and child admission waits."""
        return self._pending_items

    def _get_loop(self) -> asyncio.AbstractEventLoop:
        loop = asyncio.get_running_loop()
        if self._loop is None:
            self._loop = loop
        elif self._loop is not loop:
            raise RuntimeError("GroupedBatcher must be used from the same event loop")
        return loop

    def _check_open(self) -> None:
        if self._closed:
            raise RuntimeError("GroupedBatcher is closed")

    def _cancel_timer(self, group: _Group[K, T, S]) -> None:
        if group.timer is not None:
            group.timer.cancel()
            group.timer = None

    def _request_done(self, group: _Group[K, T, S], task: asyncio.Task[S]) -> None:
        if task not in group.pending:
            return
        group.pending.remove(task)
        self._pending_items -= 1
        if not task.cancelled():
            # A caller can stop waiting while accepted work continues.
            task.exception()
        if not group.pending and not group.closing and not self._closed:
            group.idle_since = self._loop.time()
            group.timer = self._loop.call_later(self.idle_timeout, self._begin_close, group)

    async def _close_group(self, group: _Group[K, T, S]) -> None:
        # A failed custom stop must not release capacity while its work may
        # still be alive. Keep ownership so a later stop can retry cleanup.
        await group.batcher.stop()
        if group.pending:
            await asyncio.gather(*group.pending, return_exceptions=True)
        if self._groups.get(group.key) is group:
            del self._groups[group.key]

    @staticmethod
    def _failed(task: asyncio.Task[None] | None) -> bool:
        return task is not None and task.done() and (task.cancelled() or task.exception() is not None)

    def _cleanup_done(self, task: asyncio.Task[None]) -> None:
        if not task.cancelled() and (error := task.exception()) is not None:
            self.logger.error(
                "Error stopping an idle batch group", exc_info=error if self._log_exceptions else None
            )

    def _begin_close(self, group: _Group[K, T, S]) -> asyncio.Task[None]:
        if group.close_task is None or self._failed(group.close_task):
            group.closing = True
            self._cancel_timer(group)
            group.close_task = asyncio.create_task(self._close_group(group))
            group.close_task.add_done_callback(self._cleanup_done)
        return group.close_task

    async def _get_group(self, key: K) -> _Group[K, T, S]:
        while True:
            self._check_open()
            group = self._groups.get(key)
            if group is not None:
                if group.closing:
                    await asyncio.shield(self._begin_close(group))
                    continue
                self._cancel_timer(group)
                return group
            if len(self._groups) < self.max_groups:
                batcher = self._factory(key)
                if not isinstance(batcher, AsyncBatcher):
                    if inspect.iscoroutine(batcher):
                        batcher.close()
                    raise TypeError("factory must return an unused AsyncBatcher instance")
                if batcher._loop is not None or any(g.batcher is batcher for g in self._groups.values()):
                    raise ValueError("factory must return a fresh unused batcher for each group")
                group = _Group(key, batcher)
                self._groups[key] = group
                return group
            idle = [group for group in self._groups.values() if not group.pending]
            if not idle:
                raise GroupLimitError("All batch groups are busy; wait or increase max_groups")
            oldest = min(idle, key=lambda group: group.idle_since)
            await asyncio.shield(self._begin_close(oldest))

    def map(
        self,
        source: Iterable[T] | AsyncIterable[T],
        *,
        max_pending: int = 128,
        ordered: bool = True,
    ) -> AsyncGenerator[S, None]:
        """Map a bounded source across groups, failing promptly on any item error."""
        return map_items(self.process, source, max_pending=max_pending, ordered=ordered)

    def map_outcomes(
        self,
        source: Iterable[T] | AsyncIterable[T],
        *,
        max_pending: int = 128,
        ordered: bool = True,
    ) -> AsyncGenerator[MapOutcome[T, S], None]:
        """Map across groups and yield ordinary item failures with input correlation."""
        return map_outcomes(self.process, source, max_pending=max_pending, ordered=ordered)

    async def process(
        self,
        item: T,
        *,
        wait_for_space: bool = False,
        enqueue_timeout: float | None = None,
    ) -> S:
        """Submit to a group, optionally waiting for its child queue admission.

        Busy group slots still reject new keys immediately. Once a child request
        starts, caller cancellation detaches its wait and retains child ownership,
        including any child admission wait, until completion or manager shutdown.
        """
        self._get_loop()
        self._check_open()
        if self.max_pending_items is not None and self._pending_items >= self.max_pending_items:
            raise QueueFullException("GroupedBatcher aggregate pending-item limit reached")
        self._pending_items += 1
        try:
            key = self._group_key(item)
            if inspect.isawaitable(key):
                if inspect.iscoroutine(key):
                    key.close()
                raise TypeError("group_key must return a hashable key directly")
            hash(key)
            group = await self._get_group(key)
            task = asyncio.create_task(
                group.batcher.process(item, wait_for_space=wait_for_space, enqueue_timeout=enqueue_timeout)
            )
            group.pending.add(task)
            task.add_done_callback(lambda completed: self._request_done(group, completed))
        except BaseException:
            # No child owns this reservation yet (including cancelled eviction waits).
            self._pending_items -= 1
            raise
        try:
            return await asyncio.shield(task)
        finally:
            # Eagerly completed children can return before their callback runs.
            # Reconcile now so sequential callers do not see a stale full cap.
            if task.done():
                self._request_done(group, task)

    async def _stop_groups(self) -> None:
        groups = list(self._groups.values())
        results = await asyncio.gather(
            *(self._begin_close(group) for group in groups), return_exceptions=True
        )
        for result in results:
            if isinstance(result, BaseException):
                raise result

    async def stop(self, force: bool = False, timeout: float | None = None) -> None:
        """Permanently close the manager and drain its children; timeout preserves draining."""
        self._get_loop()
        for group in self._groups.values():
            if asyncio.current_task() in group.batcher._processing_tasks or any(
                ancestor() in group.batcher._processing_tasks for ancestor in _processing_ancestors.get()
            ):
                raise RuntimeError("stop() cannot be awaited from a grouped processor")
        self._closed = True
        forcing = []
        if force:
            for group in self._groups.values():
                if group.force_task is None or self._failed(group.force_task):
                    group.force_task = asyncio.create_task(group.batcher.stop(force=True))
                    group.force_task.add_done_callback(self._cleanup_done)
                forcing.append(group.force_task)
        if self._shutdown is None or self._failed(self._shutdown):
            self._shutdown = asyncio.create_task(self._stop_groups())
        waiting = asyncio.gather(self._shutdown, *forcing)
        # Observe late failure even if this particular stop waiter times out.
        waiting.add_done_callback(lambda done: None if done.cancelled() else done.exception())
        await asyncio.wait_for(asyncio.shield(waiting), timeout)

    async def __aenter__(self) -> Self:
        self._get_loop()
        self._check_open()
        if self._entered:
            raise RuntimeError("GroupedBatcher context is already entered")
        self._entered = True
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        try:
            await self.stop()
        finally:
            self._entered = False
