"""Application-owned deadlines, shared capacity, and bounded telemetry recipes."""

from __future__ import annotations

import asyncio
import math
from functools import partial
from typing import TYPE_CHECKING

from async_batcher import AsyncBatcher, GroupedBatcher

if TYPE_CHECKING:
    from async_batcher import BatchEvent


class IntakeClosed(RuntimeError):
    """Application intake has permanently closed."""


class ResponseDeadlineExceeded(TimeoutError):
    """The caller stopped waiting; accepted work may still finish."""


class BackendDeadlineExceeded(TimeoutError):
    """The configured async backend deadline expired."""


async def backend_call(processor, items, *, timeout):
    """Bound a cooperative async backend call, preserving its own exceptions."""
    deadline = asyncio.timeout(timeout)
    try:
        async with deadline:
            return await processor(items)
    except TimeoutError as error:
        if deadline.expired():
            raise BackendDeadlineExceeded("Async backend deadline expired") from error
        raise


class Intake:
    """Prevent automatic restart after the application has closed admission."""

    def __init__(self, batcher):
        self.batcher = batcher
        self.accepting = True

    async def process(self, item, *, response_timeout=None):
        if not self.accepting:
            raise IntakeClosed("Application intake is closed")
        deadline = asyncio.timeout(response_timeout)
        try:
            async with deadline:
                return await self.batcher.process(item)
        except TimeoutError as error:
            # A backend TimeoutError is not evidence that this deadline expired.
            if deadline.expired():
                raise ResponseDeadlineExceeded("Response deadline expired") from error
            raise

    def close(self):
        self.accepting = False

    async def shutdown(self, *, grace_timeout, force_timeout):
        """Close intake, try graceful drain, then bounded force cleanup.

        Return whether force was required. A second TimeoutError leaves cleanup
        running and intake closed; the application must decide its next action.
        Neither timeout can interrupt a synchronous backend thread.
        """
        self.close()
        try:
            await self.batcher.stop(timeout=grace_timeout)
        except TimeoutError:
            await self.batcher.stop(force=True, timeout=force_timeout)
            return True
        return False


def async_groups(processor, *, group_key, backend_slots, max_groups=8, batch_size=16):
    """Share one caller-owned semaphore across otherwise independent children."""

    async def process(key, items):
        async with backend_slots:
            return await processor(key, items)

    def factory(key):
        return AsyncBatcher.from_callable(
            partial(process, key), max_batch_size=batch_size, max_queue_size=64, concurrency=2
        )

    return GroupedBatcher(factory, group_key=group_key, max_groups=max_groups)


def thread_groups(processor, *, group_key, executor, max_groups=8, batch_size=16):
    """Borrow one bounded thread pool; its caller owns the eventual thread join."""

    def factory(key):
        return AsyncBatcher.from_callable(
            partial(processor, key),
            executor=executor,
            max_batch_size=batch_size,
            max_queue_size=64,
            concurrency=2,
        )

    return GroupedBatcher(factory, group_key=group_key, max_groups=max_groups)


class BoundedExporter:
    """A lossy, payload-free async exporter with a nonblocking batch hook.

    The queue holds max_events plus at most one event being sent. The send
    callback must cooperate with cancellation and must not block the loop.
    """

    def __init__(self, send, *, max_events=64, send_timeout=1, drain_timeout=1):
        if isinstance(max_events, bool) or not isinstance(max_events, int) or max_events < 1:
            raise ValueError("max_events must be a positive integer")
        for name, value in (("send_timeout", send_timeout), ("drain_timeout", drain_timeout)):
            try:
                valid = (
                    not isinstance(value, bool)
                    and isinstance(value, int | float)
                    and math.isfinite(value)
                    and value > 0
                )
            except (TypeError, OverflowError):
                valid = False
            if not valid:
                raise ValueError(f"{name} must be finite and positive")
        self.send = send
        self.send_timeout = send_timeout
        self.drain_timeout = drain_timeout
        self.sent = 0
        self.failed = 0
        self.dropped = 0
        self._queue = asyncio.Queue(maxsize=max_events)
        self._task = None
        self._closed = False

    async def __aenter__(self):
        if self._task is not None or self._closed:
            raise RuntimeError("Exporter cannot be entered twice")
        self._task = asyncio.create_task(self._run(), name="recipe-telemetry-exporter")
        return self

    async def __aexit__(self, *exception):
        await self.close()

    @property
    def queued_events(self):
        return self._queue.qsize()

    def observe(self, event: BatchEvent):
        if self._closed or self._task is None or self._task.done():
            self.dropped += 1
            return
        try:
            self._queue.put_nowait(event)
        except asyncio.QueueFull:
            self.dropped += 1

    async def _run(self):
        while True:
            event = await self._queue.get()
            try:
                async with asyncio.timeout(self.send_timeout):
                    await self.send(event)
                self.sent += 1
            except asyncio.CancelledError:
                self.dropped += 1
                raise
            except Exception:
                self.failed += 1
            finally:
                self._queue.task_done()

    async def close(self):
        self._closed = True
        if self._task is None:
            return
        try:
            async with asyncio.timeout(self.drain_timeout):
                await self._queue.join()
        except TimeoutError:
            pass
        finally:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
            while not self._queue.empty():
                self._queue.get_nowait()
                self._queue.task_done()
                self.dropped += 1
