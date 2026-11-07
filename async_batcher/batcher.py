from __future__ import annotations

import abc
import asyncio
import logging
import math
import warnings
from collections import namedtuple
from functools import partial
from numbers import Real
from typing import TYPE_CHECKING, Generic, TypeVar

from async_batcher.exceptions import QueueFullException

if TYPE_CHECKING:
    from concurrent.futures import Executor

T = TypeVar("T")
S = TypeVar("S")


class AsyncBatcher(Generic[T, S], abc.ABC):
    """Group items by size or age and process batches asynchronously.

    Args:
        max_batch_size: Maximum items per batch, or -1 for no limit. Defaults to -1.
        max_queue_time: Maximum seconds to collect a partial batch, measured from
            its oldest item's arrival. A busy processor can delay dispatch beyond
            this limit. Defaults to 0.01.
        max_queue_size: Maximum queued items, or -1/0 for no limit. Defaults to -1.
        concurrency: Maximum concurrent batches, or -1 for no limit. Defaults to 1.
        executor: Executor for synchronous processors; None uses asyncio's default.
    """

    logger = logging.getLogger(__name__)
    QueueItem = namedtuple("QueueItem", ["item", "future", "enqueued_at"])

    def __init__(
        self,
        *,
        max_batch_size: int = -1,
        max_queue_time: float = 0.01,
        concurrency: int = 1,
        max_queue_size: int = -1,
        executor: Executor | None = None,
        **kwargs,
    ):
        super().__init__()
        for argument in ("sleep_time", "buffering_time"):
            if argument in kwargs:
                kwargs.pop(argument)
                warnings.warn(
                    f"The '{argument}' has no effect and will be removed in a future version.",
                    stacklevel=2,
                )
        if "batch_size" in kwargs:
            warnings.warn(
                "The 'batch_size' argument is deprecated and will be removed in a future version. "
                "Use 'max_batch_size' instead.",
                stacklevel=2,
            )
            max_batch_size = kwargs.pop("batch_size")
        if kwargs:
            raise TypeError(f"Unexpected arguments: {', '.join(sorted(kwargs))}")
        for name, value in (("max_batch_size", max_batch_size), ("concurrency", concurrency)):
            if isinstance(value, bool) or not isinstance(value, int) or (value != -1 and value < 1):
                raise ValueError(f"{name} must be a positive integer or -1 for no limit")
        if isinstance(max_queue_size, bool) or not isinstance(max_queue_size, int) or max_queue_size < -1:
            raise ValueError("max_queue_size must be a nonnegative integer or -1 for no limit")
        if (
            not isinstance(max_queue_time, Real)
            or isinstance(max_queue_time, bool)
            or not math.isfinite(max_queue_time)
            or max_queue_time < 0
        ):
            raise ValueError("max_queue_time must be a finite, nonnegative number")
        self.max_batch_size = max_batch_size
        self.max_queue_time = max_queue_time
        self.concurrency = concurrency
        self.executor = executor
        self._queue = asyncio.Queue(maxsize=max_queue_size)
        self._queue_changed = asyncio.Event()
        self._current_task: asyncio.Task | None = None
        self._running_batches: dict[int, asyncio.Task] = {}
        self._processing_tasks: set[asyncio.Task] = set()
        self._pending_futures: set[asyncio.Future] = set()
        self._concurrency_semaphore = asyncio.Semaphore(concurrency) if concurrency > 0 else None
        self._stop = asyncio.Event()
        self._is_running = asyncio.Event()
        self._force_stopped = False

    @abc.abstractmethod
    async def process_batch(self, batch: list[T]) -> list[S] | None:
        """Return one result per item, or None for a batch without return values.

        Subclasses may implement this as a synchronous method. Exceptions raised
        by the processor fail the entire batch; exception objects in the result
        list fail only their corresponding requests.
        """

    async def process(self, item: T) -> S:
        """Enqueue an item and await its result; reject submissions after stopping."""
        if self._stop.is_set():
            raise RuntimeError("Batcher is stopped")
        if self._queue.full():
            raise QueueFullException("The queue is full, cannot process more items at the moment.")
        logging.debug(item)
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        self._pending_futures.add(future)
        future.add_done_callback(self._pending_futures.discard)
        self._queue.put_nowait(self.QueueItem(item, future, loop.time()))
        self._queue_changed.set()
        if self._current_task is None:
            self._current_task = loop.create_task(self.run())
        return await future

    async def _fill_batch_from_queue(self) -> list[QueueItem]:
        batch = []
        loop = asyncio.get_running_loop()
        deadline = None
        while self.max_batch_size < 0 or len(batch) < self.max_batch_size:
            try:
                item = self._queue.get_nowait()
            except asyncio.QueueEmpty:
                if self._stop.is_set():
                    break
                self._queue_changed.clear()
                if deadline is None:
                    await self._queue_changed.wait()
                else:
                    remaining = deadline - loop.time()
                    if remaining <= 0:
                        break
                    try:
                        await asyncio.wait_for(self._queue_changed.wait(), timeout=remaining)
                    except asyncio.TimeoutError:
                        break
            else:
                batch.append(item)
                if deadline is None:
                    deadline = item.enqueued_at + self.max_queue_time
        return batch

    async def _batch_run(self, batch: list[QueueItem]):
        if self._force_stopped:
            return
        task = asyncio.current_task()
        self._processing_tasks.add(task)
        loop = asyncio.get_running_loop()
        started_at = loop.time()
        try:
            batch_items = [q_item.item for q_item in batch]
            if asyncio.iscoroutinefunction(self.process_batch):
                results = await self.process_batch(batch=batch_items)
            else:
                results = await loop.run_in_executor(self.executor, self.process_batch, batch_items)
            if results is None:
                results = [None] * len(batch)
            if len(results) != len(batch):
                raise ValueError(f"Expected to get {len(batch)} results, but got {len(results)}.")
        except Exception as error:
            self.logger.error("Error processing batch", exc_info=True)
            for q_item in batch:
                if not q_item.future.done():
                    q_item.future.set_exception(error)
        else:
            for q_item, result in zip(batch, results, strict=True):
                if q_item.future.done():
                    continue
                if isinstance(result, Exception):
                    q_item.future.set_exception(result)
                else:
                    q_item.future.set_result(result)
        finally:
            self._processing_tasks.discard(task)
            self.logger.debug(
                "Processed batch of %s elements in %s seconds.", len(batch), loop.time() - started_at
            )

    def _batch_done(self, task_id: int, batch: list[QueueItem], task: asyncio.Task):
        # A done callback also runs when a task is cancelled before its first step.
        self._running_batches.pop(task_id, None)
        if self._concurrency_semaphore is not None:
            self._concurrency_semaphore.release()
        error = None if task.cancelled() else task.exception()
        for q_item in batch:
            if not q_item.future.done():
                if error is not None:
                    q_item.future.set_exception(error)
                else:
                    q_item.future.cancel()

    def _cancel_pending(self):
        for future in self._pending_futures:
            future.cancel()
        while not self._queue.empty():
            self._queue.get_nowait()

    async def run(self):
        """Run until stopped and all accepted work has finished.

        Processing starts this task automatically. Only one dispatcher may run.
        """
        current_task = asyncio.current_task()
        if self._current_task is not None and self._current_task is not current_task:
            raise RuntimeError("Batcher already has a dispatcher")
        self._current_task = current_task
        self._is_running.set()
        task_id = 0
        try:
            while not self._should_stop():
                semaphore_acquired = False
                try:
                    if self._concurrency_semaphore is not None:
                        await self._concurrency_semaphore.acquire()
                        semaphore_acquired = True
                    batch = await self._fill_batch_from_queue()
                    if self._force_stopped:
                        break
                    if batch:
                        task = asyncio.create_task(self._batch_run(batch))
                        self._running_batches[task_id] = task
                        task.add_done_callback(partial(self._batch_done, task_id, batch))
                        # The completion callback now owns this semaphore permit.
                        semaphore_acquired = False
                        task_id += 1
                finally:
                    if semaphore_acquired:
                        self._concurrency_semaphore.release()
            if self._running_batches:
                await asyncio.shield(asyncio.gather(*self._running_batches.values(), return_exceptions=True))
        except BaseException:
            self._stop.set()
            self._cancel_pending()
            for task in self._running_batches.values():
                task.cancel()
            if self._running_batches:
                await asyncio.gather(*self._running_batches.values(), return_exceptions=True)
            raise
        finally:
            self._is_running.clear()

    def _should_stop(self):
        return self._stop.is_set() and self._queue.empty()

    async def is_running(self):
        """Return whether the dispatcher is running or draining active batches."""
        return self._is_running.is_set()

    async def stop(self, force: bool = False, timeout: float | None = None):
        """Reject new work and wait for shutdown.

        Graceful shutdown flushes the queue and waits for active batches. A timeout
        raises asyncio.TimeoutError while draining continues. Forced shutdown cancels all
        pending results and awaits task cleanup. Running executor functions cannot
        be interrupted and may finish after forced shutdown returns.
        """
        if asyncio.current_task() in self._processing_tasks:
            raise RuntimeError(
                "stop() cannot be awaited from process_batch; call it from outside the processor"
            )
        self._stop.set()
        self._queue_changed.set()
        if force and not self._force_stopped:
            self._force_stopped = True
            self._cancel_pending()
            if self._current_task is not None:
                self._current_task.cancel()
        tasks = list(self._running_batches.values())
        if self._current_task is not None:
            tasks.append(self._current_task)
        if tasks:
            # shield prevents a timeout or a cancelled stop caller from destroying
            # accepted work. return_exceptions permits expected forced cancellation.
            results = await asyncio.wait_for(
                asyncio.shield(asyncio.gather(*tasks, return_exceptions=True)), timeout=timeout
            )
            for result in results:
                if isinstance(result, Exception):
                    raise result
