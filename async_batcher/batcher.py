from __future__ import annotations

import abc
import asyncio
import logging
import warnings
from collections import namedtuple
from typing import TYPE_CHECKING, Generic, TypeVar

from async_batcher.exceptions import QueueFullException

if TYPE_CHECKING:
    from concurrent.futures import Executor

T = TypeVar("T")
S = TypeVar("S")


class AsyncBatcher(Generic[T, S], abc.ABC):
    """A generic class for batching and processing items asynchronously.

    Args:
        max_batch_size (int, optional): The max number of items to process in a batch.
            Defaults to -1 (no limit).
        max_queue_time (float, optional): The max time for a task to stay in the queue before processing
            it if the batch is not full and the number of running batches is less than the concurrency.
            Defaults to 0.01.
        max_queue_size (int, optional): The max number of items to keep in the queue.
            Defaults to -1 (no limit).
        concurrency (int, optional): The max number of concurrent batches to process.
            Defaults to 1. If -1, it will process all batches concurrently.
        executor (Executor, optional): The executor to use to process the batch if the `process_batch` method
            is not a Coroutine. If None, it will use the default asyncio executor. Defaults to None.
    """

    logger = logging.getLogger(__name__)
    QueueItem = namedtuple("QueueItem", ["item", "future"])

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
        if max_batch_size is None or 0 <= max_batch_size <= 1:
            raise ValueError("Valid max_batch_size value is greater than 1 or -1 for infinite")
        if concurrency is None or concurrency == 0:
            raise ValueError("Valid concurrency value is greater than 0 or -1 for infinite")
        # check deprecated arguments
        if "sleep_time" in kwargs:
            warnings.warn(
                "The 'sleep_time' has no effect and will be removed in a future version. "
                "In the current version, the process task will sleep until the result is ready.",
                stacklevel=2,
            )
        if "buffering_time" in kwargs:
            warnings.warn(
                "The 'buffering_time' has no effect and will be removed in a future version. "
                "In the current version, the process_bath task will sleep until there is any item in"
                " the queue.",
                stacklevel=2,
            )
        if "batch_size" in kwargs:
            warnings.warn(
                "The 'batch_size' argument is deprecated and will be removed in a future version. "
                "Use 'max_batch_size' instead.",
                stacklevel=2,
            )
            max_batch_size = kwargs.pop("batch_size")
        self.max_batch_size = max_batch_size
        self.max_queue_time = max_queue_time
        self.concurrency = concurrency
        self.executor = executor
        self._queue = asyncio.Queue(maxsize=max_queue_size)
        self._current_task: asyncio.Task | None = None
        self._running_batches: dict[int, asyncio.Task] = {}
        self._concurrency_semaphore = asyncio.Semaphore(concurrency) if concurrency > 0 else None
        self._stop = asyncio.Event()
        self._is_running = asyncio.Event()

    @abc.abstractmethod
    async def process_batch(self, batch: list[T]) -> list[S] | None:
        """Process a batch of items.

        This method should be overridden by the user to define how to process a batch of items.
        """

    async def process(self, item: T) -> S:
        """Add an item to the queue and get the result when it's ready.

        Args:
            item (T): The item to process.

        Returns:
            S: The result of processing the item.
        """
        if self._stop.is_set():
            raise RuntimeError("Batcher is stopped")
        if self._current_task is None:
            self._current_task = asyncio.get_running_loop().create_task(self.run())
        logging.debug(item)
        future = asyncio.get_running_loop().create_future()
        if self._queue.full():
            raise QueueFullException("The queue is full, cannot process more items at the moment.")
        await self._queue.put(self.QueueItem(item, future))
        await future
        return future.result()

    async def _fill_batch_from_queue(self, started_at: float | None) -> list[QueueItem]:
        try:
            batch = [await asyncio.wait_for(self._queue.get(), timeout=1.0)]
        except asyncio.TimeoutError:
            return []
        if started_at is None:
            started_at = asyncio.get_event_loop().time()
        while True:
            try:
                max_wait = self.max_queue_time - (asyncio.get_running_loop().time() - started_at)
                if max_wait > 0:
                    item = await asyncio.wait_for(self._queue.get(), timeout=self.max_queue_time)
                else:
                    item = self._queue.get_nowait()
                batch.append(item)
            except (asyncio.QueueEmpty, asyncio.TimeoutError):
                break
            if 0 < self.max_batch_size <= len(batch):
                break
        return batch

    async def _batch_run(self, task_id: int, batch: list[QueueItem]):
        started_at = asyncio.get_event_loop().time()
        try:
            batch_items = [q_item.item for q_item in batch]
            if asyncio.iscoroutinefunction(self.process_batch):
                results = await self.process_batch(batch=batch_items)
            else:
                results = await asyncio.get_event_loop().run_in_executor(
                    self.executor, self.process_batch, batch_items
                )
            if results is None:
                results = [None] * len(batch)
            if len(results) != len(batch):
                raise ValueError(f"Expected to get {len(batch)} results, but got {len(results)}.")
        except Exception as e:
            self.logger.error("Error processing batch", exc_info=True)
            for q_item in batch:
                q_item.future.set_exception(e)
        else:
            for q_item, result in zip(batch, results, strict=True):
                if isinstance(result, Exception):
                    q_item.future.set_exception(result)
                else:
                    q_item.future.set_result(result)
        elapsed_time = asyncio.get_event_loop().time() - started_at
        self.logger.debug(f"Processed batch of {len(batch)} elements" f" in {elapsed_time} seconds.")
        self._running_batches.pop(task_id)

    async def _concurrent_batch_run(self, task_id: int, batch: list[QueueItem]):
        async with self._concurrency_semaphore:
            await self._batch_run(task_id, batch)

    async def run(self):
        """Run the batcher asynchronously."""
        self._is_running.set()
        task_id = 0
        if self.concurrency > 0:
            started_at = None
            while not self._should_stop():
                if started_at is None:
                    started_at = asyncio.get_event_loop().time()
                semaphore_acquired = False
                try:
                    # to check if the batcher should stop, we raise a timeout after 1 second
                    # if the semaphore is not acquired
                    await asyncio.wait_for(self._concurrency_semaphore.acquire(), timeout=1.0)
                    semaphore_acquired = True
                    # if the queue is empty, we need to let the batch filler create it
                    batch = await self._fill_batch_from_queue(
                        started_at=started_at if self._queue.qsize() > 0 else None
                    )
                    if batch:
                        # create a new task to process the batch
                        self._running_batches[task_id] = asyncio.get_event_loop().create_task(
                            self._concurrent_batch_run(task_id, batch)
                        )
                        await asyncio.sleep(0)
                        task_id += 1
                    started_at = None
                except asyncio.TimeoutError:
                    pass
                finally:
                    if semaphore_acquired:
                        self._concurrency_semaphore.release()
        else:
            while not self._should_stop():
                batch = await self._fill_batch_from_queue(started_at=None)
                if batch:
                    self._running_batches[task_id] = asyncio.get_event_loop().create_task(
                        self._batch_run(task_id, batch)
                    )
                    task_id += 1
        self._is_running.clear()

    def _should_stop(self):
        return self._stop.is_set() and self._queue.qsize() == 0

    async def is_running(self):
        """Check if the batcher is running.

        Returns:
            bool: True if the batcher is running, False otherwise.
        """
        return self._is_running.is_set()

    async def stop(self, force: bool = False, timeout: float | None = None):
        """Stop the batcher asyncio task.

        Args:
            force (bool, optional): Whether to force stop the batcher without waiting for processing
                the remaining buffer items. If True, it will cancel the current task and all running tasks.
                Defaults to False.
            timeout (float, optional): The time to wait for the batcher to stop. If None, it will wait
                indefinitely. Defaults to None.
        """
        if force:
            if self._current_task and not self._current_task.done():
                self._current_task.cancel()
            for task in self._running_batches.values():
                if not task.done():
                    task.cancel()
        else:
            self._stop.set()
            if (
                self._current_task
                and not self._current_task.done()
                and not self._current_task.get_loop().is_closed()
            ):
                await asyncio.wait_for(self._current_task, timeout=timeout)
