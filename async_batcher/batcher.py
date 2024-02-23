from __future__ import annotations

import abc
import asyncio
import logging
from collections import namedtuple
from typing import Generic, TypeVar

T = TypeVar("T")
S = TypeVar("S")


class AsyncBatcher(Generic[T, S], abc.ABC):
    """A generic class for batching and processing items asynchronously.

    Args:
        batch_size (int, optional): The max number of items to process in a batch. Defaults to -1 (no limit).
        sleep_time (float, optional): The time to sleep between checking if the result is ready in seconds.
            Defaults to 0.01. Set it to a value close to the expected time to process a batch
        buffering_time (float, optional): The time to sleep after processing a batch or checking the buffer
            in seconds. Defaults to 0.001.
            You can increase this value if you don't need a low latency, but want to reduce the number of
            processed batches.
    """

    logger = logging.getLogger(__name__)
    QueueItem = namedtuple("QueueItem", ["item", "future"])

    def __init__(
        self,
        *,
        max_batch_size: int = -1,
        max_queue_time: float = 0.01,
    ):
        super().__init__()
        if max_batch_size is None or 0 <= max_batch_size <= 1:
            raise ValueError("Valid max_batch_size value is greater than 1 or -1 for infinite")
        self.max_batch_size = max_batch_size
        self.max_queue_time = max_queue_time
        self._queue = asyncio.Queue()
        self._current_task: asyncio.Task | None = None
        self._should_stop = False
        self._force_stop = False
        self._is_running = False

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
        if self._should_stop:
            raise RuntimeError("Batcher is stopped")
        if self._current_task is None:
            self._current_task = asyncio.get_running_loop().create_task(self.batch_run())
        logging.debug(item)
        future = asyncio.get_running_loop().create_future()
        await self._queue.put(self.QueueItem(item, future))
        await future
        return future.result()

    async def _fill_batch_from_queue(self):
        try:
            batch = [await asyncio.wait_for(self._queue.get(), timeout=1.0)]
        except asyncio.TimeoutError:
            return []

        started_at = asyncio.get_running_loop().time()
        while 1:
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

    async def batch_run(self):
        """Run the batcher asynchronously."""
        self._is_running = True
        while not self._should_stop or (not self._force_stop and self._queue.qsize() > 0):
            batch = await self._fill_batch_from_queue()
            if not batch:
                continue

            started_at = asyncio.get_event_loop().time()
            try:
                batch_items = [q_item.item for q_item in batch]
                if asyncio.iscoroutinefunction(self.process_batch):
                    results = await self.process_batch(batch=batch_items)
                else:
                    results = await asyncio.get_event_loop().run_in_executor(
                        None, self.process_batch, batch_items
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
                    q_item.future.set_result(result)
            elapsed_time = asyncio.get_event_loop().time() - started_at
            self.logger.debug(f"Processed batch of {len(batch)} elements" f" in {elapsed_time} seconds.")
        self._is_running = False

    def stop(self, force: bool = False):
        """Stop the batcher asyncio task.

        Args:
            force (bool, optional): Whether to force stop the batcher without waiting for processing
                the remaining buffer items. Defaults to False.
        """
        if force:
            self._force_stop = True
        self._should_stop = True
        if (
            self._current_task
            and not self._current_task.done()
            and not self._current_task.get_loop().is_closed()
        ):
            self._current_task.get_loop().run_until_complete(self._current_task)
