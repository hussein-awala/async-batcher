from __future__ import annotations

import abc
import asyncio
import logging
import uuid
from collections import deque
from threading import Thread
from typing import Generic, TypeVar

T = TypeVar("T")
S = TypeVar("S")


class AsyncBatcher(Generic[T, S], abc.ABC, Thread):
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

    _buffer = deque[tuple[uuid.UUID, T]]
    _results = dict[uuid.UUID, S]
    logger = logging.getLogger(__name__)

    def __init__(
        self,
        *,
        batch_size: int = -1,
        sleep_time: float = 0.01,
        buffering_time: float = 0.001,
    ):
        super().__init__()
        self.batch_size = batch_size
        self.sleep_time = sleep_time
        self.buffering_time = buffering_time
        self._buffer = deque()
        self._results = {}
        self._current_batch = 0
        self._should_stop = False
        self._force_stop = False
        self._is_running = False

    @abc.abstractmethod
    async def process_batch(self, *, batch: list[T]) -> list[S] | None:
        """Process a batch of items.

        This method should be overridden by the user to define how to process a batch of items.
        """

    async def _process_single(self, *, item: T) -> S:
        """Process a single item.

        This method adds the item to the buffer and waits for the result to be ready.
        It's used by the `process` method to add an item to the buffer and get the result when it's ready.

        Args:
            item (T): The item to process.

        Returns:
            S: The result of processing the item.
        """
        started_at = asyncio.get_event_loop().time()
        query_id = uuid.uuid4()
        last_checked_batch = self._current_batch
        self.logger.debug(f"Adding item {query_id} to the buffer.")
        self._buffer.append((query_id, item))
        while True:
            if self._current_batch > last_checked_batch:
                last_checked_batch = self._current_batch
                if query_id in self._results:
                    elapsed_time = asyncio.get_event_loop().time() - started_at
                    self.logger.debug(f"Item {query_id} is ready after {elapsed_time} seconds.")
                    return self._results.pop(query_id)
            await asyncio.sleep(self.sleep_time)

    async def process(self, *, item: T) -> S:
        """Add an item to the buffer and get the result when it's ready.

        Args:
            item (T): The item to process.

        Returns:
            S: The result of processing the item.
        """
        if not self._is_running:
            raise RuntimeError("The batcher is not running.")
        result = await asyncio.get_event_loop().create_task(self._process_single(item=item))
        if isinstance(result, Exception):
            raise result
        return result

    async def arun(self):
        self._is_running = True
        while not self._should_stop or (not self._force_stop and len(self._buffer) > 0):
            ids = []
            batch = []
            while self._buffer and (len(batch) < self.batch_size or self.batch_size == -1):
                query_id, item = self._buffer.popleft()
                batch.append(item)
                ids.append(query_id)
            if batch:
                started_at = asyncio.get_event_loop().time()
                try:
                    results = await asyncio.get_event_loop().create_task(self.process_batch(batch=batch))
                    if results is None:
                        results = [None] * len(batch)
                    if len(results) != len(batch):
                        raise ValueError(f"Expected to get {len(batch)} results, but got {len(results)}.")
                except Exception as e:
                    self.logger.error("Error processing batch", exc_info=True)
                    results = [e] * len(batch)
                for query_id, result in zip(ids, results):
                    self._results[query_id] = result
                elapsed_time = asyncio.get_event_loop().time() - started_at
                self.logger.debug(
                    f"Processed batch {self._current_batch} of {len(batch)} elements"
                    f" in {elapsed_time} seconds."
                )
                self._current_batch += 1
                await asyncio.sleep(self.buffering_time)
            else:
                self.logger.debug("No items to process. Sleeping.")
                await asyncio.sleep(self.buffering_time)
        self._is_running = False

    def run(self):
        """Run the batcher thread."""
        asyncio.run(self.arun())

    def stop(self, force: bool = False):
        """Stop the batcher thread.

        Args:
            force (bool, optional): Whether to force stop the batcher without waiting for processing
                the remaining buffer items. Defaults to False.
        """
        if force:
            self._force_stop = True
        self._should_stop = True
