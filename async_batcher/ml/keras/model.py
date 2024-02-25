from __future__ import annotations

from typing import TYPE_CHECKING

from async_batcher.batcher import AsyncBatcher

if TYPE_CHECKING:
    from concurrent.futures import Executor

    from keras import Model


class KerasAsyncBatcher(AsyncBatcher):
    """Batcher for Keras models.

    Args:
        model: The Keras model to use for prediction.
        executor: The executor to use for running the prediction.
        max_batch_size (int, optional): The max number of items to process in a batch.
            Defaults to -1 (no limit).
        max_queue_time (float, optional): The max time for a task to stay in the queue before
            processing it if the batch is not full and the number of running batches is less
            than the concurrency. Defaults to 0.01.
        concurrency (int, optional): The max number of concurrent batches to process.
            Defaults to 1. If -1, it will process all batches concurrently.
        executor (Executor, optional): The executor to use to process the batch.
            If None, it will use the default asyncio executor. Defaults to None.
    """

    def __init__(
        self,
        *,
        model: Model,
        max_batch_size: int = -1,
        max_queue_time: float = 0.01,
        concurrency: int = 1,
        executor: Executor | None = None,
        **kwargs,
    ):
        super().__init__(
            max_batch_size=max_batch_size,
            max_queue_time=max_queue_time,
            concurrency=concurrency,
            executor=executor,
            **kwargs,
        )
        self.model = model

    def process_batch(self, batch):
        return self.model.predict(batch, batch_size=len(batch))
