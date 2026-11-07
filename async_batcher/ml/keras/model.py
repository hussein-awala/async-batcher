from __future__ import annotations

from typing import TYPE_CHECKING

from async_batcher.batcher import AsyncBatcher

if TYPE_CHECKING:
    from concurrent.futures import Executor

    from keras import Model


class KerasAsyncBatcher(AsyncBatcher):
    """Batcher for dense Keras models on Python 3.11 and newer.

    The keras extra requires Keras 3.15.1 or newer and a separately installed
    backend. Core batching remains available on Python 3.10.

    Samples are combined along a new batch axis. List, tuple, and dictionary outputs
    retain their structure for each request. Override process_batch for models
    with multiple inputs or custom/ragged input structures.

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
        max_queue_size (int, optional): The maximum number of queued inputs. Defaults to -1 (no limit).
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
        max_queue_size: int = -1,
        executor: Executor | None = None,
        **kwargs,
    ):
        super().__init__(
            max_batch_size=max_batch_size,
            max_queue_time=max_queue_time,
            concurrency=concurrency,
            max_queue_size=max_queue_size,
            executor=executor,
            **kwargs,
        )
        self.model = model

    def process_batch(self, batch):
        import numpy as np

        predictions = self.model.predict(np.asarray(batch), batch_size=len(batch))
        return self._split_predictions(predictions, len(batch))

    @classmethod
    def _split_predictions(cls, predictions, batch_size):
        if isinstance(predictions, dict):
            outputs = {name: cls._split_predictions(value, batch_size) for name, value in predictions.items()}
            return [{name: value[index] for name, value in outputs.items()} for index in range(batch_size)]
        if isinstance(predictions, list | tuple):
            outputs = [cls._split_predictions(value, batch_size) for value in predictions]
            container = tuple if isinstance(predictions, tuple) else list
            return [container(value[index] for value in outputs) for index in range(batch_size)]
        try:
            result_count = len(predictions)
        except TypeError as error:
            raise ValueError("Each Keras output must have a leading batch dimension") from error
        if result_count != batch_size:
            raise ValueError(f"Expected {batch_size} predictions in each Keras output, got {result_count}")
        return predictions
