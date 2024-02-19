from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from async_batcher.batcher import AsyncBatcher

if TYPE_CHECKING:
    from concurrent.futures import Executor

    from sklearn.base import BaseEstimator


class SklearnAsyncBatcher(AsyncBatcher):
    """Batcher for scikit-learn models.

    Args:
        model: The scikit-learn model to use for prediction.
        executor: The executor to use for running the prediction.
        batch_size (int, optional): The max number of items to process in a batch. Defaults to -1 (no limit).
        sleep_time (float, optional): The time to sleep between checking if the result is ready in seconds.
            Defaults to 0.01. Set it to a value close to the expected time to process a batch
        buffering_time (float, optional): The time to sleep after processing a batch or checking the buffer
            in seconds. Defaults to 0.001.
            You can increase this value if you don't need a low latency, but want to reduce the number of
            processed batches.
    """

    def __init__(
        self,
        *,
        model: BaseEstimator,
        executor: Executor | None = None,
        batch_size: int = -1,
        sleep_time: float = 0.01,
        buffering_time: float = 0.001,
    ):
        super().__init__(batch_size=batch_size, sleep_time=sleep_time, buffering_time=buffering_time)
        self.model = model
        self.executor = executor

    async def process_batch(self, batch):
        if hasattr(self.model, "predict"):
            result = await asyncio.get_event_loop().run_in_executor(self.executor, self.model.predict, batch)
            return result
        else:
            raise AttributeError("Model does not have a predict method")
