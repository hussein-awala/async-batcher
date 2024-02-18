from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from async_batcher.batcher import AsyncBatcher

if TYPE_CHECKING:
    from concurrent.futures import Executor

    from sklearn.base import BaseEstimator


class SklearnAsyncBatcher(AsyncBatcher):
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
