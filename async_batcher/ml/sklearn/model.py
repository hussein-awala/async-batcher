from __future__ import annotations

from typing import TYPE_CHECKING

from async_batcher.batcher import AsyncBatcher

if TYPE_CHECKING:
    from sklearn.base import BaseEstimator


class SklearnAsyncBatcher(AsyncBatcher):
    def __init__(
        self,
        *,
        model: BaseEstimator,
        batch_size: int = -1,
        sleep_time: float = 0.01,
        buffering_time: float = 0.001,
    ):
        super().__init__(batch_size=batch_size, sleep_time=sleep_time, buffering_time=buffering_time)
        self.model = model

    async def process_batch(self, batch):
        if hasattr(self.model, "predict"):
            return self.model.predict(batch)
        else:
            raise AttributeError("Model does not have a predict method")
