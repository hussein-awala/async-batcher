from __future__ import annotations

from typing import TYPE_CHECKING

from async_batcher.batcher import AsyncBatcher

if TYPE_CHECKING:
    from keras import Model


class SklearnAsyncBatcher(AsyncBatcher):
    def __init__(
        self,
        *,
        model: Model,
        batch_size: int = -1,
        sleep_time: float = 0.01,
        buffering_time: float = 0.001,
    ):
        super().__init__(batch_size=batch_size, sleep_time=sleep_time, buffering_time=buffering_time)
        self.model = model

    def process_batch(self, batch):
        return self.model.predict(batch, batch_size=len(batch))
