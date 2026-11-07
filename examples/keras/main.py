from __future__ import annotations

import asyncio
import gc
from pathlib import Path

from async_batcher.batcher import AsyncBatcher
from fastapi import FastAPI

import keras


class MlBatcher(AsyncBatcher[list[float], list[float]]):
    def __init__(self, model, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.model: keras.Model = model

    def process_batch(self, batch: list[list[float]]) -> list[list[float]]:
        import numpy as np

        return self.model.predict(np.asarray(batch, dtype="float32"), verbose=0).tolist()


app = FastAPI()
model = keras.models.load_model(Path(__file__).resolve().parents[1] / "diabetes_tf_model.h5", compile=False)


batcher = MlBatcher(model=model, max_queue_time=0.001)


@app.on_event("startup")
async def startup_event():
    gc.freeze()


@app.on_event("shutdown")
async def shutdown_event():
    await batcher.stop()


@app.post("/predict")
async def predict(data: list[float]):
    import numpy as np

    result = await asyncio.to_thread(model.predict, np.asarray([data], dtype="float32"), verbose=0)
    return result.tolist()[0][0]


@app.post("/optimized_predict")
async def optimized_predict(data: list[float]):
    return (await batcher.process(item=data))[0]
