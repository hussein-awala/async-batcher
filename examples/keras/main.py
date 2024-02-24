from __future__ import annotations

import asyncio
import gc
from typing import TYPE_CHECKING

import tensorflow as tf
from async_batcher.batcher import AsyncBatcher
from fastapi import FastAPI

if TYPE_CHECKING:
    import keras.src.engine.sequential


class MlBatcher(AsyncBatcher[list[float], list[float]]):
    def __init__(self, model, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.model: keras.src.engine.sequential.Sequential = model

    def process_batch(self, batch: list[list[float]]) -> list[float]:
        return self.model.predict(batch, verbose=0).tolist()


app = FastAPI()
model = tf.keras.models.load_model("../diabetes_tf_model.h5")


batcher = MlBatcher(model=model, max_queue_time=0.001)


@app.on_event("startup")
async def startup_event():
    gc.freeze()


@app.on_event("shutdown")
def shutdown_event():
    asyncio.run(batcher.stop())


@app.post("/predict")
async def predict(data: list[float]):
    return model.predict([data]).tolist()[0][0]


@app.post("/optimized_predict")
async def optimized_predict(data: list[float]):
    return (await batcher.process(item=data))[0]
