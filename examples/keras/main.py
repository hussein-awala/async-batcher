from __future__ import annotations

import asyncio
import gc

import keras.src.engine.sequential
import tensorflow as tf
from async_batcher.batcher import AsyncBatcher
from fastapi import FastAPI


class MlBatcher(AsyncBatcher[list[float], list[float]]):
    def __init__(self, model, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.model: keras.src.engine.sequential.Sequential = model
        print(type(self.model))

    def predict(self, batch):
        return self.model.predict(batch, verbose=0)

    async def process_batch(self, batch: list[list[float]]) -> list[float]:
        batch_result = await asyncio.get_event_loop().run_in_executor(None, self.predict, batch)
        return batch_result.tolist()


app = FastAPI()
model = tf.keras.models.load_model("../diabetes_tf_model.h5")


batcher = MlBatcher(model=model, max_queue_time=0.001)

@app.on_event("startup")
async def startup_event():
    gc.freeze()

@app.on_event("shutdown")
def shutdown_event():
    batcher.stop()


@app.post("/predict")
async def predict(data: list[float]):
    return model.predict([data]).tolist()[0][0]


@app.post("/optimized_predict")
async def optimized_predict(data: list[float]):
    return (await batcher.process(item=data))[0]
