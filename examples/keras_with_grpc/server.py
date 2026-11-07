from __future__ import annotations

import asyncio
import logging
from functools import partial
from pathlib import Path

import grpc
import predictor_pb2
import predictor_pb2_grpc
from async_batcher.batcher import AsyncBatcher

import keras


class MlBatcher(AsyncBatcher[list[float], list[float]]):
    def __init__(self, model, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.model = model

    async def process_batch(self, batch: list[list[float]]) -> list[list[float]]:
        import numpy as np

        predict = partial(self.model.predict, np.asarray(batch, dtype="float32"), verbose=0)
        batch_result = await asyncio.get_running_loop().run_in_executor(self.executor, predict)
        return batch_result.tolist()


class PredictorGRPC(predictor_pb2_grpc.KerasPredictorServicer):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        model = keras.models.load_model(
            Path(__file__).resolve().parents[1] / "diabetes_tf_model.h5", compile=False
        )
        self.batcher = MlBatcher(model=model, max_batch_size=200, max_queue_time=0.01)

    async def Predict(
        self,
        request: predictor_pb2.Vector,
        context: grpc.aio.ServicerContext,
    ) -> predictor_pb2.PredictionResult:
        result = await self.batcher.process(item=list(request.data))
        return predictor_pb2.PredictionResult(result=result[0])


async def serve() -> None:
    server = grpc.aio.server()
    predictor = PredictorGRPC()
    predictor_pb2_grpc.add_KerasPredictorServicer_to_server(predictor, server)
    listen_addr = "[::]:50051"
    server.add_insecure_port(listen_addr)
    logging.info(f"Starting server on {listen_addr}")
    await server.start()

    try:
        await server.wait_for_termination()
    finally:
        logging.info("Starting graceful shutdown...")
        try:
            # Stop accepting RPCs and let existing handlers finish before closing
            # the batcher those handlers still need.
            await server.stop(30)
        finally:
            await predictor.batcher.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(serve())
