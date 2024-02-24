from __future__ import annotations

import asyncio
import logging

import grpc
import predictor_pb2
import predictor_pb2_grpc
import tensorflow as tf
from async_batcher.batcher import AsyncBatcher


class MlBatcher(AsyncBatcher[list[float], list[float]]):
    def __init__(self, model, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.model = model

    async def process_batch(self, batch: list[list[float]]) -> list[float]:
        batch_result = await asyncio.get_event_loop().run_in_executor(None, self.model.predict, batch)
        return batch_result.tolist()


class PredictorGRPC(predictor_pb2_grpc.KerasPredictorServicer):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        model = tf.keras.models.load_model("../diabetes_tf_model.h5")
        self.batcher = MlBatcher(model=model, max_batch_size=200, max_queue_time=0.01)

    async def Predict(
        self,
        request: predictor_pb2.Vector,
        context: grpc.aio.ServicerContext,
    ) -> predictor_pb2.PredictionResult:
        result = await self.batcher.process(item=list(request.data))
        return predictor_pb2.PredictionResult(result=result[0])


_cleanup_coroutines = []


async def serve() -> None:
    server = grpc.aio.server()
    predictor = PredictorGRPC()
    predictor_pb2_grpc.add_KerasPredictorServicer_to_server(predictor, server)
    listen_addr = "[::]:50051"
    server.add_insecure_port(listen_addr)
    logging.info(f"Starting server on {listen_addr}")
    await server.start()

    async def server_graceful_shutdown():
        logging.info("Starting graceful shutdown...")
        await predictor.batcher.stop()
        await server.stop(30)

    _cleanup_coroutines.append(server_graceful_shutdown())
    await server.wait_for_termination()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(serve())
    finally:
        if _cleanup_coroutines:
            loop.run_until_complete(*_cleanup_coroutines)
        loop.close()
