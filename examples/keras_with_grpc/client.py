from __future__ import annotations

import asyncio
import random

import grpc
import predictor_pb2
import predictor_pb2_grpc


async def random_predict() -> None:
    async with grpc.aio.insecure_channel("localhost:50051") as channel:
        stub = predictor_pb2_grpc.KerasPredictorStub(channel)
        response: predictor_pb2.PredictionResult = await stub.Predict(
            predictor_pb2.Vector(data=[random.random() for _ in range(10)])
        )
    print(f"Prediction result: {response.result}")


async def arun() -> None:
    await asyncio.gather(*[random_predict() for _ in range(1000)])


if __name__ == "__main__":
    asyncio.run(arun())
