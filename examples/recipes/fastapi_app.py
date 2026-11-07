"""FastAPI lifespan recipe with a toy predictor and real batched SQLite writes."""

from __future__ import annotations

import asyncio
import os
import tempfile
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Annotated

from async_batcher import AsyncBatcher, QueueFullException
from examples.recipes.operations import (
    BackendDeadlineExceeded,
    Intake,
    ResponseDeadlineExceeded,
)
from examples.recipes.sqlite_stream import SQLiteWriter
from examples.recipes.support import BatchMeasurements, Document
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, Field

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Sequence

Feature = Annotated[float, Field(ge=-1_000_000, le=1_000_000, allow_inf_nan=False)]


class PredictRequest(BaseModel):
    features: list[Feature] = Field(min_length=3, max_length=3)


class DocumentRequest(BaseModel):
    id: str = Field(min_length=1, max_length=128)
    text: str = Field(max_length=4096)


def demo_predict(rows: list[list[float]]) -> list[float]:
    """A deterministic weighted sum for demonstration; not a trained model."""
    return [sum(weight * value for weight, value in zip((0.2, 0.3, 0.5), row, strict=True)) for row in rows]


@dataclass
class Services:
    predictions: AsyncBatcher[list[float], float]
    documents: AsyncBatcher[Document, str]
    prediction_measurements: BatchMeasurements
    document_measurements: BatchMeasurements
    prediction_intake: Intake
    document_intake: Intake
    accepting: bool = True


def create_app(
    database: Path,
    *,
    prediction_processor: Callable[
        [list[list[float]]], Sequence[float] | Awaitable[Sequence[float]]
    ] = demo_predict,
    batch_size: int = 16,
    queue_size: int = 64,
    max_queue_time: float = 0.01,
    response_timeout: float | None = None,
    shutdown_grace: float | None = None,
    shutdown_force: float | None = None,
) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app):
        writer = SQLiteWriter(database)
        await asyncio.to_thread(writer.initialize)
        prediction_measurements = BatchMeasurements()
        document_measurements = BatchMeasurements()
        predictions = AsyncBatcher.from_callable(
            prediction_processor,
            max_batch_size=batch_size,
            max_queue_size=queue_size,
            max_queue_time=max_queue_time,
            concurrency=1,
            on_batch=prediction_measurements.observe,
        )
        documents = AsyncBatcher.from_callable(
            writer.write,
            max_batch_size=batch_size,
            max_queue_size=queue_size,
            max_queue_time=max_queue_time,
            concurrency=1,
            max_batch_cost=65536,
            item_cost=lambda document: len(document.id.encode()) + len(document.text.encode()),
            on_batch=document_measurements.observe,
        )
        prediction_intake, document_intake = Intake(predictions), Intake(documents)
        services = Services(
            predictions,
            documents,
            prediction_measurements,
            document_measurements,
            prediction_intake,
            document_intake,
        )
        app.state.services = services
        try:
            yield
        finally:
            # Close both intakes before either child begins draining.
            services.accepting = False
            prediction_intake.close()
            document_intake.close()
            try:
                outcomes = await asyncio.gather(
                    *(
                        intake.shutdown(grace_timeout=shutdown_grace, force_timeout=shutdown_force)
                        for intake in (prediction_intake, document_intake)
                    ),
                    return_exceptions=True,
                )
                errors = [outcome for outcome in outcomes if isinstance(outcome, BaseException)]
                if errors:
                    raise BaseExceptionGroup("Batcher shutdown failed", errors)
            finally:
                app.state.services = None

    app = FastAPI(title="AsyncBatcher recipe (demo model)", lifespan=lifespan)
    app.state.services = None

    def active_services(request: Request) -> Services:
        services = request.app.state.services
        if services is None or not services.accepting:
            raise HTTPException(status_code=503, detail="Batchers are not accepting requests")
        return services

    async def submit(intake, item):
        try:
            return await intake.process(item, response_timeout=response_timeout)
        except ResponseDeadlineExceeded as error:
            raise HTTPException(
                status_code=504, detail="Response deadline expired; work may still finish"
            ) from error
        except BackendDeadlineExceeded as error:
            raise HTTPException(
                status_code=504, detail="Configured async backend deadline expired"
            ) from error
        except QueueFullException as error:
            raise HTTPException(
                status_code=503,
                detail="Batch queue is full; retry later",
                headers={"Retry-After": "1"},
            ) from error

    @app.post("/predict")
    async def predict(body: PredictRequest, request: Request):
        score = await submit(active_services(request).prediction_intake, body.features)
        return {"score": score}

    @app.post("/documents", status_code=201)
    async def put_document(body: DocumentRequest, request: Request):
        identifier = await submit(active_services(request).document_intake, Document(body.id, body.text))
        return {"id": identifier}

    @app.get("/metrics")
    async def metrics(request: Request):
        services = active_services(request)
        return {
            "prediction": services.prediction_measurements.report(services.predictions),
            "storage": services.document_measurements.report(services.documents),
        }

    return app


def demo_app() -> FastAPI:
    """Uvicorn --factory entry point; set the database path before starting it."""
    database = Path(
        os.environ.get("BATCHER_DEMO_DATABASE", str(Path(tempfile.gettempdir()) / "batcher-web.sqlite"))
    )
    return create_app(database, response_timeout=2, shutdown_grace=5, shutdown_force=1)
