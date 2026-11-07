from __future__ import annotations

import asyncio

import httpx2
import pytest
from examples.recipes.fastapi_app import create_app
from examples.recipes.operations import backend_call
from tests.core_helpers import wait_until


@pytest.mark.asyncio
async def test_http_response_timeout_returns_504_without_claiming_backend_rollback(tmp_path):
    started, release = asyncio.Event(), asyncio.Event()
    completed = []

    async def process(rows):
        started.set()
        await release.wait()
        completed.extend(rows)
        return [sum(row) for row in rows]

    app = create_app(
        tmp_path / "deadline.sqlite", prediction_processor=process, batch_size=1, response_timeout=0.01
    )
    async with app.router.lifespan_context(app):
        async with httpx2.AsyncClient(
            transport=httpx2.ASGITransport(app), base_url="http://recipe"
        ) as client:
            request = asyncio.create_task(client.post("/predict", json={"features": [1, 2, 3]}))
            try:
                await asyncio.wait_for(started.wait(), 2)
                response = await asyncio.wait_for(request, 2)
                assert response.status_code == 504
                assert "may still finish" in response.json()["detail"]
                assert app.state.services.predictions.stats().in_flight_items == 1
            finally:
                release.set()
                await asyncio.gather(request, return_exceptions=True)
    assert completed == [[1, 2, 3]]


@pytest.mark.asyncio
async def test_configured_backend_deadline_has_a_distinct_http_response(tmp_path):
    cleaned = asyncio.Event()

    async def backend(rows):
        try:
            await asyncio.Event().wait()
        finally:
            cleaned.set()

    async def process(rows):
        return await backend_call(backend, rows, timeout=0.01)

    app = create_app(
        tmp_path / "deadline.sqlite", prediction_processor=process, batch_size=1, response_timeout=2
    )
    async with app.router.lifespan_context(app):
        async with httpx2.AsyncClient(
            transport=httpx2.ASGITransport(app), base_url="http://recipe"
        ) as client:
            response = await client.post("/predict", json={"features": [1, 2, 3]})
            assert response.status_code == 504
            assert response.json()["detail"] == "Configured async backend deadline expired"
            assert cleaned.is_set()


@pytest.mark.asyncio
async def test_deployment_drain_closes_http_intake_before_forcing_work(tmp_path):
    started, cleaned = asyncio.Event(), asyncio.Event()

    async def process(rows):
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            cleaned.set()

    app = create_app(
        tmp_path / "shutdown.sqlite",
        prediction_processor=process,
        batch_size=1,
        shutdown_grace=0.01,
        shutdown_force=1,
    )
    lifetime = app.router.lifespan_context(app)
    await lifetime.__aenter__()
    services = app.state.services
    request = closing = None
    try:
        async with httpx2.AsyncClient(
            transport=httpx2.ASGITransport(app), base_url="http://recipe"
        ) as client:
            request = asyncio.create_task(client.post("/predict", json={"features": [1, 2, 3]}))
            await asyncio.wait_for(started.wait(), 2)
            closing = asyncio.create_task(lifetime.__aexit__(None, None, None))
            await wait_until(lambda: not services.accepting)
            assert (await client.post("/predict", json={"features": [1, 2, 3]})).status_code == 503
            await asyncio.wait_for(closing, 2)
            await asyncio.gather(request, return_exceptions=True)
        assert request.cancelled()
        assert cleaned.is_set()
        assert app.state.services is None
        assert not await services.predictions.is_running()
        assert not await services.documents.is_running()
    finally:
        if request is not None:
            request.cancel()
            await asyncio.gather(request, return_exceptions=True)
        if closing is None:
            closing = asyncio.create_task(lifetime.__aexit__(None, None, None))
        await asyncio.wait_for(closing, 2)
