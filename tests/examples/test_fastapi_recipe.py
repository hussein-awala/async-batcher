from __future__ import annotations

import asyncio
import sqlite3
from contextlib import closing

import httpx2
import pytest
from examples.recipes.fastapi_app import create_app
from fastapi.testclient import TestClient
from tests.core_helpers import wait_until


def rows(database):
    with closing(sqlite3.connect(database)) as connection:
        return dict(connection.execute("SELECT id, text FROM documents"))


def test_real_lifespan_http_validation_and_sqlite_persistence(tmp_path):
    database = tmp_path / "web.sqlite"
    app = create_app(database)
    assert app.state.services is None
    with TestClient(app) as client:
        services = app.state.services
        assert client.post("/predict", json={"features": [1, 2, 3]}).json() == {"score": 2.3}
        assert client.post("/predict", json={"features": [1, 2]}).status_code == 422
        assert client.post("/predict", json={"features": [1, 2, "NaN"]}).status_code == 422
        assert client.post("/documents", json={"id": "", "text": "invalid"}).status_code == 422
        first = client.post("/documents", json={"id": "a", "text": "original"})
        assert first.status_code == 201 and first.json() == {"id": "a"}
        assert client.post("/documents", json={"id": "a", "text": "updated"}).status_code == 201
        measurements = client.get("/metrics").json()
        assert measurements["prediction"]["stats"]["accepted"] == 1
        assert measurements["storage"]["stats"]["succeeded"] == 2
    assert app.state.services is None
    assert services.accepting is False
    assert rows(database) == {"a": "updated"}


@pytest.mark.asyncio
async def test_simultaneous_http_requests_form_actual_batches_and_route_results(tmp_path):
    app = create_app(tmp_path / "web.sqlite", batch_size=2, max_queue_time=60)
    async with app.router.lifespan_context(app):
        async with httpx2.AsyncClient(
            transport=httpx2.ASGITransport(app), base_url="http://recipe"
        ) as client:
            async with asyncio.timeout(2):
                predicted = await asyncio.gather(
                    client.post("/predict", json={"features": [1, 2, 3]}),
                    client.post("/predict", json={"features": [3, 2, 1]}),
                )
                stored = await asyncio.gather(
                    client.post("/documents", json={"id": "a", "text": "first"}),
                    client.post("/documents", json={"id": "b", "text": "second"}),
                )
            assert [response.json()["score"] for response in predicted] == pytest.approx([2.3, 1.7])
            assert [response.status_code for response in stored] == [201, 201]
            measurements = (await client.get("/metrics")).json()
            assert measurements["prediction"]["batch_size_histogram"] == {"2": 1}
            assert measurements["storage"]["batch_size_histogram"] == {"2": 1}
    assert rows(tmp_path / "web.sqlite") == {"a": "first", "b": "second"}


@pytest.mark.asyncio
async def test_full_queue_returns_503_and_does_not_enqueue(tmp_path):
    started = asyncio.Event()
    release = asyncio.Event()

    async def slow_prediction(inputs):
        started.set()
        await release.wait()
        return [sum(values) for values in inputs]

    app = create_app(
        tmp_path / "web.sqlite",
        prediction_processor=slow_prediction,
        batch_size=1,
        queue_size=1,
    )
    async with app.router.lifespan_context(app):
        async with httpx2.AsyncClient(
            transport=httpx2.ASGITransport(app), base_url="http://recipe"
        ) as client:
            requests = []
            try:
                requests.append(asyncio.create_task(client.post("/predict", json={"features": [1, 1, 1]})))
                await asyncio.wait_for(started.wait(), 2)
                requests.append(asyncio.create_task(client.post("/predict", json={"features": [2, 2, 2]})))
                await wait_until(lambda: app.state.services.predictions.stats().queued_items == 1)
                rejected = await client.post("/predict", json={"features": [3, 3, 3]})
                assert rejected.status_code == 503
                assert rejected.headers["retry-after"] == "1"
                assert app.state.services.predictions.stats().accepted == 2
                release.set()
                assert [response.json() for response in await asyncio.gather(*requests)] == [
                    {"score": 3},
                    {"score": 6},
                ]
                assert (await client.post("/predict", json={"features": [4, 4, 4]})).json() == {"score": 12}
            finally:
                release.set()
                for request in requests:
                    request.cancel()
                await asyncio.gather(*requests, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("error_type", [ValueError, TimeoutError])
async def test_processor_failure_returns_500_and_later_requests_still_work(tmp_path, error_type):
    async def prediction(inputs):
        if inputs[0][0] < 0:
            raise error_type("demo processor failed")
        return [sum(values) for values in inputs]

    app = create_app(tmp_path / "web.sqlite", prediction_processor=prediction, batch_size=1)
    async with app.router.lifespan_context(app):
        async with httpx2.AsyncClient(
            transport=httpx2.ASGITransport(app, raise_app_exceptions=False), base_url="http://recipe"
        ) as client:
            assert (await client.post("/predict", json={"features": [-1, 2, 3]})).status_code == 500
            assert (await client.post("/predict", json={"features": [1, 2, 3]})).json() == {"score": 6}
            snapshot = (await client.get("/metrics")).json()["prediction"]["stats"]
            assert snapshot["failed"] == 1 and snapshot["succeeded"] == 1


@pytest.mark.asyncio
async def test_cancelled_http_caller_does_not_let_lifespan_abandon_accepted_work(tmp_path):
    started = asyncio.Event()
    release = asyncio.Event()
    finished = []

    async def prediction(inputs):
        started.set()
        await release.wait()
        finished.extend(inputs)
        return [sum(values) for values in inputs]

    app = create_app(tmp_path / "web.sqlite", prediction_processor=prediction, batch_size=1)
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
            request.cancel()
            with pytest.raises(asyncio.CancelledError):
                await request
            closing = asyncio.create_task(lifetime.__aexit__(None, None, None))
            await wait_until(lambda: not services.accepting)
            assert not closing.done()
            assert (await client.post("/predict", json={"features": [1, 2, 3]})).status_code == 503
            release.set()
            await asyncio.wait_for(closing, 2)
        assert finished == [[1, 2, 3]]
        assert app.state.services is None
        assert not await services.predictions.is_running()
        assert not await services.documents.is_running()
    finally:
        release.set()
        if request is not None:
            request.cancel()
            await asyncio.gather(request, return_exceptions=True)
        if closing is None:
            closing = asyncio.create_task(lifetime.__aexit__(None, None, None))
        await asyncio.wait_for(closing, 2)


@pytest.mark.asyncio
async def test_startup_failure_does_not_leave_an_active_application(tmp_path):
    app = create_app(tmp_path / "missing-directory" / "web.sqlite")
    before = asyncio.all_tasks()
    with pytest.raises(sqlite3.OperationalError):
        async with app.router.lifespan_context(app):
            pytest.fail("startup should fail")
    assert app.state.services is None
    assert asyncio.all_tasks() == before
