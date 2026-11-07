from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager

import pytest
import pytest_asyncio

httpx2 = pytest.importorskip("httpx2")
sdk = pytest.importorskip("pinecone")

from examples.recipes.cohere_pinecone import (  # noqa: E402
    VECTOR_REQUEST_BUDGET,
    CohereEmbedder,
    index_documents,
    vector_cost,
)
from examples.recipes.support import Document  # noqa: E402
from tests.core_helpers import wait_until  # noqa: E402


@pytest_asyncio.fixture
async def providers():
    state = {
        "requests": [],
        "failures": [],
        "dimension": 3,
        "cohere_status": "200 OK",
        "stored": {},
        "write_started": asyncio.Event(),
        "write_release": None,
    }
    handlers = set()

    async def respond(reader, writer):
        task = asyncio.current_task()
        handlers.add(task)
        try:
            async with asyncio.timeout(5):
                header = await reader.readuntil(b"\r\n\r\n")
                lines = header.split(b"\r\n")
                method, path, _ = lines[0].decode().split(" ")
                headers = dict(line.split(b":", 1) for line in lines[1:] if b":" in line)
                headers = {key.lower(): value.strip() for key, value in headers.items()}
                body = await reader.readexactly(int(headers.get(b"content-length", b"0")))
                payload = json.loads(body) if body else None
                state["requests"].append((method, path, payload, headers, body))
                status = "200 OK"
                if path == "/describe_index_stats":
                    response = {"dimension": state["dimension"], "namespaces": {}, "totalVectorCount": 0}
                elif path == "/v2/embed":
                    status = state["cohere_status"]
                    response = state.get(
                        "reply",
                        {
                            "embeddings": {
                                "float": [
                                    state.get("row", [float(len(text)), -1e-300, 1e100])
                                    for text in payload["texts"]
                                ]
                            }
                        },
                    )
                elif path == "/vectors/upsert":
                    state["write_started"].set()
                    if state["write_release"] is not None:
                        await state["write_release"].wait()
                    if any(vector["id"] == "reject" for vector in payload["vectors"]):
                        status, response = "400 Bad Request", {"code": 3, "message": "local rejection"}
                    else:
                        state["stored"].update(
                            {vector["id"]: vector["values"] for vector in payload["vectors"]}
                        )
                        response = {"upsertedCount": len(payload["vectors"])}
                else:
                    raise AssertionError(f"Unexpected provider request: {path}")
                encoded = json.dumps(response).encode()
                writer.write(
                    f"HTTP/1.1 {status}\r\nContent-Type: application/json\r\n"
                    f"Content-Length: {len(encoded)}\r\nConnection: close\r\n\r\n".encode()
                    + encoded
                )
                await writer.drain()
        except Exception as error:
            state["failures"].append(error)
        finally:
            writer.close()
            await writer.wait_closed()
            handlers.discard(task)

    server = await asyncio.start_server(respond, "127.0.0.1", 0)
    state["host"] = f"http://127.0.0.1:{server.sockets[0].getsockname()[1]}"
    async with server:
        try:
            yield state
        finally:
            for task in list(handlers):
                task.cancel()
            await asyncio.gather(*handlers, return_exceptions=True)
    assert not state["failures"]


@asynccontextmanager
async def clients(state):
    async with (
        httpx2.AsyncClient(
            base_url=state["host"],
            headers={"Authorization": "Bearer local-cohere-key"},
            timeout=2,
            trust_env=False,
        ) as client,
        sdk.AsyncIndex(host=state["host"], api_key="local-pinecone-key", timeout=2) as index,
    ):
        yield client, index


async def pipeline(state, source, **kwargs):
    async with clients(state) as (client, index):
        return await asyncio.wait_for(
            index_documents(
                source,
                cohere_client=client,
                index=index,
                model="explicit-test-v4",
                dimension=3,
                namespace="tenant/model-v4",
                **kwargs,
            ),
            4,
        )


@pytest.mark.asyncio
async def test_real_http_contract_routes_ids_and_checks_index_before_embedding(providers):
    documents = [Document('quote"\\' + str(i), "été " + str(i)) for i in range(5)]
    async with clients(providers) as (client, index):
        report = await index_documents(
            documents,
            cohere_client=client,
            index=index,
            model="chosen-v4",
            dimension=3,
            namespace='tenant/"quoted"\\',
            batch_size=2,
        )
        # Both native clients remain usable after the pipeline closes its batchers.
        await index.describe_index_stats()
        assert not client.is_closed
    assert providers["requests"][0][1] == "/describe_index_stats"
    embedding_calls = [r for r in providers["requests"] if r[1] == "/v2/embed"]
    assert [text for r in embedding_calls for text in r[2]["texts"]] == [d.text for d in documents]
    for _, _, payload, headers, _ in embedding_calls:
        assert payload == {
            "texts": payload["texts"],
            "model": "chosen-v4",
            "input_type": "search_document",
            "embedding_types": ["float"],
            "output_dimension": 3,
            "truncate": "NONE",
        }
        assert headers[b"authorization"] == b"Bearer local-cohere-key"
        assert 1 <= len(payload["texts"]) <= 2
    writes = [r for r in providers["requests"] if r[1] == "/vectors/upsert"]
    assert [vector["id"] for r in writes for vector in r[2]["vectors"]] == [d.id for d in documents]
    for _, _, payload, headers, body in writes:
        assert payload["namespace"] == 'tenant/"quoted"\\'
        assert headers[b"api-key"] == b"local-pinecone-key"
        estimated = sum(vector_cost(sdk.Vector(**vector)) for vector in payload["vectors"])
        estimated += len(json.dumps(payload["namespace"]).encode()) + 128
        assert len(body) <= estimated <= VECTOR_REQUEST_BUDGET
        assert all(set(vector) == {"id", "values"} for vector in payload["vectors"])
    assert providers["stored"] == {d.id: [float(len(d.text)), -1e-300, 1e100] for d in documents}
    assert report["completed_items"] == 5
    assert report["embedding"]["stats"]["succeeded"] == report["storage"]["stats"]["succeeded"] == 5


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "reply",
    [
        None,
        [],
        {},
        {"embeddings": []},
        {"embeddings": {"float": []}},
        {"embeddings": {"float": [[1, 2, 3], [1, 2]]}},
        {"embeddings": {"float": [[1, 2, 3], [True, 2, 3]]}},
        {"embeddings": {"float": [[1, 2, 3], [float("nan"), 2, 3]]}},
        {"embeddings": {"float": [[1, 2, 3], ["1", 2, 3]]}},
    ],
)
async def test_malformed_embedding_response_never_writes_valid_prefix(providers, reply):
    providers["reply"] = reply
    async with clients(providers) as (client, _):
        embedder = CohereEmbedder(client, model="chosen-v4", dimension=3)
        with pytest.raises(ValueError, match="Cohere"):
            await embedder.embed([Document("one", "first"), Document("two", "second")])
    assert providers["stored"] == {}
    assert [r[1] for r in providers["requests"]] == ["/v2/embed"]


@pytest.mark.asyncio
async def test_malformed_pipeline_response_prevents_upsert(providers):
    providers["reply"] = {"embeddings": {"float": [[1, 2]]}}
    with pytest.raises(ValueError, match="dimension"):
        await pipeline(providers, [Document("one", "text")])
    assert [r[1] for r in providers["requests"]] == ["/describe_index_stats", "/v2/embed"]


@pytest.mark.asyncio
async def test_dimension_mismatch_does_not_consume_source_or_call_cohere(providers):
    providers["dimension"] = 4
    consumed = []

    async def source():
        consumed.append(True)
        yield Document("one", "text")

    with pytest.raises(ValueError, match="index dimension"):
        await pipeline(providers, source())
    assert consumed == []
    assert [r[1] for r in providers["requests"]] == ["/describe_index_stats"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "document",
    [
        Document("", "text"),
        Document("é", "text"),
        Document("x" * 513, "text"),
        Document("nul\x00", "text"),
        Document("ok", " "),
        Document("ok", "x" * 65537),
    ],
)
async def test_invalid_input_is_rejected_before_paid_embedding(providers, document):
    with pytest.raises(ValueError):
        await pipeline(providers, [document])
    assert [r[1] for r in providers["requests"]] == ["/describe_index_stats"]


@pytest.mark.asyncio
async def test_cohere_error_is_propagated_without_recipe_retry(providers):
    providers["cohere_status"] = "429 Too Many Requests"
    providers["reply"] = {"message": "local throttling"}
    with pytest.raises(httpx2.HTTPStatusError) as error:
        await pipeline(providers, [Document("one", "text")])
    assert error.value.response.status_code == 429
    assert [r[1] for r in providers["requests"]].count("/v2/embed") == 1
    assert providers["stored"] == {}


@pytest.mark.asyncio
async def test_failed_later_write_keeps_prior_acknowledged_vector(providers):
    with pytest.raises(sdk.ApiError) as error:
        await pipeline(
            providers, [Document("first", "a"), Document("reject", "b")], batch_size=1, max_pending=1
        )
    assert error.value.status_code == 400
    assert providers["stored"] == {"first": [1.0, -1e-300, 1e100]}


@pytest.mark.asyncio
async def test_two_stage_backpressure_and_cancellation_close_source_and_drain(providers):
    consumed = []
    source_closed = asyncio.Event()
    providers["write_release"] = asyncio.Event()

    async def source():
        try:
            for i in range(20):
                consumed.append(i)
                yield Document(str(i), "text")
        finally:
            source_closed.set()

    before = asyncio.all_tasks()
    task = asyncio.create_task(pipeline(providers, source(), batch_size=1, max_pending=2))
    try:
        await asyncio.wait_for(providers["write_started"].wait(), 2)
        await wait_until(lambda: len(consumed) == 4)
        await asyncio.sleep(0)
        assert consumed == [0, 1, 2, 3]
        task.cancel()
        providers["write_release"].set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 2)
        assert source_closed.is_set()
        assert providers["stored"]
    finally:
        providers["write_release"].set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    await wait_until(lambda: not (asyncio.all_tasks() - before - {asyncio.current_task()}))


@pytest.mark.asyncio
async def test_empty_source_skips_both_paid_stages(providers):
    report = await pipeline(providers, [])
    assert report["completed_items"] == 0
    assert [r[1] for r in providers["requests"]] == ["/describe_index_stats"]


@pytest.mark.asyncio
@pytest.mark.parametrize("namespace", ["", " ", "tenant/é", "nul\x00", "x" * 513])
async def test_invalid_namespace_is_rejected_before_either_provider_call(providers, namespace):
    async with clients(providers) as (client, index):
        with pytest.raises(ValueError, match="Namespace"):
            await index_documents(
                [Document("one", "text")],
                cohere_client=client,
                index=index,
                model="chosen-v4",
                dimension=3,
                namespace=namespace,
            )
    assert providers["requests"] == []


@pytest.mark.asyncio
async def test_high_dimension_vectors_split_under_conservative_serialized_budget(providers):
    providers["dimension"] = 20000
    providers["row"] = [-1.7976931348623157e308, 5e-324] * 10000
    async with clients(providers) as (client, index):
        report = await asyncio.wait_for(
            index_documents(
                [Document(str(i), "text") for i in range(3)],
                cohere_client=client,
                index=index,
                model="explicit-test-v4",
                dimension=20000,
                namespace="tenant",
                batch_size=3,
            ),
            5,
        )
    assert report["completed_items"] == 3
    writes = [r for r in providers["requests"] if r[1] == "/vectors/upsert"]
    assert len(writes) >= 2
    assert all(len(r[2]["vectors"]) <= 2 and len(r[4]) <= VECTOR_REQUEST_BUDGET for r in writes)


@pytest.mark.parametrize(
    ("host", "cohere_key", "pinecone_key", "message"),
    [
        ("http://example.invalid", "dummy-cohere", "dummy-pinecone", "explicit https://"),
        ("https://example.invalid", "", "dummy-pinecone", "Set COHERE_API_KEY"),
        ("https://example.invalid", "dummy-cohere", "", "Set COHERE_API_KEY"),
    ],
)
def test_cli_rejects_configuration_before_opening_clients(
    monkeypatch, capsys, tmp_path, host, cohere_key, pinecone_key, message
):
    import sys

    from examples.recipes import cohere_pinecone as recipe

    monkeypatch.setenv("COHERE_API_KEY", cohere_key)
    monkeypatch.setenv("PINECONE_API_KEY", pinecone_key)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "cohere-pinecone",
            "--input",
            str(tmp_path / "input.jsonl"),
            "--host",
            host,
            "--namespace",
            "tenant",
            "--model",
            "chosen-v4",
            "--dimension",
            "3",
        ],
    )

    def unexpected_run(args):
        pytest.fail("Invalid CLI configuration must not open provider clients")

    monkeypatch.setattr(recipe, "run", unexpected_run)
    with pytest.raises(SystemExit) as error:
        recipe.main()
    assert error.value.code == 2
    assert message in capsys.readouterr().err


def test_cli_parses_explicit_configuration_and_prints_report(monkeypatch, capsys, tmp_path):
    import sys

    from examples.recipes import cohere_pinecone as recipe

    source = tmp_path / "input.jsonl"
    monkeypatch.setenv("COHERE_API_KEY", "dummy-cohere")
    monkeypatch.setenv("PINECONE_API_KEY", "dummy-pinecone")
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "cohere-pinecone",
            "--input",
            str(source),
            "--host",
            "https://example.invalid",
            "--namespace",
            "tenant",
            "--model",
            "chosen-v4",
            "--dimension",
            "3",
        ],
    )
    observed = []

    async def run(args):
        observed.append(vars(args))
        return {"completed_items": 2, "storage": {"stats": {"pending": 0}}}

    monkeypatch.setattr(recipe, "run", run)
    recipe.main()
    assert observed == [
        {
            "input": source,
            "host": "https://example.invalid",
            "namespace": "tenant",
            "model": "chosen-v4",
            "dimension": 3,
        }
    ]
    assert json.loads(capsys.readouterr().out) == {"completed_items": 2, "storage": {"stats": {"pending": 0}}}


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [False, True])
async def test_run_constructs_real_clients_and_closes_them_after_pipeline(
    providers, monkeypatch, tmp_path, failure
):
    from types import SimpleNamespace

    from examples.recipes import cohere_pinecone as recipe

    source = tmp_path / "input.jsonl"
    source.write_text('{"id":"one","text":"first"}\n{"id":"two","text":"second"}\n')
    monkeypatch.setenv("COHERE_API_KEY", "dummy-cohere")
    monkeypatch.setenv("PINECONE_API_KEY", "dummy-pinecone")
    native_client, native_index = httpx2.AsyncClient, sdk.AsyncIndex
    clients_created, index_lifetime = [], []

    def client_factory(**kwargs):
        # Inspect the production configuration, then redirect only the destination
        # to the local wire fixture. Never send dummy credentials to a real provider.
        assert kwargs == {
            "base_url": "https://api.cohere.com",
            "headers": {"Authorization": "Bearer dummy-cohere"},
            "timeout": 30,
            "follow_redirects": False,
        }
        kwargs["base_url"] = providers["host"]
        client = native_client(**kwargs, trust_env=False)
        clients_created.append(client)
        return client

    @asynccontextmanager
    async def index_factory(**kwargs):
        assert kwargs == {"host": "https://index.example.invalid", "api_key": "dummy-pinecone", "timeout": 30}
        kwargs["host"] = providers["host"]
        try:
            async with native_index(**kwargs) as index:
                index_lifetime.append("entered")
                try:
                    yield index
                finally:
                    index_lifetime.append("exiting")
        finally:
            index_lifetime.append("closed")

    # Replace the recipe's module reference, not the shared HTTPX2 SDK module.
    monkeypatch.setattr(recipe, "httpx2", SimpleNamespace(AsyncClient=client_factory))
    monkeypatch.setattr(recipe, "AsyncIndex", index_factory)
    args = SimpleNamespace(
        input=source, host="https://index.example.invalid", namespace="tenant", model="chosen-v4", dimension=3
    )
    if failure:
        providers["cohere_status"] = "429 Too Many Requests"
        with pytest.raises(httpx2.HTTPStatusError):
            await recipe.run(args)
        assert providers["stored"] == {}
    else:
        report = await recipe.run(args)
        assert report["completed_items"] == 2
        assert set(providers["stored"]) == {"one", "two"}
    assert len(clients_created) == 1 and clients_created[0].is_closed
    assert index_lifetime == ["entered", "exiting", "closed"]
    for _, path, _, headers, _ in providers["requests"]:
        if path == "/v2/embed":
            assert headers[b"authorization"] == b"Bearer dummy-cohere"
        else:
            assert headers[b"api-key"] == b"dummy-pinecone"
