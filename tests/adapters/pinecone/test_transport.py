from __future__ import annotations

import asyncio
import json
from urllib.parse import parse_qs, urlsplit

import pytest
import pytest_asyncio

sdk = pytest.importorskip("pinecone")

from async_batcher.pinecone import (  # noqa: E402
    AsyncPineconeDeleteBatcher,
    AsyncPineconeFetchBatcher,
    AsyncPineconeUpsertBatcher,
)


@pytest_asyncio.fixture
async def pinecone_server():
    requests = []
    failures = []

    async def respond(reader, writer):
        try:
            header = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), 5)
            lines = header.split(b"\r\n")
            method, target, _ = lines[0].decode().split(" ")
            headers = dict(line.split(b":", 1) for line in lines[1:] if b":" in line)
            headers = {key.lower(): value.strip() for key, value in headers.items()}
            body = await reader.readexactly(int(headers.get(b"content-length", b"0")))
            payload = json.loads(body) if body else None
            url = urlsplit(target)
            query = parse_qs(url.query)
            requests.append((method, url.path, query, payload, headers))
            identifiers = query.get("ids", []) if method == "GET" else (payload or {}).get("ids", [])
            if payload and "vectors" in payload:
                identifiers = [vector["id"] for vector in payload["vectors"]]
            if "oversized" in identifiers:
                status, response = "413 Payload Too Large", {"code": 8, "message": "test request too large"}
            elif "rejected" in identifiers:
                status, response = "400 Bad Request", {"code": 3, "message": "test rejection"}
            elif url.path == "/vectors/upsert":
                status, response = "200 OK", {"upsertedCount": len(payload["vectors"])}
            elif url.path == "/vectors/fetch":
                status, response = (
                    "200 OK",
                    {
                        "namespace": query.get("namespace", [""])[0],
                        "vectors": {
                            "sparse": {"id": "sparse", "sparseValues": {"indices": [9], "values": [0.7]}},
                            "dense": {"id": "dense", "values": [0.2, 0.8], "metadata": {"label": "stored"}},
                        },
                    },
                )
            elif url.path == "/vectors/delete":
                status, response = "200 OK", {}
            else:
                raise AssertionError(f"Unexpected SDK request: {method} {target}")
            encoded = json.dumps(response).encode()
            writer.write(
                f"HTTP/1.1 {status}\r\nContent-Type: application/json\r\n"
                f"Content-Length: {len(encoded)}\r\nConnection: close\r\n\r\n".encode()
                + encoded
            )
            await writer.drain()
        except Exception as error:
            failures.append(error)
        finally:
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_server(respond, "127.0.0.1", 0)
    async with server:
        yield f"http://127.0.0.1:{server.sockets[0].getsockname()[1]}", requests
    assert not failures


@pytest.mark.asyncio
async def test_real_sdk_serializes_native_batches_and_decodes_vector_responses(pinecone_server):
    host, requests = pinecone_server
    async with sdk.AsyncIndex(host=host, api_key="local-test-key", timeout=2) as index:
        upsert = AsyncPineconeUpsertBatcher(index=index, namespace="article space")
        fetch = AsyncPineconeFetchBatcher(index=index, namespace="article space")
        delete = AsyncPineconeDeleteBatcher(index=index, namespace="article space")
        vectors = [
            sdk.Vector(id="dense", values=[0.2, 0.8], metadata={"label": "stored"}),
            sdk.Vector(id="sparse", sparse_values=sdk.SparseValues(indices=[9], values=[0.7])),
        ]
        try:
            assert await asyncio.wait_for(upsert.process_batch(vectors), 5) == [None, None]
            results = await asyncio.wait_for(fetch.process_batch(["dense", "missing", "sparse", "dense"]), 5)
            assert results[0] == vectors[0]
            assert results[1] is None
            assert results[2] == vectors[1]
            assert results[3] == vectors[0]
            assert isinstance(results[0], sdk.Vector)
            assert (
                await asyncio.wait_for(delete.process_batch(["dense", "missing", "dense"]), 5) == [None] * 3
            )
        finally:
            await asyncio.gather(upsert.stop(), fetch.stop(), delete.stop())
    assert [request[:2] for request in requests] == [
        ("POST", "/vectors/upsert"),
        ("GET", "/vectors/fetch"),
        ("POST", "/vectors/delete"),
    ]
    assert requests[0][3] == {
        "namespace": "article space",
        "vectors": [
            {"id": "dense", "values": [0.2, 0.8], "metadata": {"label": "stored"}},
            {"id": "sparse", "values": [], "sparseValues": {"indices": [9], "values": [0.7]}},
        ],
    }
    assert requests[1][2] == {"ids": ["dense", "missing", "sparse"], "namespace": ["article space"]}
    assert requests[2][3] == {"ids": ["dense", "missing"], "namespace": "article space"}
    assert all(request[4][b"api-key"] == b"local-test-key" for request in requests)


@pytest.mark.asyncio
async def test_real_sdk_http_error_is_preserved_for_each_queued_request(pinecone_server):
    host, requests = pinecone_server
    async with sdk.AsyncIndex(host=host, api_key="local-test-key", timeout=2) as index:
        batcher = AsyncPineconeFetchBatcher(index=index, max_batch_size=2)
        try:
            results = await asyncio.wait_for(
                asyncio.gather(batcher.process("rejected"), batcher.process("dense"), return_exceptions=True),
                5,
            )
            assert all(isinstance(result, sdk.ApiError) and result.status_code == 400 for result in results)
        finally:
            await batcher.stop()
    assert len(requests) == 1


@pytest.mark.asyncio
async def test_invalid_vector_id_is_rejected_before_sending(pinecone_server):
    host, requests = pinecone_server
    async with sdk.AsyncIndex(host=host, api_key="local-test-key", timeout=2) as index:
        batcher = AsyncPineconeUpsertBatcher(index=index)
        with pytest.raises(ValueError, match="ASCII"):
            await batcher.process_batch([sdk.Vector(id="", values=[1])])
    assert requests == []


@pytest.mark.asyncio
async def test_upsert_size_rejection_is_propagated_without_hidden_bulk_splitting(pinecone_server):
    host, requests = pinecone_server
    async with sdk.AsyncIndex(host=host, api_key="local-test-key", timeout=2) as index:
        batcher = AsyncPineconeUpsertBatcher(index=index, max_batch_size=2)
        try:
            results = await asyncio.wait_for(
                asyncio.gather(
                    batcher.process(sdk.Vector(id="oversized", values=[1])),
                    batcher.process(sdk.Vector(id="peer", values=[2])),
                    return_exceptions=True,
                ),
                5,
            )
            assert all(isinstance(result, sdk.PayloadTooLargeError) for result in results)
        finally:
            await batcher.stop()
    assert len(requests) == 1
