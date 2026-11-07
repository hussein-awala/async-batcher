from __future__ import annotations

import asyncio
import json

import pytest

aioboto3 = pytest.importorskip("aioboto3")

from async_batcher.aws.dynamodb.get import AsyncDynamoDbGetBatcher, GetItem  # noqa: E402
from async_batcher.aws.dynamodb.write import AsyncDynamoDbWriteBatcher, WriteOperation  # noqa: E402


@pytest.mark.asyncio
async def test_patched_transports_preserve_dynamodb_resource_requests():
    requests = []

    async def respond(reader, writer):
        try:
            header = await reader.readuntil(b"\r\n\r\n")
            headers = dict(line.split(b":", 1) for line in header.split(b"\r\n")[1:] if b":" in line)
            headers = {key.lower(): value.strip() for key, value in headers.items()}
            payload = json.loads(await reader.readexactly(int(headers[b"content-length"])))
            operation = headers[b"x-amz-target"].decode().split(".")[-1]
            requests.append((operation, payload))
            if operation == "BatchGetItem":
                response = {"Responses": {"test": [{"id": {"S": "one"}, "value": {"N": "3"}}]}}
            else:
                response = {"UnprocessedItems": {}}
            body = json.dumps(response).encode()
            writer.write(
                b"HTTP/1.1 200 OK\r\nContent-Type: application/x-amz-json-1.0\r\n"
                + f"Content-Length: {len(body)}\r\nConnection: close\r\n\r\n".encode()
                + body
            )
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_server(respond, "127.0.0.1", 0)
    async with server:
        port = server.sockets[0].getsockname()[1]
        session = aioboto3.Session(
            aws_access_key_id="test", aws_secret_access_key="test", region_name="us-west-2"
        )
        kwargs = {"aioboto3_session": session, "endpoint_url": f"http://127.0.0.1:{port}"}
        reader = AsyncDynamoDbGetBatcher(**kwargs)
        writer = AsyncDynamoDbWriteBatcher(**kwargs)
        result = await asyncio.wait_for(
            reader.process_batch([GetItem(table_name="test", key={"id": "one"})]), 5
        )
        assert result == [{"id": "one", "value": 3}]
        await asyncio.wait_for(
            writer.process_batch([WriteOperation(operation="PUT", table_name="test", data={"id": "two"})]), 5
        )

    assert [operation for operation, _ in requests] == ["BatchGetItem", "BatchWriteItem"]
    assert requests[0][1]["RequestItems"]["test"]["Keys"] == [{"id": {"S": "one"}}]
    assert requests[1][1]["RequestItems"]["test"] == [{"PutRequest": {"Item": {"id": {"S": "two"}}}}]
