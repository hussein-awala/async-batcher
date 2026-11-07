from __future__ import annotations

import asyncio
import json

import pytest
import pytest_asyncio

httpx2 = pytest.importorskip("httpx2")
from examples.recipes import provider_policy as policy  # noqa: E402


@pytest_asyncio.fixture
async def provider():
    state = {"requests": [], "reject": 1, "retry_after": "2", "errors": []}
    tasks = set()

    async def handler(reader, writer):
        task = asyncio.current_task()
        tasks.add(task)
        try:
            async with asyncio.timeout(5):
                header = await reader.readuntil(b"\r\n\r\n")
                fields = dict(line.split(b":", 1) for line in header.split(b"\r\n")[1:] if b":" in line)
                fields = {key.lower(): value.strip() for key, value in fields.items()}
                payload = json.loads(await reader.readexactly(int(fields[b"content-length"])))
                state["requests"].append(payload)
                rejected = len(state["requests"]) <= state["reject"]
                status = "429 Too Many Requests" if rejected else "200 OK"
                body = json.dumps({"values": [value * 2 for value in payload["items"]]}).encode()
                writer.write(
                    (
                        f"HTTP/1.1 {status}\r\nContent-Length: {len(body)}\r\n"
                        f"Retry-After: {state['retry_after']}\r\nContent-Type: application/json\r\n"
                        "Connection: close\r\n\r\n"
                    ).encode()
                    + body
                )
                await writer.drain()
        except Exception as error:
            state["errors"].append(error)
        finally:
            writer.close()
            await writer.wait_closed()
            tasks.discard(task)

    server = await asyncio.start_server(handler, "127.0.0.1", 0)
    state["host"] = f"http://127.0.0.1:{server.sockets[0].getsockname()[1]}"
    async with server:
        try:
            yield state
        finally:
            for task in list(tasks):
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
    assert not state["errors"]


async def post(client, payload):
    response = await client.post("/bulk", json=payload)
    if response.status_code == 429:
        raise policy.Throttled(response.headers.get("Retry-After"))
    response.raise_for_status()
    return response.json()["values"]


@pytest.mark.asyncio
async def test_real_http_429_is_retried_once_after_server_minimum(provider, monkeypatch):
    now, waits = [0.0], []
    monkeypatch.setattr(policy, "monotonic", lambda: now[0])

    async def advance(seconds):
        waits.append(seconds)
        now[0] += seconds

    monkeypatch.setattr(policy, "sleep", advance)
    limiter = policy.ProviderPolicy(policy.Pacer(requests_per_second=10), base_delay=0.1)
    async with httpx2.AsyncClient(
        base_url=provider["host"], trust_env=False, transport=httpx2.AsyncHTTPTransport(retries=0), timeout=2
    ) as client:

        async def operation():
            return await post(client, {"items": [2, 3]})

        assert await limiter.run(operation, units=2) == [4, 6]
        assert not client.is_closed
    assert provider["requests"] == [{"items": [2, 3]}, {"items": [2, 3]}]
    assert waits == [2]
    assert (limiter.attempts, limiter.retry_attempts, limiter.throttled_responses) == (2, 1, 1)


@pytest.mark.asyncio
async def test_cancellation_during_real_http_throttle_backoff_sends_no_retry(provider, monkeypatch):
    waiting = asyncio.Event()

    async def held(seconds):
        waiting.set()
        await asyncio.Future()

    monkeypatch.setattr(policy, "sleep", held)
    limiter = policy.ProviderPolicy(policy.Pacer(requests_per_second=10))
    async with httpx2.AsyncClient(
        base_url=provider["host"], trust_env=False, transport=httpx2.AsyncHTTPTransport(retries=0), timeout=2
    ) as client:

        async def operation():
            return await post(client, {"items": [1]})

        task = asyncio.create_task(limiter.run(operation))
        try:
            await asyncio.wait_for(waiting.wait(), 3)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
    assert provider["requests"] == [{"items": [1]}]
    assert limiter.attempts == 1 and limiter.retry_attempts == 0
