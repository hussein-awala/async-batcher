from __future__ import annotations

import asyncio
import base64
from contextlib import asynccontextmanager

import pytest

aioboto3 = pytest.importorskip("aioboto3")
web = pytest.importorskip("aiohttp.web")

from aiobotocore.config import AioConfig  # noqa: E402
from async_batcher.aws.firehose import AsyncFirehoseBatcher  # noqa: E402
from async_batcher.aws.kinesis import AsyncKinesisBatcher, KinesisRecord  # noqa: E402
from botocore.exceptions import ConnectionClosedError  # noqa: E402

pytestmark = pytest.mark.asyncio


def acknowledgement(service, value):
    if service == "kinesis":
        return {"ShardId": "shardId-0", "SequenceNumber": value}
    return {"RecordId": value}


def response(service, entries):
    key, count = (
        ("Records", "FailedRecordCount") if service == "kinesis" else ("RequestResponses", "FailedPutCount")
    )
    return web.json_response({key: entries, count: sum("ErrorCode" in entry for entry in entries)})


def inputs(service, count):
    data = [str(index).encode() for index in range(count)]
    return (
        [KinesisRecord(data=value, partition_key="customer") for value in data]
        if service == "kinesis"
        else data
    )


@asynccontextmanager
async def transport(service, reply, *, sdk_attempts=1, max_retries=3, max_batch_size=500):
    requests = []

    async def handle(request):
        payload = await request.json()
        values = [base64.b64decode(record["Data"]).decode() for record in payload["Records"]]
        requests.append(values)
        return await reply(request, values, len(requests))

    app = web.Application()
    app.router.add_post("/", handle)
    runner = web.AppRunner(app)
    await runner.setup()
    try:
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        async with aioboto3.Session().client(
            service,
            region_name="us-east-1",
            endpoint_url=f"http://127.0.0.1:{runner.addresses[0][1]}",
            aws_access_key_id="local-test",
            aws_secret_access_key="local-test",
            config=AioConfig(
                retries={"mode": "standard", "total_max_attempts": sdk_attempts},
                connect_timeout=2,
                read_timeout=2,
            ),
        ) as client:
            options = {"max_retries": max_retries, "max_batch_size": max_batch_size}
            batcher = (
                AsyncKinesisBatcher(client, stream_name="events", **options)
                if service == "kinesis"
                else AsyncFirehoseBatcher(client, delivery_stream_name="events", **options)
            )
            try:
                yield batcher, requests
            finally:
                await batcher.stop(force=True, timeout=5)
    finally:
        await runner.cleanup()


@pytest.mark.parametrize("service", ["kinesis", "firehose"])
async def test_http_partial_acknowledgements_retry_only_rejected_records(service):
    async def reply(request, values, attempt):
        entries = [acknowledgement(service, value) for value in values]
        if attempt == 1:
            entries[1] = {
                "ErrorCode": "ProvisionedThroughputExceededException"
                if service == "kinesis"
                else "ServiceUnavailableException",
                "ErrorMessage": "local rejection",
            }
        return response(service, entries)

    async with transport(service, reply) as (batcher, requests):
        async with asyncio.timeout(10):
            results = await asyncio.gather(*(batcher.process(item) for item in inputs(service, 3)))
        assert requests == [["0", "1", "2"], ["1"]]
        assert results == [acknowledgement(service, str(index)) for index in range(3)]


@pytest.mark.parametrize("service", ["kinesis", "firehose"])
@pytest.mark.parametrize("sdk_attempts", [1, 2], ids=["sdk-retries-disabled", "sdk-retries-enabled"])
async def test_lost_http_reply_preserves_prior_chunk_and_leaves_retry_ownership_to_sdk(service, sdk_attempts):
    accepted = []

    async def reply(request, values, attempt):
        # Model acceptance followed by a lost reply. A retry may repeat an accepted
        # record; an acknowledgement cannot establish exactly-once external effects.
        accepted.extend(values)
        if attempt == 2:
            request.transport.abort()
            return web.Response()
        return response(service, [acknowledgement(service, value) for value in values])

    # Disable adapter record retries when SDK retries are enabled, and vice versa.
    async with transport(
        service, reply, sdk_attempts=sdk_attempts, max_retries=3 if sdk_attempts == 1 else 0
    ) as (
        batcher,
        requests,
    ):
        async with asyncio.timeout(10):
            results = await batcher.process_batch(inputs(service, 501))
        assert results[:500] == [acknowledgement(service, str(index)) for index in range(500)]
        assert requests[:2] == [[str(index) for index in range(500)], ["500"]]
        if sdk_attempts == 1:
            assert isinstance(results[500], ConnectionClosedError)
            assert len(requests) == 2
            assert accepted.count("500") == 1
        else:
            assert results[500] == acknowledgement(service, "500")
            assert requests[2:] == [["500"]]
            assert accepted.count("500") == 2


@pytest.mark.parametrize("service", ["kinesis", "firehose"])
@pytest.mark.parametrize("force_stop", [False, True], ids=["caller-cancellation", "force-stop"])
async def test_cancellation_of_real_http_request_keeps_client_owned_by_caller(service, force_stop):
    started = asyncio.Event()
    release = asyncio.Event()

    async def reply(request, values, attempt):
        if attempt == 1:
            started.set()
            await release.wait()
        return response(service, [acknowledgement(service, value) for value in values])

    async with transport(service, reply, max_batch_size=2) as (batcher, requests):
        tasks = [asyncio.create_task(batcher.process(item)) for item in inputs(service, 2)]
        try:
            await asyncio.wait_for(started.wait(), 2)
            if force_stop:
                await batcher.stop(force=True, timeout=5)
                outcomes = await asyncio.gather(*tasks, return_exceptions=True)
                assert all(isinstance(outcome, asyncio.CancelledError) for outcome in outcomes)
            else:
                tasks[0].cancel()
                with pytest.raises(asyncio.CancelledError):
                    await tasks[0]
                assert not tasks[1].done()
            release.set()
            if not force_stop:
                assert await asyncio.wait_for(tasks[1], 5) == acknowledgement(service, "1")
                await batcher.stop(timeout=5)
            # The same real SDK client remains usable after the batcher stops.
            assert await asyncio.wait_for(batcher.process_batch(inputs(service, 1)), 5) == [
                acknowledgement(service, "0")
            ]
            assert requests == [["0", "1"], ["0"]]
        finally:
            release.set()
            await batcher.stop(force=True, timeout=5)
            await asyncio.gather(*tasks, return_exceptions=True)
