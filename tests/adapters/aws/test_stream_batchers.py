from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, Mock

import pytest
import pytest_asyncio

aioboto3 = pytest.importorskip("aioboto3")
AioStubber = pytest.importorskip("aiobotocore.stub").AioStubber
ClientError = pytest.importorskip("botocore.exceptions").ClientError
_records = pytest.importorskip("async_batcher.aws._records")
AsyncFirehoseBatcher = pytest.importorskip("async_batcher.aws.firehose").AsyncFirehoseBatcher
kinesis = pytest.importorskip("async_batcher.aws.kinesis")
AsyncKinesisBatcher = kinesis.AsyncKinesisBatcher
KinesisRecord = kinesis.KinesisRecord
RecordBatchError = kinesis.RecordBatchError


@pytest_asyncio.fixture(params=["kinesis", "firehose"])
async def stream(request):
    service = request.param
    session = aioboto3.Session()
    async with session.client(
        service, region_name="us-east-1", aws_access_key_id="test", aws_secret_access_key="test"
    ) as client:
        if service == "kinesis":
            batcher = AsyncKinesisBatcher(client, stream_name="events", max_batch_size=2, max_queue_time=0)
            method, key, count = "put_records", "Records", "FailedRecordCount"
            target = {"StreamName": "events"}
            inputs = [KinesisRecord(data=bytes([i]), partition_key="clé") for i in range(3)]
            records = [{"Data": item.data, "PartitionKey": item.partition_key} for item in inputs]
            successes = [{"ShardId": "shardId-0", "SequenceNumber": str(i)} for i in range(3)]
            code = "ProvisionedThroughputExceededException"
        else:
            batcher = AsyncFirehoseBatcher(
                client, delivery_stream_name="events", max_batch_size=2, max_queue_time=0
            )
            method, key, count = "put_record_batch", "RequestResponses", "FailedPutCount"
            target = {"DeliveryStreamName": "events"}
            inputs = [bytes([i]) for i in range(3)]
            records = [{"Data": item} for item in inputs]
            successes = [{"RecordId": str(i)} for i in range(3)]
            code = "ServiceUnavailableException"
        with AioStubber(client) as stubber:
            yield {
                "batcher": batcher,
                "client": client,
                "stubber": stubber,
                "method": method,
                "key": key,
                "count": count,
                "target": target,
                "inputs": inputs,
                "records": records,
                "successes": successes,
                "error": {"ErrorCode": code, "ErrorMessage": "retry"},
            }
            await batcher.stop(force=True)
            stubber.assert_no_pending_responses()


def respond(stream, records, entries):
    response = {stream["key"]: entries}
    failed = sum("ErrorCode" in entry for entry in entries)
    # The Kinesis SDK model incorrectly requires FailedRecordCount >= 1 when
    # present. Omit its optional zero count here; HTTP tests cover the real zero.
    if failed or stream["method"] != "put_records":
        response[stream["count"]] = failed
    stream["stubber"].add_response(
        stream["method"],
        response,
        {"Records": records, **stream["target"]},
    )


@pytest.mark.asyncio
async def test_sdk_requests_and_per_item_acknowledgements(stream):
    respond(stream, stream["records"][:2], stream["successes"][:2])
    results = await asyncio.gather(*(stream["batcher"].process(item) for item in stream["inputs"][:2]))
    assert results == stream["successes"][:2]


@pytest.mark.asyncio
@pytest.mark.parametrize("jitter", [False, True])
async def test_partial_retry_preserves_order_and_does_not_resend_successes(stream, monkeypatch, jitter):
    sleep = AsyncMock()
    random = Mock(side_effect=lambda low, high: high / 4)
    monkeypatch.setattr(_records.asyncio, "sleep", sleep)
    monkeypatch.setattr(_records.random, "uniform", random)
    stream["batcher"].retry_jitter = jitter
    respond(stream, stream["records"], [stream["error"], stream["successes"][1], stream["error"]])
    respond(stream, [stream["records"][0], stream["records"][2]], [stream["error"], stream["successes"][2]])
    respond(stream, [stream["records"][0]], [stream["successes"][0]])
    assert await stream["batcher"].process_batch(stream["inputs"]) == stream["successes"]
    assert [call.args for call in sleep.await_args_list] == (
        [(0.025,), (0.05,)] if jitter else [(0.1,), (0.2,)]
    )
    assert random.call_count == (2 if jitter else 0)


@pytest.mark.asyncio
async def test_exhaustion_fails_only_the_rejected_caller(stream, monkeypatch):
    monkeypatch.setattr(_records.asyncio, "sleep", AsyncMock())
    stream["batcher"].max_retries = 1
    respond(stream, stream["records"][:2], [stream["successes"][0], stream["error"]])
    respond(stream, [stream["records"][1]], [stream["error"]])
    results = await asyncio.gather(
        *(stream["batcher"].process(item) for item in stream["inputs"][:2]), return_exceptions=True
    )
    assert results[0] == stream["successes"][0]
    assert isinstance(results[1], RecordBatchError)
    assert results[1].code == stream["error"]["ErrorCode"]
    assert results[1].attempts == 2


@pytest.mark.asyncio
async def test_request_exception_keeps_prior_acknowledgements(stream, monkeypatch):
    monkeypatch.setattr(_records.asyncio, "sleep", AsyncMock())
    respond(stream, stream["records"][:2], [stream["successes"][0], stream["error"]])
    stream["stubber"].add_client_error(
        stream["method"],
        service_error_code="AccessDeniedException",
        expected_params={"Records": [stream["records"][1]], **stream["target"]},
    )
    results = await stream["batcher"].process_batch(stream["inputs"][:2])
    assert results[0] == stream["successes"][0]
    assert isinstance(results[1], ClientError)


@pytest.mark.asyncio
async def test_unknown_record_error_is_not_retried(stream):
    error = {"ErrorCode": "InvalidArgumentException", "ErrorMessage": "bad record"}
    respond(stream, stream["records"][:1], [error])
    results = await stream["batcher"].process_batch(stream["inputs"][:1])
    assert isinstance(results[0], RecordBatchError)
    assert results[0].attempts == 1


@pytest.mark.asyncio
async def test_empty_batch_does_not_call_sdk(stream):
    assert await stream["batcher"].process_batch([]) == []


@pytest.mark.asyncio
async def test_stop_does_not_close_borrowed_client(stream, monkeypatch):
    close = AsyncMock()
    monkeypatch.setattr(stream["client"], "close", close)
    await stream["batcher"].stop()
    close.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "entries",
    [
        [],
        [{}],
        [None],
        ["ack"],
        [{"ErrorCode": 1}],
        [{"RecordId": ""}],
        [{"ErrorCode": "InternalFailure", "ErrorMessage": None}],
    ],
)
async def test_malformed_response_does_not_report_success(stream, entries, monkeypatch):
    call = AsyncMock(return_value={stream["key"]: entries})
    monkeypatch.setattr(stream["client"], stream["method"], call)
    result = await stream["batcher"].process_batch(stream["inputs"][:1])
    assert isinstance(result[0], ValueError)
    call.assert_awaited_once()


@pytest.mark.asyncio
async def test_force_stop_cancels_pending_sdk_call(stream, monkeypatch):
    started = asyncio.Event()
    cancelled = asyncio.Event()

    async def pending(**kwargs):
        started.set()
        try:
            await asyncio.Future()
        finally:
            cancelled.set()

    monkeypatch.setattr(stream["client"], stream["method"], pending)
    request = asyncio.create_task(stream["batcher"].process(stream["inputs"][0]))
    await asyncio.wait_for(started.wait(), 1)
    await asyncio.wait_for(stream["batcher"].stop(force=True), 1)
    assert cancelled.is_set()
    assert request.cancelled()


@pytest.mark.parametrize("size", [-1, 0, 501, True])
def test_batch_size_limits(size):
    with pytest.raises(ValueError):
        AsyncKinesisBatcher(None, stream_name="events", max_batch_size=size)
    with pytest.raises(ValueError):
        AsyncFirehoseBatcher(None, delivery_stream_name="events", max_batch_size=size)


@pytest.mark.parametrize(
    "batcher_type, target",
    [
        (AsyncKinesisBatcher, {"stream_name": "events"}),
        (AsyncFirehoseBatcher, {"delivery_stream_name": "events"}),
    ],
)
@pytest.mark.parametrize("size", [1, 500])
def test_legacy_batch_size_overrides_explicit_limit_before_validation(batcher_type, target, size):
    with pytest.warns(UserWarning, match="batch_size.*deprecated"):
        batcher = batcher_type(None, **target, max_batch_size=501, batch_size=size)
    assert batcher.max_batch_size == size


@pytest.mark.parametrize(
    "batcher_type, target",
    [
        (AsyncKinesisBatcher, {"stream_name": "events"}),
        (AsyncFirehoseBatcher, {"delivery_stream_name": "events"}),
    ],
)
@pytest.mark.parametrize("size", [-1, 501])
def test_legacy_batch_size_cannot_bypass_service_count_limit(batcher_type, target, size):
    with pytest.warns(UserWarning, match="batch_size.*deprecated"):
        with pytest.raises(ValueError, match="between 1 and 500"):
            batcher_type(None, **target, batch_size=size)


@pytest.mark.parametrize("retries", [-1, True, 1.5, None])
def test_retry_validation(retries):
    with pytest.raises(ValueError, match="max_retries"):
        AsyncKinesisBatcher(None, stream_name="events", max_retries=retries)
    with pytest.raises(ValueError, match="max_retries"):
        AsyncFirehoseBatcher(None, delivery_stream_name="events", max_retries=retries)


@pytest.mark.parametrize("target", [{}, {"stream_name": "a", "stream_arn": "b"}, {"stream_name": ""}])
def test_kinesis_requires_one_target(target):
    with pytest.raises(ValueError):
        AsyncKinesisBatcher(None, **target)


@pytest.mark.asyncio
@pytest.mark.parametrize("hash_key", ["-1", "01", "١", "1.0", str(2**128), 1])
async def test_kinesis_hash_key_validation(hash_key):
    with pytest.raises(ValueError, match="explicit_hash_key"):
        await AsyncKinesisBatcher(None, stream_name="events").process_batch(
            [KinesisRecord(data=b"x", partition_key="p", explicit_hash_key=hash_key)]
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("key", ["", "a" * 257, None])
async def test_kinesis_partition_key_validation(key):
    with pytest.raises(ValueError, match="partition_key"):
        await AsyncKinesisBatcher(None, stream_name="events").process_batch(
            [KinesisRecord(data=b"x", partition_key=key)]
        )


@pytest.mark.asyncio
async def test_kinesis_counts_utf8_key_bytes_in_record_limit():
    with pytest.raises(ValueError, match="10 MiB"):
        await AsyncKinesisBatcher(None, stream_name="events").process_batch(
            [KinesisRecord(data=b"x" * (10 * 1024 * 1024 - 1), partition_key="é")]
        )


@pytest.mark.asyncio
async def test_byte_limits_split_native_requests(stream):
    if stream["method"] == "put_records":
        data = b"x" * (5 * 1024 * 1024)
        items = [KinesisRecord(data=data, partition_key="p")] * 2
        requests = [[{"Data": data, "PartitionKey": "p"}]] * 2
        responses = [[stream["successes"][0]], [stream["successes"][1]]]
    else:
        data = b"x" * (1000 * 1024)
        items = [data] * 5
        requests = [[{"Data": data}] * 4, [{"Data": data}]]
        responses = [[stream["successes"][0]] * 4, [stream["successes"][1]]]
    for records, entries in zip(requests, responses, strict=True):
        respond(stream, records, entries)
    assert await stream["batcher"].process_batch(items) == sum(responses, [])


@pytest.mark.asyncio
async def test_large_direct_batch_splits_at_500_records(stream):
    respond(stream, [stream["records"][0]] * 500, [stream["successes"][0]] * 500)
    respond(stream, [stream["records"][0]], [stream["successes"][0]])
    result = await stream["batcher"].process_batch([stream["inputs"][0]] * 501)
    assert result == [stream["successes"][0]] * 501


@pytest.mark.asyncio
async def test_kinesis_arn_and_explicit_hash_use_sdk_schema():
    session = aioboto3.Session()
    async with session.client(
        "kinesis", region_name="us-east-1", aws_access_key_id="test", aws_secret_access_key="test"
    ) as client:
        arn = "arn:aws:kinesis:us-east-1:123456789012:stream/events"
        data = KinesisRecord(data=b"x", partition_key="p", explicit_hash_key=str(2**128 - 1))
        success = {"ShardId": "shardId-0", "SequenceNumber": "1"}
        with AioStubber(client) as stubber:
            stubber.add_response(
                "put_records",
                {"Records": [success]},
                {
                    "StreamARN": arn,
                    "Records": [
                        {"Data": b"x", "PartitionKey": "p", "ExplicitHashKey": data.explicit_hash_key}
                    ],
                },
            )
            assert await AsyncKinesisBatcher(client, stream_arn=arn).process_batch([data]) == [success]
            stubber.assert_no_pending_responses()


@pytest.mark.asyncio
@pytest.mark.parametrize("data", [b"x" * (1000 * 1024 + 1), "text", bytearray(b"x")])
async def test_firehose_prevalidates_entire_batch(data):
    with pytest.raises(ValueError, match="1,000 KiB"):
        await AsyncFirehoseBatcher(None, delivery_stream_name="events").process_batch([b"valid", data])


@pytest.mark.asyncio
@pytest.mark.parametrize("service", ["kinesis", "firehose"])
async def test_real_sdk_serializes_bytes_and_reads_zero_failure_response(service):
    web = pytest.importorskip("aiohttp.web")
    seen = []
    response = (
        {"FailedRecordCount": 0, "Records": [{"ShardId": "shardId-0", "SequenceNumber": "123"}]}
        if service == "kinesis"
        else {"FailedPutCount": 0, "RequestResponses": [{"RecordId": "record-id"}]}
    )

    async def handle(request):
        seen.append((request.headers["X-Amz-Target"], await request.json()))
        return web.json_response(response)

    app = web.Application()
    app.router.add_post("/", handle)
    runner = web.AppRunner(app)
    await runner.setup()
    try:
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = runner.addresses[0][1]
        session = aioboto3.Session()
        async with session.client(
            service,
            region_name="us-east-1",
            endpoint_url=f"http://127.0.0.1:{port}",
            aws_access_key_id="local-test",
            aws_secret_access_key="local-test",
        ) as client:
            if service == "kinesis":
                batcher = AsyncKinesisBatcher(client, stream_name="events")
                value = KinesisRecord(data=b"hello\x00", partition_key="p")
                expected = {"Data": "aGVsbG8A", "PartitionKey": "p"}
            else:
                batcher = AsyncFirehoseBatcher(client, delivery_stream_name="events")
                value = b"hello\x00"
                expected = {"Data": "aGVsbG8A"}
            try:
                result = await asyncio.wait_for(batcher.process(value), 3)
            finally:
                await batcher.stop(force=True)
        assert result == next(value for value in response.values() if isinstance(value, list))[0]
        assert len(seen) == 1
        assert seen[0][0].endswith(".PutRecords" if service == "kinesis" else ".PutRecordBatch")
        assert seen[0][1]["Records"] == [expected]
    finally:
        await runner.cleanup()


@pytest.mark.asyncio
async def test_zero_retries_returns_first_rejection(stream):
    stream["batcher"].max_retries = 0
    respond(stream, stream["records"][:1], [stream["error"]])
    result = await stream["batcher"].process_batch(stream["inputs"][:1])
    assert isinstance(result[0], RecordBatchError)
    assert result[0].attempts == 1


@pytest.mark.asyncio
async def test_kinesis_prevalidates_before_any_write():
    with pytest.raises(ValueError, match="data must be bytes"):
        await AsyncKinesisBatcher(None, stream_name="events").process_batch(
            [
                KinesisRecord(data=b"valid", partition_key="p"),
                KinesisRecord(data="invalid", partition_key="p"),
            ]
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("code", [[], {}, 1, None, ""])
async def test_malformed_error_with_ack_fields_preserves_prior_chunk_success(stream, monkeypatch, code):
    acknowledgement = stream["successes"][0]
    malformed = {**acknowledgement, "ErrorCode": code}
    call = AsyncMock(
        side_effect=[
            {stream["key"]: [acknowledgement] * 500},
            {stream["key"]: [malformed]},
        ]
    )
    monkeypatch.setattr(stream["client"], stream["method"], call)
    results = await stream["batcher"].process_batch([stream["inputs"][0]] * 501)
    assert results[:500] == [acknowledgement] * 500
    assert isinstance(results[500], ValueError)
    assert call.await_count == 2


@pytest.mark.parametrize("target", [None, "", b"events", 123])
def test_firehose_rejects_invalid_stream_names(target):
    with pytest.raises(ValueError, match="delivery_stream_name"):
        AsyncFirehoseBatcher(None, delivery_stream_name=target)
