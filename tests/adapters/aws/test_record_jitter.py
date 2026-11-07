from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, Mock

import pytest
from async_batcher.aws import _records
from async_batcher.aws.firehose import AsyncFirehoseBatcher
from async_batcher.aws.kinesis import AsyncKinesisBatcher


def send(operation, *, retries=8):
    return _records.send_records(
        operation,
        [{"Data": b"payload"}],
        [7],
        max_bytes=100,
        max_retries=retries,
        response_key="Records",
        success_keys=("RecordId",),
        retry_codes={"retry"},
        retry_jitter=True,
    )


@pytest.mark.parametrize("fraction", [0, 0.5, 1])
@pytest.mark.asyncio
async def test_jitter_uses_full_capped_interval_without_changing_retry_count(monkeypatch, fraction):
    operation = AsyncMock(return_value={"Records": [{"ErrorCode": "retry"}]})
    sleep = AsyncMock()
    random = Mock(side_effect=lambda low, high: low + (high - low) * fraction)
    monkeypatch.setattr(_records.asyncio, "sleep", sleep)
    monkeypatch.setattr(_records.random, "uniform", random)
    result = await send(operation)
    caps = [0.1, 0.2, 0.4, 0.8, 1.6, 3.2, 5, 5]
    assert [call.args for call in random.call_args_list] == [(0, cap) for cap in caps]
    assert [call.args[0] for call in sleep.await_args_list] == [cap * fraction for cap in caps]
    assert operation.await_count == 9
    assert isinstance(result[0], _records.RecordBatchError)
    assert result[0].attempts == 9


@pytest.mark.asyncio
async def test_jitter_does_not_retry_unknown_request_outcomes(monkeypatch):
    failure = TimeoutError("outcome unknown")
    operation = AsyncMock(side_effect=failure)
    sleep, random = AsyncMock(), Mock()
    monkeypatch.setattr(_records.asyncio, "sleep", sleep)
    monkeypatch.setattr(_records.random, "uniform", random)
    assert await send(operation) == [failure]
    assert operation.await_count == 1
    sleep.assert_not_awaited()
    random.assert_not_called()


@pytest.mark.asyncio
async def test_cancellation_during_jitter_wait_prevents_retry(monkeypatch):
    operation = AsyncMock(return_value={"Records": [{"ErrorCode": "retry"}]})
    started = asyncio.Event()

    async def sleep(delay):
        started.set()
        await asyncio.Future()

    monkeypatch.setattr(_records.asyncio, "sleep", sleep)
    task = asyncio.create_task(send(operation))
    try:
        await asyncio.wait_for(started.wait(), 2)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert operation.await_count == 1
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.parametrize(
    "batcher_type, target",
    [
        (AsyncKinesisBatcher, {"stream_name": "events"}),
        (AsyncFirehoseBatcher, {"delivery_stream_name": "events"}),
    ],
)
def test_jitter_is_opt_in_and_validated(batcher_type, target):
    assert batcher_type(None, **target).retry_jitter is False
    assert batcher_type(None, **target, retry_jitter=True).retry_jitter is True
    for invalid in (None, 1, 0, "true"):
        with pytest.raises(TypeError, match="retry_jitter"):
            batcher_type(None, **target, retry_jitter=invalid)
