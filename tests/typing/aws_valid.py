"""Strict Kinesis consumers with the locked aiobotocore client stubs."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextvars import Context
from typing import Any, assert_type

from aiobotocore.client import AioBaseClient
from async_batcher import BatcherStats, BatchEvent
from async_batcher.aws.kinesis import AsyncKinesisBatcher, KinesisRecord, RecordBatchError


def observe(event: BatchEvent) -> None:
    assert_type(event.failed, int)


def record_cost(record: KinesisRecord) -> int:
    return len(record.data)


def create(client: AioBaseClient) -> AsyncKinesisBatcher:
    return AsyncKinesisBatcher(
        client,
        stream_name="events",
        max_batch_size=10,
        max_retries=2,
        retry_jitter=True,
        max_queue_size=20,
        max_batch_cost=100,
        item_cost=record_cost,
        on_batch=observe,
        processor_context=Context(),
        log_exceptions=False,
        discard_cancelled=True,
    )


async def consume(client: AioBaseClient) -> None:
    record = KinesisRecord(data=b"payload", partition_key="key")
    assert_type(record.data, bytes)
    assert_type(record.partition_key, str)
    async with create(client) as batcher:
        assert_type(batcher, AsyncKinesisBatcher)
        assert_type(batcher.client, AioBaseClient)
        assert_type(await batcher.process(record, wait_for_space=True), dict[str, Any])
        assert_type(batcher.map([record], max_pending=2), AsyncGenerator[dict[str, Any], None])
        assert_type(batcher.stats(), BatcherStats)
    error = RecordBatchError("InternalFailure", "rejected", 3)
    assert_type(error.code, str)
    assert_type(error.attempts, int)
