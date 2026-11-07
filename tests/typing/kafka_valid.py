"""Strict installed-wheel consumers using the real locked aiokafka types."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextvars import Context
from typing import assert_type

from aiokafka import AIOKafkaProducer
from aiokafka.structs import RecordMetadata
from async_batcher import BatcherStats, BatchEvent
from async_batcher.kafka import AsyncKafkaBatcher, KafkaRecord


def observe(event: BatchEvent) -> None:
    assert_type(event.succeeded, int)


def record_cost(record: KafkaRecord) -> int:
    return len(record.topic.encode())


def create(producer: AIOKafkaProducer) -> AsyncKafkaBatcher:
    return AsyncKafkaBatcher(
        producer=producer,
        max_batch_size=10,
        max_batch_cost=100,
        item_cost=record_cost,
        on_batch=observe,
        processor_context=Context(),
        log_exceptions=False,
        discard_cancelled=True,
        max_queue_size=20,
        concurrency=1,
    )


async def consume(producer: AIOKafkaProducer) -> None:
    record = KafkaRecord(topic="events", partition=0, value=b"payload", headers=(("kind", b"demo"),))
    assert_type(record.topic, str)
    assert_type(record.partition, int)
    async with create(producer) as batcher:
        assert_type(batcher, AsyncKafkaBatcher)
        assert_type(batcher.producer, AIOKafkaProducer)
        result = await batcher.process(record, wait_for_space=True, enqueue_timeout=0.5)
        assert_type(result, RecordMetadata | None)
        if result is not None:
            assert_type(result.topic, str)
            assert_type(result.offset, int)
            assert_type(result.timestamp, int | None)
        assert_type(batcher.map([record], max_pending=2), AsyncGenerator[RecordMetadata | None, None])
        assert_type(batcher.stats(), BatcherStats)


def metadata_fields_are_not_any(metadata: RecordMetadata) -> None:
    assert_type(metadata.partition, int)
    assert_type(metadata.topic_partition.topic, str)
