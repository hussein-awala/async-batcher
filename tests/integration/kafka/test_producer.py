from __future__ import annotations

import asyncio
import time
import uuid
from contextlib import asynccontextmanager

import pytest

pytest.importorskip("aiokafka")

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import MessageSizeTooLargeError
from aiokafka.structs import RecordMetadata
from async_batcher.kafka import AsyncKafkaBatcher, KafkaRecord

pytestmark = [pytest.mark.asyncio, pytest.mark.integration, pytest.mark.integration_kafka]
BOOTSTRAP = "127.0.0.1:9092"


@asynccontextmanager
async def topic_with_config(**config):
    topic = f"async-batcher-{uuid.uuid4().hex}"
    admin = AIOKafkaAdminClient(bootstrap_servers=BOOTSTRAP, request_timeout_ms=10000)
    created = False
    try:
        await admin.start()
        response = await admin.create_topics(
            [NewTopic(topic, num_partitions=2, replication_factor=1, topic_configs=config)],
            timeout_ms=10000,
        )
        assert all(error[1] == 0 for error in response.topic_errors), response
        created = True
        yield topic
    finally:
        try:
            if created:
                await admin.delete_topics([topic], timeout_ms=10000)
        finally:
            await admin.close()


async def consume(topic, count, *, isolation_level="read_uncommitted"):
    consumer = AIOKafkaConsumer(
        bootstrap_servers=BOOTSTRAP,
        group_id=None,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        request_timeout_ms=10000,
        isolation_level=isolation_level,
    )
    try:
        await consumer.start()
        consumer.assign([TopicPartition(topic, partition) for partition in (0, 1)])
        await consumer.seek_to_beginning()
        records = []
        async with asyncio.timeout(30):
            while len(records) < count:
                fetched = await consumer.getmany(timeout_ms=1000)
                records.extend(
                    record for partition_records in fetched.values() for record in partition_records
                )
        assert len(records) == count
        return records
    finally:
        await consumer.stop()


@pytest.mark.parametrize("timestamp_type", ["CreateTime", "LogAppendTime"])
async def test_native_batches_deliver_exact_records_offsets_and_broker_timestamp_semantics(timestamp_type):
    async with topic_with_config(**{"message.timestamp.type": timestamp_type}) as topic:
        producer = AIOKafkaProducer(
            bootstrap_servers=BOOTSTRAP,
            enable_idempotence=True,
            max_batch_size=150,
            request_timeout_ms=10000,
        )
        batcher = AsyncKafkaBatcher(producer=producer, max_batch_size=12)
        now = int(time.time() * 1000)
        inputs = [
            KafkaRecord(
                topic=topic,
                partition=index % 2,
                key=f"key-{index}".encode(),
                value=f"value-{index}".encode(),
                timestamp_ms=now - 1000 + index,
                headers=(("trace", str(index).encode()),),
            )
            for index in range(12)
        ]
        try:
            await producer.start()
            async with asyncio.timeout(30):
                results = await asyncio.gather(*(batcher.process(record) for record in inputs))
            received = {(record.partition, record.offset): record for record in await consume(topic, 12)}
            for index, (record, result) in enumerate(zip(inputs, results, strict=True)):
                assert isinstance(result, RecordMetadata)
                assert (result.topic, result.partition, result.offset) == (topic, index % 2, index // 2)
                stored = received[result.partition, result.offset]
                assert (stored.key, stored.value, tuple(stored.headers)) == (
                    record.key,
                    record.value,
                    record.headers,
                )
                assert result.timestamp == stored.timestamp
                assert result.timestamp_type == stored.timestamp_type == (timestamp_type == "LogAppendTime")
                if timestamp_type == "CreateTime":
                    assert result.timestamp == record.timestamp_ms
                else:
                    assert result.timestamp >= now
        finally:
            await batcher.stop(force=True)
            await producer.stop()


async def test_broker_rejects_oversized_record_without_losing_neighboring_batches():
    async with topic_with_config(**{"max.message.bytes": "256"}) as topic:
        producer = AIOKafkaProducer(bootstrap_servers=BOOTSTRAP, max_batch_size=100, request_timeout_ms=10000)
        batcher = AsyncKafkaBatcher(producer=producer, max_batch_size=3)
        values = (b"first", b"x" * 1000, b"last")
        try:
            await producer.start()
            async with asyncio.timeout(30):
                results = await asyncio.gather(
                    *(
                        batcher.process(KafkaRecord(topic=topic, partition=0, value=value))
                        for value in values
                    ),
                    return_exceptions=True,
                )
            assert isinstance(results[0], RecordMetadata) and results[0].offset == 0
            assert isinstance(results[1], MessageSizeTooLargeError)
            assert isinstance(results[2], RecordMetadata) and results[2].offset == 1
            assert [record.value for record in await consume(topic, 2)] == [b"first", b"last"]
        finally:
            await batcher.stop(force=True)
            await producer.stop()


async def test_cross_topic_serializers_tombstones_headers_and_producer_ownership():
    def serialize(value):
        return None if value is None else value.encode()

    async with topic_with_config() as first, topic_with_config() as second:
        producer = AIOKafkaProducer(
            bootstrap_servers=BOOTSTRAP,
            key_serializer=serialize,
            value_serializer=serialize,
            request_timeout_ms=10000,
        )
        records = [
            KafkaRecord(topic=first, partition=0, key="key", value="", headers=(("trace", None),)),
            KafkaRecord(topic=second, partition=1, key="key", value="other"),
            KafkaRecord(topic=first, partition=0, key="key", value=None),
            KafkaRecord(topic=second, partition=0, value="last", headers=(("same", b"1"), ("same", b"2"))),
        ]
        batcher = AsyncKafkaBatcher(producer=producer, max_batch_size=len(records))
        try:
            await producer.start()
            async with asyncio.timeout(30):
                results = await asyncio.gather(*(batcher.process(record) for record in records))
                await batcher.stop(timeout=10)
                direct = await producer.send_and_wait(first, key="owner", value="still open", partition=1)
            received = {
                (row.topic, row.partition, row.offset): row
                for row in [*(await consume(first, 3)), *(await consume(second, 2))]
            }
            for record, result in zip(records, results, strict=True):
                assert isinstance(result, RecordMetadata)
                row = received[result.topic, result.partition, result.offset]
                assert (row.topic, row.partition) == (record.topic, record.partition)
                assert (row.key, row.value, tuple(row.headers)) == (
                    serialize(record.key),
                    serialize(record.value),
                    record.headers,
                )
            assert received[direct.topic, direct.partition, direct.offset].value == b"still open"
        finally:
            await batcher.stop(force=True, timeout=10)
            await producer.stop()


async def test_transaction_abort_hides_records_and_commit_exposes_them_to_read_committed_consumers():
    async with topic_with_config() as topic:
        producer = AIOKafkaProducer(
            bootstrap_servers=BOOTSTRAP,
            transactional_id=f"batcher-transaction-{uuid.uuid4().hex}",
            request_timeout_ms=10000,
        )
        batcher = AsyncKafkaBatcher(producer=producer, max_batch_size=2)
        try:
            async with asyncio.timeout(45):
                await producer.start()
                await producer.begin_transaction()
                aborted = await asyncio.gather(
                    *(
                        batcher.process(KafkaRecord(topic=topic, partition=0, value=value))
                        for value in (b"abort-1", b"abort-2")
                    )
                )
                assert all(isinstance(result, RecordMetadata) for result in aborted)
                await producer.abort_transaction()
                async with producer.transaction():
                    committed = await asyncio.gather(
                        *(
                            batcher.process(KafkaRecord(topic=topic, partition=0, value=value))
                            for value in (b"commit-1", b"commit-2")
                        )
                    )
                assert all(isinstance(result, RecordMetadata) for result in committed)
                await batcher.stop(timeout=10)
                # Transaction ownership remains with the caller after the batcher stops.
                async with producer.transaction():
                    await producer.send_and_wait(topic, b"owner-commit", partition=0)
            committed_records = await consume(topic, 3, isolation_level="read_committed")
            assert [record.value for record in committed_records] == [
                b"commit-1",
                b"commit-2",
                b"owner-commit",
            ]
            assert [record.offset for record in committed_records[:2]] == [
                result.offset for result in committed
            ]
            all_records = await consume(topic, 5)
            assert [record.value for record in all_records] == [
                b"abort-1",
                b"abort-2",
                b"commit-1",
                b"commit-2",
                b"owner-commit",
            ]
        finally:
            await batcher.stop(force=True, timeout=10)
            await producer.stop()
