from __future__ import annotations

import asyncio
from collections import defaultdict
from dataclasses import replace
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio

pytest.importorskip("aiokafka")

from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaTimeoutError, MessageSizeTooLargeError
from aiokafka.metrics import ProducerMetricsCollector
from aiokafka.producer.message_accumulator import MessageBatch
from aiokafka.protocol.metadata import MetadataResponse_v0
from aiokafka.record.memory_records import MemoryRecords
from aiokafka.structs import RecordMetadata
from async_batcher.kafka import AsyncKafkaBatcher, KafkaRecord

from tests.core_helpers import wait_until


@pytest_asyncio.fixture
async def producer_factory(monkeypatch):
    producers = []

    def make(*, complete=None, enqueue_error=None, **kwargs):
        producer = AIOKafkaProducer(**kwargs)
        producers.append(producer)
        # Supply an actual SDK metadata response; no network or fake SDK types.
        producer._metadata.update_metadata(
            MetadataResponse_v0(
                [(0, "localhost", 9092)],
                [
                    (0, topic, [(0, partition, 0, [0], [0]) for partition in (0, 1)])
                    for topic in ("events", "other")
                ],
            )
        )
        deliveries = []
        offsets = defaultdict(int)

        async def add_batch(builder, topic_partition, timeout):
            assert builder.closed(), "Adapter must close native batches before sending"
            if enqueue_error is not None:
                error = enqueue_error(topic_partition)
                if error is not None:
                    raise error
            message = MessageBatch(topic_partition, builder, 30, 0, ProducerMetricsCollector())
            message.drain_ready()
            deliveries.append(message)
            if complete is None:
                message.done(offsets[topic_partition], timestamp=-1, log_start_offset=0)
                offsets[topic_partition] += message.record_count
            else:
                complete(message)
            return asyncio.shield(message.future)

        monkeypatch.setattr(producer._message_accumulator, "add_batch", add_batch)
        return producer, deliveries

    yield make
    for producer in producers:
        await producer.stop()


def decoded(message):
    records = MemoryRecords(bytes(message.get_data_buffer()))
    return list(records.next_batch())


@pytest.mark.asyncio
async def test_native_batches_route_records_and_preserve_serializers_headers_and_timestamps(producer_factory):
    producer, deliveries = producer_factory(
        key_serializer=lambda value: None if value is None else value.encode(),
        value_serializer=lambda value: None if value is None else value.encode(),
    )
    batcher = AsyncKafkaBatcher(producer=producer)
    records = [
        KafkaRecord(
            topic="events",
            partition=1,
            key="a",
            value="first",
            timestamp_ms=1000,
            headers=(("trace", b"one"), ("nullable", None)),
        ),
        KafkaRecord(topic="events", partition=0, value="second", timestamp_ms=2000),
        KafkaRecord(topic="other", partition=1, value="third", timestamp_ms=3000),
        KafkaRecord(topic="events", partition=1, key="a", value=None, timestamp_ms=4000),
    ]
    results = await batcher.process_batch(records)
    assert all(isinstance(result, RecordMetadata) for result in results)
    assert [(r.topic, r.partition, r.offset, r.timestamp, r.timestamp_type) for r in results] == [
        ("events", 1, 0, 1000, 0),
        ("events", 0, 0, 2000, 0),
        ("other", 1, 0, 3000, 0),
        ("events", 1, 1, 4000, 0),
    ]
    assert len(deliveries) == 3
    first, tombstone = decoded(deliveries[0])
    assert (first.key, first.value, first.headers, first.timestamp) == (
        b"a",
        b"first",
        [("trace", b"one"), ("nullable", None)],
        1000,
    )
    assert tombstone.key == b"a" and tombstone.value is None


@pytest.mark.asyncio
@pytest.mark.parametrize("timestamp, offset, expected", [(9000, 10, [9000, 9000]), (-1, -1, [-1, -1])])
async def test_log_append_time_and_unknown_offsets_are_not_fabricated(
    producer_factory, timestamp, offset, expected
):
    producer, _ = producer_factory(complete=lambda message: message.done(offset, timestamp=timestamp))
    batcher = AsyncKafkaBatcher(producer=producer)
    results = await batcher.process_batch(
        [
            KafkaRecord(topic="events", partition=0, value=b"a", timestamp_ms=1000),
            KafkaRecord(topic="events", partition=0, value=b"b", timestamp_ms=2000),
        ]
    )
    assert [r.timestamp for r in results] == expected
    assert [r.offset for r in results] == ([10, 11] if offset >= 0 else [-1, -1])


@pytest.mark.asyncio
async def test_full_native_builders_split_and_keep_oversized_failure_isolated(producer_factory):
    failure = MessageSizeTooLargeError("broker limit")
    offsets = defaultdict(int)

    def complete(message):
        if message._builder.size() > 120:
            message.failure(failure)
        else:
            message.done(offsets[message.tp], timestamp=-1)
            offsets[message.tp] += message.record_count

    producer, deliveries = producer_factory(max_batch_size=90, complete=complete)
    records = [
        KafkaRecord(topic="events", partition=0, value=value) for value in (b"a" * 15, b"b" * 200, b"c" * 15)
    ]
    results = await AsyncKafkaBatcher(producer=producer).process_batch(records)
    assert results[0].offset == 0
    assert results[1] is failure
    assert results[2].offset == 1
    assert [m.record_count for m in deliveries] == [1, 1, 1]
    assert [decoded(m)[0].value for m in deliveries] == [r.value for r in records]


@pytest.mark.asyncio
async def test_invalid_records_and_serializer_failures_do_not_discard_valid_peers(producer_factory):
    def serialize(value):
        if value == "bad":
            raise ValueError("serializer failed")
        return value.encode()

    producer, deliveries = producer_factory(value_serializer=serialize)
    valid = KafkaRecord(topic="events", partition=0, value="good")
    records = [
        valid,
        replace(valid, value="bad"),
        replace(valid, partition=-1),
        replace(valid, headers=(("x", "not bytes"),)),
        replace(valid, value=None),
        object(),
        valid,
    ]
    results = await AsyncKafkaBatcher(producer=producer).process_batch(records)
    assert results[0].offset == 0 and results[-1].offset == 1
    assert all(isinstance(result, Exception) for result in results[1:-1])
    assert [r.value for r in decoded(deliveries[0])] == [b"good", b"good"]


@pytest.mark.asyncio
@pytest.mark.parametrize("enqueue_failure", [False, True])
async def test_partition_failures_preserve_other_partition_successes(producer_factory, enqueue_failure):
    failure = KafkaTimeoutError("partition failed")

    def complete(message):
        if message.tp.partition == 1:
            message.failure(failure)
        else:
            message.done(30, timestamp=-1)

    producer, _ = producer_factory(
        complete=None if enqueue_failure else complete,
        enqueue_error=(lambda tp: failure if tp.partition == 1 else None) if enqueue_failure else None,
    )
    results = await AsyncKafkaBatcher(producer=producer).process_batch(
        [
            KafkaRecord(topic="events", partition=1, value=b"bad"),
            KafkaRecord(topic="events", partition=0, value=b"good"),
            KafkaRecord(topic="events", partition=1, value=b"also bad"),
        ]
    )
    assert results[0] is failure and results[2] is failure
    assert isinstance(results[1], RecordMetadata)


@pytest.mark.asyncio
async def test_no_ack_and_empty_batches(producer_factory):
    producer, deliveries = producer_factory(acks=0, complete=lambda message: message.done_noack())
    batcher = AsyncKafkaBatcher(producer=producer)
    assert await batcher.process_batch([]) == []
    assert not deliveries
    assert await batcher.process_batch([KafkaRecord(topic="events", partition=0, value=b"x")]) == [None]


@pytest.mark.asyncio
async def test_force_stop_leaves_producer_owned_delivery_alive_and_observes_late_failure(
    producer_factory, harness
):
    producer, deliveries = producer_factory(complete=lambda message: None)
    producer.start = AsyncMock()
    original_stop = producer.stop
    producer.stop = AsyncMock(wraps=original_stop)
    errors = []
    loop = asyncio.get_running_loop()
    previous_handler = loop.get_exception_handler()
    loop.set_exception_handler(lambda loop, context: errors.append(context))
    try:
        batcher = harness.create(AsyncKafkaBatcher, producer=producer, max_batch_size=1)
        request = harness.spawn(batcher.process(KafkaRecord(topic="events", partition=0, value=b"x")))
        await wait_until(lambda: len(deliveries) == 1)
        await batcher.stop(force=True)
        with pytest.raises(asyncio.CancelledError):
            await request
        assert not deliveries[0].future.cancelled()
        deliveries[0].failure(KafkaTimeoutError("late delivery failure"))
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert not errors
        producer.start.assert_not_called()
        producer.stop.assert_not_called()
    finally:
        loop.set_exception_handler(previous_handler)


@pytest.mark.asyncio
async def test_cancelled_caller_does_not_cancel_native_delivery_or_peer(producer_factory, harness):
    producer, deliveries = producer_factory(complete=lambda message: None)
    batcher = harness.create(AsyncKafkaBatcher, producer=producer, max_batch_size=2)
    first, peer = [
        harness.spawn(batcher.process(KafkaRecord(topic="events", partition=0, value=value)))
        for value in (b"a", b"b")
    ]
    await wait_until(lambda: len(deliveries) == 1)
    first.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first
    assert not deliveries[0].future.cancelled()
    deliveries[0].done(42, timestamp=-1)
    assert (await asyncio.wait_for(peer, timeout=1)).offset == 43


@pytest.mark.asyncio
async def test_full_builder_retries_deterministic_serializer_without_duplicate_records(producer_factory):
    calls = []

    def serialize(value):
        calls.append(value)
        return value.encode()

    producer, deliveries = producer_factory(max_batch_size=90, value_serializer=serialize)
    values = ["a" * 15, "b" * 15]
    results = await AsyncKafkaBatcher(producer=producer).process_batch(
        [KafkaRecord(topic="events", partition=0, value=value) for value in values]
    )
    assert [result.offset for result in results] == [0, 1]
    assert calls == [values[0], values[1], values[1]]
    assert [record.value for message in deliveries for record in decoded(message)] == [
        value.encode() for value in values
    ]


@pytest.mark.asyncio
async def test_independently_cancelled_delivery_cancels_only_its_partition(producer_factory, harness):
    def complete(message):
        if message.tp.partition == 1:
            message.future.cancel()
        else:
            message.done(50, timestamp=-1)

    producer, _ = producer_factory(complete=complete)
    batcher = harness.create(AsyncKafkaBatcher, producer=producer, max_batch_size=2)
    results = await asyncio.gather(
        *(
            batcher.process(KafkaRecord(topic="events", partition=partition, value=b"x"))
            for partition in (1, 0)
        ),
        return_exceptions=True,
    )
    assert isinstance(results[0], asyncio.CancelledError)
    assert results[1].offset == 50


@pytest.mark.asyncio
async def test_force_stop_cancels_blocked_native_enqueue(producer_factory, harness, monkeypatch):
    producer, deliveries = producer_factory()
    entered = asyncio.Event()
    cancelled = asyncio.Event()

    async def blocked_enqueue(*args, **kwargs):
        entered.set()
        try:
            await asyncio.Future()
        finally:
            cancelled.set()

    monkeypatch.setattr(producer._message_accumulator, "add_batch", blocked_enqueue)
    batcher = harness.create(AsyncKafkaBatcher, producer=producer, max_batch_size=1)
    request = harness.spawn(batcher.process(KafkaRecord(topic="events", partition=0, value=b"x")))
    await asyncio.wait_for(entered.wait(), 1)
    await asyncio.wait_for(batcher.stop(force=True), 1)
    assert cancelled.is_set()
    assert not deliveries
    with pytest.raises(asyncio.CancelledError):
        await request


@pytest.mark.asyncio
async def test_new_builder_failure_preserves_previously_acknowledged_batch(producer_factory, monkeypatch):
    producer, deliveries = producer_factory(max_batch_size=90)
    first_builder = producer.create_batch()
    failure = RuntimeError("cannot create next builder")
    builders = iter([first_builder])

    def create_batch():
        try:
            return next(builders)
        except StopIteration:
            raise failure from None

    monkeypatch.setattr(producer, "create_batch", create_batch)
    results = await AsyncKafkaBatcher(producer=producer).process_batch(
        [KafkaRecord(topic="events", partition=0, value=value) for value in (b"a" * 15, b"b" * 15, b"c" * 15)]
    )
    assert results[0].offset == 0
    assert results[1] is failure and results[2] is failure
    assert len(deliveries) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fields, error_type, message",
    [
        ({"topic": ""}, ValueError, "topic must be a nonempty string"),
        ({"topic": None}, ValueError, "topic must be a nonempty string"),
        ({"partition": True}, ValueError, "partition must be a nonnegative integer"),
        ({"partition": 0.5}, ValueError, "partition must be a nonnegative integer"),
        ({"timestamp_ms": True}, TypeError, "timestamp_ms must be an integer or None"),
        ({"timestamp_ms": 1.5}, TypeError, "timestamp_ms must be an integer or None"),
        ({"timestamp_ms": "1000"}, TypeError, "timestamp_ms must be an integer or None"),
        ({"headers": ((b"trace", b"value"),)}, TypeError, "headers must contain string keys"),
    ],
)
async def test_invalid_routing_and_metadata_do_not_reach_native_batches(
    producer_factory, fields, error_type, message
):
    producer, deliveries = producer_factory()
    valid = KafkaRecord(topic="events", partition=0, value=b"valid", timestamp_ms=1000)
    results = await AsyncKafkaBatcher(producer=producer).process_batch([replace(valid, **fields), valid])
    assert isinstance(results[0], error_type)
    assert message in str(results[0])
    assert results[1].offset == 0
    assert len(deliveries) == 1
    assert [record.value for record in decoded(deliveries[0])] == [b"valid"]


@pytest.mark.asyncio
async def test_all_serializer_failures_do_not_enqueue_empty_native_batches(producer_factory):
    def serialize(value):
        raise ValueError(f"cannot serialize {value}")

    producer, deliveries = producer_factory(value_serializer=serialize)
    values = ("first", "second", "third")
    results = await AsyncKafkaBatcher(producer=producer).process_batch(
        [KafkaRecord(topic="events", partition=index % 2, value=value) for index, value in enumerate(values)]
    )
    assert all(isinstance(result, ValueError) for result in results)
    assert [str(result) for result in results] == [f"cannot serialize {value}" for value in values]
    assert not deliveries
