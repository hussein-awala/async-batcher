from __future__ import annotations

import asyncio
from collections import defaultdict
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast

from aiokafka.errors import MessageSizeTooLargeError

from async_batcher.batcher import AsyncBatcher

if TYPE_CHECKING:
    from aiokafka import AIOKafkaProducer
    from aiokafka.producer.message_accumulator import BatchBuilder
    from aiokafka.structs import RecordMetadata


@dataclass(frozen=True, slots=True, kw_only=True)
class KafkaRecord:
    """A record routed to an explicit Kafka topic and partition.

    Key and value use the supplied producer's serializers. Timestamps are epoch
    milliseconds. Headers are pairs of strings and bytes (or None).
    """

    topic: str
    partition: int
    value: Any = None
    key: Any = None
    timestamp_ms: int | None = None
    headers: tuple[tuple[str, bytes | None], ...] = ()


_UNSET = object()


def _observe_delivery(future: asyncio.Future) -> None:
    # A forced stop may abandon an already-enqueued send. Observe late failures
    # without cancelling the caller-owned producer's delivery future.
    if not future.cancelled():
        future.exception()


class AsyncKafkaBatcher(AsyncBatcher[KafkaRecord, "RecordMetadata | None"]):
    """Collect explicit native Kafka batches using a caller-owned, started producer.

    Records are grouped by topic/partition and split at the producer builder's
    byte limit. The AsyncBatcher max_batch_size is a record count, not a byte
    limit. Producer retries, acknowledgements, serializers, and transactions stay
    under caller control; this adapter never starts, stops, or flushes the producer.
    Serializers must be deterministic: a record is serialized again when retried
    after filling a native batch.

    Results are per-record RecordMetadata, or None when acks=0. A failed native
    batch affects its records only. Cancellation cannot undo accepted Kafka sends.
    Keep concurrency=1 when ordering across separate AsyncBatcher batches matters.
    """

    def __init__(self, *, producer: AIOKafkaProducer, **kwargs):
        super().__init__(**kwargs)
        self.producer = producer

    @staticmethod
    def _validate(record: KafkaRecord) -> None:
        if not isinstance(record, KafkaRecord):
            raise TypeError("Expected a KafkaRecord")
        if not isinstance(record.topic, str) or not record.topic:
            raise ValueError("topic must be a nonempty string")
        if (
            isinstance(record.partition, bool)
            or not isinstance(record.partition, int)
            or record.partition < 0
        ):
            raise ValueError("partition must be a nonnegative integer")
        if record.key is None and record.value is None:
            raise ValueError("At least one of key or value must be provided")
        if record.timestamp_ms is not None and (
            isinstance(record.timestamp_ms, bool) or not isinstance(record.timestamp_ms, int)
        ):
            raise TypeError("timestamp_ms must be an integer or None")
        for key, value in record.headers:
            if not isinstance(key, str) or (value is not None and not isinstance(value, bytes)):
                raise TypeError("headers must contain string keys and bytes or None values")

    async def process_batch(self, batch: list[KafkaRecord]) -> list[RecordMetadata | None | BaseException]:
        results: list[object] = [_UNSET] * len(batch)
        partitions = defaultdict(list)
        for index, record in enumerate(batch):
            try:
                self._validate(record)
            except Exception as error:
                results[index] = error
            else:
                partitions[record.topic, record.partition].append((index, record))
        await asyncio.gather(
            *(
                self._process_partition(topic, partition, records, results)
                for (topic, partition), records in partitions.items()
            )
        )
        if any(result is _UNSET for result in results):
            raise RuntimeError("A Kafka record did not receive a result")
        return cast("list[RecordMetadata | None | BaseException]", results)

    async def _process_partition(self, topic, partition, records, results):
        try:
            builder = self.producer.create_batch()
            pending = []
            for index, record in records:
                try:
                    metadata = builder.append(
                        timestamp=record.timestamp_ms,
                        key=record.key,
                        value=record.value,
                        headers=list(record.headers),
                    )
                except Exception as error:
                    results[index] = error
                    continue
                if metadata is None:
                    await self._deliver(builder, topic, partition, pending, results)
                    pending = []
                    builder = self.producer.create_batch()
                    try:
                        metadata = builder.append(
                            timestamp=record.timestamp_ms,
                            key=record.key,
                            value=record.value,
                            headers=list(record.headers),
                        )
                    except Exception as error:
                        results[index] = error
                        continue
                if metadata is None:
                    results[index] = MessageSizeTooLargeError("Record cannot fit in an empty Kafka batch")
                else:
                    pending.append((index, metadata))
            await self._deliver(builder, topic, partition, pending, results)
        except asyncio.CancelledError as error:
            if asyncio.current_task().cancelling():
                raise
            self._fail_remaining(records, results, error)
        except Exception as error:
            self._fail_remaining(records, results, error)

    @staticmethod
    def _fail_remaining(records, results, error):
        for index, _ in records:
            if results[index] is _UNSET:
                results[index] = error

    async def _deliver(self, builder: BatchBuilder, topic, partition, pending, results):
        if not pending:
            return
        builder.close()
        try:
            delivery = await self.producer.send_batch(builder, topic, partition=partition)
            delivery.add_done_callback(_observe_delivery)
            metadata = await asyncio.shield(delivery)
        except asyncio.CancelledError as error:
            if asyncio.current_task().cancelling():
                raise
            for index, _ in pending:
                results[index] = error
        except Exception as error:
            for index, _ in pending:
                results[index] = error
        else:
            for index, relative in pending:
                if metadata is None:
                    results[index] = None
                    continue
                # -1 denotes unavailable broker metadata (e.g. an old duplicate
                # sequence). Never turn it into an invented nonnegative offset.
                offset = metadata.offset + relative.offset if metadata.offset >= 0 else metadata.offset
                timestamp = metadata.timestamp
                if metadata.offset >= 0 and metadata.timestamp_type == 0:
                    timestamp = relative.timestamp
                results[index] = metadata._replace(offset=offset, timestamp=timestamp)
