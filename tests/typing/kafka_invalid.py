from __future__ import annotations

from typing import TYPE_CHECKING

from async_batcher.kafka import AsyncKafkaBatcher, KafkaRecord

if TYPE_CHECKING:
    from aiokafka import AIOKafkaProducer
    from async_batcher import BatchEvent


async def async_observer(event: BatchEvent) -> None:
    return None


def invalid_constructor(producer: AIOKafkaProducer) -> None:
    AsyncKafkaBatcher(producer=object())  # error: arg-type
    AsyncKafkaBatcher(producer=producer, max_batch_size="large")  # error: arg-type
    AsyncKafkaBatcher(producer=producer, max_queue_size=1.5)  # error: arg-type
    AsyncKafkaBatcher(producer=producer, item_cost=lambda record: "many")  # error: arg-type, return-value
    AsyncKafkaBatcher(producer=producer, on_batch=async_observer)  # error: arg-type
    AsyncKafkaBatcher(producer=producer, max_bacth_size=10)  # error: call-arg
    AsyncKafkaBatcher(producer=producer, log_exceptions="yes")  # error: arg-type
    AsyncKafkaBatcher(producer=producer, discard_cancelled="yes")  # error: arg-type
    KafkaRecord(topic="events", partition="zero", value=b"data")  # error: arg-type
    KafkaRecord(topic="events", partition=0, headers=(("key", "text"),))  # error: arg-type


async def invalid_requests(batcher: AsyncKafkaBatcher, record: KafkaRecord) -> None:
    await batcher.process(b"raw")  # error: arg-type
    batcher.map(["raw"])  # error: list-item
    result = await batcher.process(record)
    _offset = result.offset  # error: union-attr
    if result is not None:
        _bad_offset: str = result.offset  # error: assignment
