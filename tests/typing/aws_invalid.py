from __future__ import annotations

from typing import TYPE_CHECKING

from async_batcher.aws.kinesis import AsyncKinesisBatcher, KinesisRecord

if TYPE_CHECKING:
    from aiobotocore.client import AioBaseClient
    from async_batcher import BatchEvent


async def async_observer(event: BatchEvent) -> None:
    return None


def wrong_cost(record: KinesisRecord) -> str:
    return "big"


def invalid_constructor(client: AioBaseClient) -> None:
    AsyncKinesisBatcher(object(), stream_name="events")  # error: arg-type
    AsyncKinesisBatcher(client, stream_name=1)  # error: arg-type
    AsyncKinesisBatcher(client, stream_name="events", max_batch_size="many")  # error: arg-type
    AsyncKinesisBatcher(client, stream_name="events", max_retries="many")  # error: arg-type
    AsyncKinesisBatcher(client, stream_name="events", max_queue_time="soon")  # error: arg-type
    AsyncKinesisBatcher(client, stream_name="events", on_batch=async_observer)  # error: arg-type
    AsyncKinesisBatcher(client, stream_name="events", item_cost=wrong_cost)  # error: arg-type
    AsyncKinesisBatcher(client, stream_name="events", typo=True)  # error: call-arg
    AsyncKinesisBatcher(client, stream_name="events", log_exceptions="yes")  # error: arg-type
    AsyncKinesisBatcher(client, stream_name="events", discard_cancelled="yes")  # error: arg-type
    AsyncKinesisBatcher(client, stream_name="events", retry_jitter="yes")  # error: arg-type
    KinesisRecord(data="text", partition_key="key")  # error: arg-type
    KinesisRecord(data=b"data", partition_key=1)  # error: arg-type


async def invalid_requests(batcher: AsyncKinesisBatcher, record: KinesisRecord) -> None:
    await batcher.process(b"raw")  # error: arg-type
    batcher.map(["raw"])  # error: list-item
    _result: int = await batcher.process(record)  # error: assignment
    record.data = b"changed"  # error: misc
