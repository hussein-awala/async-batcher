from __future__ import annotations

from typing import TYPE_CHECKING, Any

from async_batcher.aws._records import send_records, validate_options
from async_batcher.batcher import AsyncBatcher

if TYPE_CHECKING:
    from aiobotocore.client import AioBaseClient


class AsyncFirehoseBatcher(AsyncBatcher[bytes, dict[str, Any]]):
    """Send bytes with a caller-owned aiobotocore/aioboto3 Firehose client.

    Each caller receives its RecordId acknowledgement or its own error. Requests
    split at 500 records / 4 MiB; each record must be at most 1,000 KiB before
    base64 encoding. Add any destination delimiters to your bytes yourself.
    ``max_retries`` (default 5) retries only per-record unavailable/internal errors;
    SDK request-level retries remain governed by the client's configuration.
    Delivery may contain duplicates. Acknowledgement means Firehose accepted the
    record, not that its destination has received it. Stop this batcher before
    closing the client; stop() never closes the client.
    """

    def __init__(
        self,
        client: AioBaseClient,
        *,
        delivery_stream_name: str,
        max_batch_size: int = 500,
        max_retries: int = 5,
        **kwargs: Any,
    ):
        super().__init__(max_batch_size=max_batch_size, **kwargs)
        validate_options(max_batch_size, max_retries)
        if not isinstance(delivery_stream_name, str) or not delivery_stream_name:
            raise ValueError("delivery_stream_name must be a nonempty string")
        self.client = client
        self.delivery_stream_name = delivery_stream_name
        self.max_retries = max_retries

    async def process_batch(self, batch: list[bytes]) -> list[dict[str, Any] | Exception]:
        for data in batch:
            if not isinstance(data, bytes) or len(data) > 1000 * 1024:
                raise ValueError("Firehose record data must be bytes of at most 1,000 KiB")
        if not batch:
            return []
        return await send_records(
            self.client.put_record_batch,
            [{"Data": data} for data in batch],
            [len(data) for data in batch],
            max_bytes=4 * 1024 * 1024,
            max_retries=self.max_retries,
            response_key="RequestResponses",
            success_keys=("RecordId",),
            retry_codes={"ServiceUnavailableException", "InternalFailure"},
            DeliveryStreamName=self.delivery_stream_name,
        )
