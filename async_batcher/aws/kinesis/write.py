from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from async_batcher.aws._records import send_records, validate_options
from async_batcher.batcher import AsyncBatcher

if TYPE_CHECKING:
    from aiobotocore.client import AioBaseClient

_MAX_BYTES = 10 * 1024 * 1024


@dataclass(frozen=True, kw_only=True)
class KinesisRecord:
    data: bytes
    partition_key: str
    explicit_hash_key: str | None = None


class AsyncKinesisBatcher(AsyncBatcher[KinesisRecord, dict[str, Any]]):
    """Send records with a caller-owned aiobotocore/aioboto3 Kinesis client.

    Supply exactly one of ``stream_name`` or ``stream_arn``. Each caller receives
    its service acknowledgement (ShardId and SequenceNumber), or its own error.
    Records are split at 500 records / 10 MiB including UTF-8 partition keys.
    ``max_retries`` (default 5) retries only per-record throttling/internal errors;
    SDK request-level retries remain governed by the client's configuration.
    PutRecords does not guarantee ordering or exactly-once delivery.
    Stop this batcher before closing the client; stop() never closes the client.
    """

    def __init__(
        self,
        client: AioBaseClient,
        *,
        stream_name: str | None = None,
        stream_arn: str | None = None,
        max_batch_size: int = 500,
        max_retries: int = 5,
        **kwargs: Any,
    ):
        super().__init__(max_batch_size=max_batch_size, **kwargs)
        validate_options(max_batch_size, max_retries)
        if (stream_name is None) == (stream_arn is None):
            raise ValueError("provide exactly one of stream_name or stream_arn")
        value = stream_name if stream_name is not None else stream_arn
        if not isinstance(value, str) or not value:
            raise ValueError("stream name or ARN must be a nonempty string")
        self.client = client
        self.stream = {"StreamName" if stream_name is not None else "StreamARN": value}
        self.max_retries = max_retries

    async def process_batch(self, batch: list[KinesisRecord]) -> list[dict[str, Any] | Exception]:
        records = []
        sizes = []
        for item in batch:
            if not isinstance(item.data, bytes):
                raise ValueError("Kinesis record data must be bytes")
            if not isinstance(item.partition_key, str) or not 1 <= len(item.partition_key) <= 256:
                raise ValueError("partition_key must be a string containing 1 to 256 characters")
            size = len(item.data) + len(item.partition_key.encode("utf-8"))
            if size > _MAX_BYTES:
                raise ValueError("Kinesis record data and partition key must not exceed 10 MiB")
            record = {"Data": item.data, "PartitionKey": item.partition_key}
            if item.explicit_hash_key is not None:
                key = item.explicit_hash_key
                if (
                    not isinstance(key, str)
                    or not key.isascii()
                    or not key.isdecimal()
                    or len(key) > 39
                    or str(int(key)) != key
                    or int(key) >= 2**128
                ):
                    raise ValueError("explicit_hash_key must be a decimal integer from 0 to 2**128 - 1")
                record["ExplicitHashKey"] = key
            records.append(record)
            sizes.append(size)
        if not records:
            return []
        return await send_records(
            self.client.put_records,
            records,
            sizes,
            max_bytes=_MAX_BYTES,
            max_retries=self.max_retries,
            response_key="Records",
            success_keys=("ShardId", "SequenceNumber"),
            retry_codes={"ProvisionedThroughputExceededException", "InternalFailure"},
            **self.stream,
        )
