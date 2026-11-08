# Kinesis and Firehose

Install the `aws` extra for Async Batcher 0.3.0; see [installation](installation.md)
for Python requirements and release availability:

```bash
python -m pip install 'async-batcher[aws]==0.3.0'
```
These adapters accept an asynchronous client from
aioboto3 or aiobotocore. You own the client: keep it open until the batcher has
stopped. One batcher targets one stream.

```python
import asyncio
import aioboto3
from async_batcher.aws.kinesis import AsyncKinesisBatcher, KinesisRecord


async def main():
    async with aioboto3.Session().client("kinesis", region_name="us-east-1") as client:
        batcher = AsyncKinesisBatcher(client, stream_name="events", max_batch_size=100)
        try:
            results = await asyncio.gather(
                batcher.process(KinesisRecord(data=b"first", partition_key="customer-1")),
                batcher.process(KinesisRecord(data=b"second", partition_key="customer-2")),
            )
            print(results)  # Each result has ShardId and SequenceNumber.
        finally:
            await batcher.stop()


asyncio.run(main())
```

For Kinesis, provide exactly one of `stream_name` or `stream_arn`.
`KinesisRecord` accepts bytes, a partition key of 1–256 characters, and an optional
`explicit_hash_key` containing a decimal integer from 0 through `2**128 - 1`.
Requests split at 500 records or 10 MiB, including UTF-8 partition keys. A single
record exceeding that limit is rejected before any record in its batch is sent.
A stream's configured maximum record size can be lower than the API maximum;
the service enforces that configuration.

For Firehose, create the client with `session.client("firehose", ...)` and use:

```python
from async_batcher.aws.firehose import AsyncFirehoseBatcher


async def put_event(client):
    batcher = AsyncFirehoseBatcher(client, delivery_stream_name="events")
    try:
        result = await batcher.process(b'{"event": "opened"}\n')
        print(result["RecordId"])
    finally:
        await batcher.stop()
```

Firehose accepts bytes up to 1,000 KiB per record before base64 encoding. Requests
split at 500 records or 4 MiB. Include any delimiters needed by your destination
in the bytes; the batcher does not add them. A RecordId confirms acceptance by
Firehose, not delivery to the destination.

Both batchers accept the [core options](api.md#configuration), including `max_queue_time`, `concurrency`,
and `max_queue_size`. `max_batch_size` defaults to 500 and must be 1–500.
The deprecated `batch_size` alias still warns and overrides `max_batch_size`; the same 1–500
limit applies to its effective value.
`max_retries` defaults to 5 retries after the initial request; zero disables
record retries. Exponential backoff starts at 0.1 seconds and caps at 5 seconds.
Set `retry_jitter=True` to choose each delay uniformly between zero and that backoff
limit, spreading concurrent clients' retry attempts. It defaults to `False`, preserving
the deterministic delays. Jitter changes only the waiting interval; retry counts, eligible
error codes, and cancellation behavior are unchanged. This follows AWS's
[backoff and jitter guidance](https://aws.amazon.com/builders-library/timeouts-retries-and-backoff-with-jitter/).
Only explicitly rejected records with retryable service error codes are retried.
Successful records are never deliberately resent by the batcher. Exhausted or
nonretryable record errors raise `RecordBatchError` for that caller, with `code`,
`message`, and `attempts` attributes. An SDK request exception affects only the
records still awaiting a result from that request; earlier acknowledgements are
preserved. SDK request retries follow the client's configuration.

Kinesis PutRecords does not guarantee record order. Neither service guarantees
exactly-once delivery: SDK retries, interrupted requests, or cancellation can
leave an ambiguous write outcome. Include an application identifier when
consumers need to recognize duplicates. Canceling a waiting caller does not
retract an accepted record.

The AWS adapter suite tests the real SDK request schema, positional response
mapping, partial retries, size splitting, and localhost HTTP serialization.
It does not contact paid AWS services.

API references: [Kinesis PutRecords](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_PutRecords.html)
and [Firehose PutRecordBatch](https://docs.aws.amazon.com/firehose/latest/APIReference/API_PutRecordBatch.html).
