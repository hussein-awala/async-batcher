# Kafka producer batches


Install the `kafka` extra for Async Batcher 0.3.0; see [installation](installation.md)
for Python requirements and release availability:

```bash
python -m pip install 'async-batcher[kafka]==0.3.0'
```
Use `AsyncKafkaBatcher` with a caller-owned aiokafka producer.
The adapter collects application requests into [native Kafka batches](https://aiokafka.readthedocs.io/en/stable/producer.html#direct-batch-control)
and groups them by an explicit topic and partition. The caller supplies and starts the producer:

```python
import asyncio

from aiokafka import AIOKafkaProducer
from async_batcher.kafka import AsyncKafkaBatcher, KafkaRecord


async def publish():
    producer = AIOKafkaProducer(bootstrap_servers="localhost:9092", enable_idempotence=True)
    await producer.start()
    batcher = AsyncKafkaBatcher(producer=producer, max_batch_size=100, max_queue_time=0.01)
    try:
        results = await asyncio.gather(
            *(
                batcher.process(KafkaRecord(topic="events", partition=0, value=value))
                for value in (b"first", b"second")
            )
        )
        print([(result.partition, result.offset) for result in results])
    finally:
        try:
            await batcher.stop()
        finally:
            await producer.stop()


asyncio.run(publish())
```

`KafkaRecord` accepts `topic`, `partition`, `key`, `value`, `timestamp_ms` (epoch milliseconds),
and `headers` (string/bytes-or-None pairs). A keyed `value=None` is a tombstone. Choose partitions
in your application; the adapter does not invoke the producer's partitioner. Key/value objects use
the supplied producer's serializers, which must be deterministic: a record can be serialized again
when it fills one native batch and is retried in the next.

The batcher's `max_batch_size` counts records. The producer's `max_batch_size` limits native batch
bytes; full builders are sent before remaining records are appended to a new builder. aiokafka can
accept an oversized first record into an otherwise empty builder. That record is sent alone, and a
broker size rejection fails that record without discarding successful neighboring batches.
`max_queue_size` and `max_queue_time` have the usual AsyncBatcher meanings. Keep `concurrency=1`
(the default) to retain per-partition order across separate application batches.

Each request returns its own aiokafka `RecordMetadata`, including its offset and timestamp.
CreateTime uses the individual record's timestamp; LogAppendTime uses the broker's timestamp.
Unknown offsets/timestamps remain unknown. With `acks=0`, the result is `None` and delivery is
unconfirmed. Validation, serialization, enqueue, and broker failures affect their corresponding
records; `asyncio.gather(..., return_exceptions=True)` lets callers inspect partial successes.
The adapter does not retry whole application batches or roll back successful records.

The caller owns producer start/stop, retries, acknowledgements, serializers, and transactions.
Stop the batcher before stopping the producer. Cancellation or `stop(force=True)` cannot retract
records already accepted by Kafka; pending producer deliveries continue and may succeed after the
caller is cancelled. Avoid blind resubmission when the delivery outcome is unknown.

See [core lifecycle](api.md) and [live test setup](../CONTRIBUTING.md#integration-tests).
