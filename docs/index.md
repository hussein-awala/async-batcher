# Async Batcher

Combine concurrent Python requests into bounded batches and route each result back to its caller.
Wrap an existing bulk function, subclass `AsyncBatcher`, or use an adapter for database writes,
messaging, and model inference. The core has no runtime dependencies and requires Python 3.11+.

## Start with your workload

| You need to… | Start here |
| --- | --- |
| Batch predictions or writes in a web service | [Bounded service configuration](starting-configurations.md#interactive-service-reject-overload-explicitly), then [FastAPI](fastapi-recipe.md) |
| Import a file or consume an async stream | [Bounded maps](bulk-processing.md) and [import/recovery choices](cookbook.md#choose-an-import-or-recovery-policy) |
| Use an existing database or messaging client | [Database adapters](databases.md), [DynamoDB](dynamodb.md), [AWS streams](aws-streams.md), or [Kafka](kafka.md) |
| Send embeddings to a vector store | [Embedding pipeline](cohere-pinecone.md) and [durable handoff](durable-embeddings.md) |
| Separate tenants, model revisions, or destinations | [Compatible request groups](grouped-batching.md) |
| Limit bytes, tokens, or estimated work | [Cost budgets](cost-aware-batching.md) and [variable-length inference](variable-length-inference.md) |

Batching needs a useful bulk operation. It does not add durable delivery or exactly-once writes.
The [workload guide](workloads.md) explains when batching helps and how to choose limits.

## Install

These examples target **Async Batcher 0.3.0** and require Python 3.11 or newer.
The shared [release notes](https://hussein.awala.fr/async-batcher/release-notes/)
show which versions are published. Install into your application's environment:

```bash
python -m pip install 'async-batcher==0.3.0'
```

The core needs no optional clients. Use [installation](installation.md) to select
extras or add the package with uv, and read [migration from 0.2.2](migration.md)
when upgrading. Repository recipes have their own [setup](installation.md#run-the-repository-recipes).

## Your first batch

Save this as `demo.py` and run `python demo.py` in your package environment:

```python
import asyncio

from async_batcher import AsyncBatcher


async def double_many(values: list[int]) -> list[int]:
    return [value * 2 for value in values]


async def main():
    async with AsyncBatcher.from_callable(
        double_many, max_batch_size=5, max_queue_size=20, max_queue_time=0.01, concurrency=2
    ) as batcher:
        results = await asyncio.gather(*(batcher.process(i) for i in range(10)))
        print(results)  # [0, 2, 4, 6, 8, 10, 12, 14, 16, 18]


asyncio.run(main())
```

Submit `process()` calls concurrently to form batches. Awaiting each request before submitting
the next normally produces single-item batches. For large inputs, use [bounded maps](bulk-processing.md)
instead of creating a task for every item. The async context manager drains accepted work before
exiting; keep clients and executors alive until that cleanup finishes.

Continue with [functions and cleanup](functions.md), the [two bounded configurations](starting-configurations.md),
or the [API and lifecycle reference](api.md).
