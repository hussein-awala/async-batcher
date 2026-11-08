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

## Install the matching source

Check the version banner and selector before following an example. **Development documentation
contains unreleased features.** The [migration guide](migration.md) compares the published package
with the current source and explains how to pin a reviewed revision. For an older published release,
use its [release notes and source documentation](https://github.com/hussein-awala/async-batcher/releases).

To use the matching source with [uv](https://docs.astral.sh/uv/getting-started/installation/):

```bash
git clone https://github.com/hussein-awala/async-batcher.git
cd async-batcher
git checkout --detach main
uv sync --locked --no-default-groups --python 3.12
```

The published site pins this checkout to the source revision in its banner. When reading the
Markdown directly on GitHub, replace `main` with the release tag or reviewed commit you need.

Run Python with `uv run --no-sync python`. Install only the optional extras your backend needs,
as shown in its integration guide. The [contributor guide](../CONTRIBUTING.md) covers test tools.

## Your first batch

Save this as `demo.py` in the checkout and run `uv run --no-sync python demo.py`:

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
