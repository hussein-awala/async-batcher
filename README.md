# AsyncBatcher

AsyncBatcher combines concurrent Python requests into batches and routes each result back to its caller.
Wrap an existing bulk function, subclass `AsyncBatcher`, or use an adapter for a supported database,
messaging client, or prediction model. The core package has no runtime dependencies.

## Start with your workload

| You need to… | Start here |
| --- | --- |
| Combine concurrent web requests into model predictions or writes | [Function quickstart](#quickstart), then the [FastAPI lifespan recipe](docs/fastapi-recipe.md) |
| Import a large file or consume an async stream without keeping all results | [Bounded maps](docs/bulk-processing.md) and [SQLite imports](docs/cookbook.md#import-a-file-or-async-stream-into-sqlite) |
| Use an existing database, messaging, or prediction client | Choose its [adapter](#optional-adapters) and read its failure/ownership rules |
| Send embeddings to a vector store | [Two-stage pipeline](docs/cookbook.md#connect-embedding-and-vector-storage-stages) and [Pinecone](docs/pinecone.md) |
| Keep tenants, model revisions, or backend destinations separate | [Compatible request groups](docs/grouped-batching.md) and the [workload guide](docs/workloads.md) |
| Limit bytes, tokens, or estimated work as well as request count | [Cost budgets](docs/cost-aware-batching.md) |

The [workload guide](docs/workloads.md) helps choose limits, preserve routing metadata, and decide
whether batching fits your backend. Batching requires a useful bulk operation; it is not a rate
limiter, durable queue, or exactly-once delivery system.

## Install

**These docs describe the current repository checkout**, which requires Python 3.11 or newer.
New adapters and behavior on `main` may not yet be included in the published package.
For the latest published release, use `python -m pip install async-batcher` and consult the
[corresponding release](https://github.com/hussein-awala/async-batcher/releases).

To use the source documented here, install [uv](https://docs.astral.sh/uv/getting-started/installation/)
0.13.7 or newer and run:

```bash
git clone https://github.com/hussein-awala/async-batcher.git
cd async-batcher
uv sync --locked --no-default-groups --python 3.12
```

This installs only the checkout in `.venv`, without test tools or optional clients. Run Python with
`uv run --no-sync python`; add extras using the commands in the adapter guides. Development and
test tools are described in [Contributing](CONTRIBUTING.md).
To install just the checkout in an existing environment, `python -m pip install .` also works.

## Quickstart

Save this as `demo.py` in the repository and run `uv run --no-sync python demo.py`:

```python
import asyncio

from async_batcher import AsyncBatcher


async def double_many(values: list[int]) -> list[int]:
    return [value * 2 for value in values]


async def main():
    async with AsyncBatcher.from_callable(
        double_many, max_batch_size=5, max_queue_time=0.01, concurrency=2
    ) as batcher:
        results = await asyncio.gather(*(batcher.process(i) for i in range(10)))
        print(results)  # [0, 2, 4, 6, 8, 10, 12, 14, 16, 18]


asyncio.run(main())
```

`process()` is a coroutine: submit calls concurrently to form batches. A loop that awaits each
call before submitting the next normally produces single-item batches. Processing starts
automatically. Stop the batcher before closing its clients, model resources, or executor.
Existing [subclasses](docs/api.md) and adapters support the same managed lifecycle.

## Guides

- [Choose by workload](docs/workloads.md): request compatibility, safe metadata, and configuration ownership.
- [Batch an existing function](docs/functions.md): callable processors and managed cleanup.
- [Bulk and streaming producers](docs/bulk-processing.md): bounded maps and waiting for queue space.
- [Cost budgets](docs/cost-aware-batching.md): byte, token, or work limits alongside batch counts.
- [Variable-length inference](docs/variable-length-inference.md): padding, conservative token budgets, and per-request output shapes.
- [Operational visibility](docs/observability.md): snapshots and payload-free completion events.
- [Grouped batching](docs/grouped-batching.md): isolate compatible requests with bounded child ownership.
- [Cookbook](docs/cookbook.md): runnable SQLite imports, embedding pipelines, and measured batch sizes.
- [FastAPI recipe](docs/fastapi-recipe.md): lifespan-managed predictions, SQLite writes, and HTTP overload handling.
- [Operational recipes](docs/operations.md): separate deadlines, shared backend capacity, thread lifetime, and bounded telemetry.
- [Core API and lifecycle](docs/api.md): limits, result mapping, errors, shutdown, restart, and executors.
- [Contributing](CONTRIBUTING.md): locked development environments, test selection, integration services, and CI.
- [Benchmarks](BENCHMARK.md): reproducible core measurements and archived application results.
- [Public type contracts](docs/typing.md): inferred results, typed options, and installed-package checks.
- [Security reporting](SECURITY.md) and [Code of Conduct](CODE_OF_CONDUCT.md).

## Optional adapters

The eight extras are independent; importing `async_batcher` does not import their clients.
All adapters use the core lifecycle, with the limits and resource ownership described in each guide.

| Extra | Operations | Guide |
| --- | --- | --- |
| `aws` | DynamoDB reads/writes; Kinesis and Firehose records | [DynamoDB](docs/dynamodb.md), [AWS streams](docs/aws-streams.md) |
| `sqlalchemy` | SQLAlchemy inserts and ORM updates | [SQLAlchemy](docs/databases.md#sqlalchemy) |
| `scylla` | cqlengine insert/update/delete batches | [ScyllaDB](docs/databases.md#scylladb) |
| `mongodb` | Ordered native PyMongo writes | [MongoDB](docs/databases.md#mongodb) |
| `kafka` | Native aiokafka producer batches | [Kafka](docs/kafka.md) |
| `pinecone` | Vector upsert/fetch/delete | [Pinecone](docs/pinecone.md) |
| `keras` | Dense-input Keras inference | [Keras](docs/machine-learning.md#keras) |
| `sklearn` | Dense or sparse scikit-learn inference | [scikit-learn](docs/machine-learning.md#scikit-learn) |

Services that do not offer a suitable bulk operation need a custom `process_batch` implementation.
Batching alone does not enforce a service's request-rate limit or guarantee exactly-once side effects.
