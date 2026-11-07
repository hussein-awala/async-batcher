# AsyncBatcher

AsyncBatcher combines concurrent Python requests into batches and routes each result back to its caller.
Subclass `AsyncBatcher` for your own bulk operation, or use an adapter for a supported database,
messaging client, or prediction model. The core package has no runtime dependencies.

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
uv sync --locked --python 3.12
```

This installs the checkout and its default test tools in `.venv`. Run Python with
`uv run --no-sync python`; add extras using the commands in the adapter guides.
To install just the checkout in an existing environment, `python -m pip install .` also works.

## Quickstart

Save this as `demo.py` in the repository and run `uv run --no-sync python demo.py`:

```python
import asyncio

from async_batcher import AsyncBatcher


class DoubleBatcher(AsyncBatcher[int, int]):
    async def process_batch(self, batch: list[int]) -> list[int]:
        return [value * 2 for value in batch]


async def main():
    batcher = DoubleBatcher(max_batch_size=5, max_queue_time=0.01, concurrency=2)
    try:
        results = await asyncio.gather(*(batcher.process(i) for i in range(10)))
        print(results)  # [0, 2, 4, 6, 8, 10, 12, 14, 16, 18]
    finally:
        await batcher.stop()


asyncio.run(main())
```

`process()` is a coroutine: submit calls concurrently to form batches. A loop that awaits each
call before submitting the next normally produces single-item batches. Processing starts
automatically. Stop the batcher before closing its clients, model resources, or executor.

## Guides

- [Batch an existing function](docs/functions.md): callable processors and managed cleanup.
- [Core API and lifecycle](docs/api.md): limits, result mapping, errors, shutdown, restart, and executors.
- [Contributing](CONTRIBUTING.md): locked development environments, test selection, integration services, and CI.
- [Benchmarks](BENCHMARK.md): reproducible core measurements and archived application results.
- [Operational statistics](docs/observability.md): immutable snapshots, batch events, and optional monitoring recipes.
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
