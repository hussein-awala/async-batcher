# AsyncBatcher

[Documentation](https://hussein.awala.fr/async-batcher/0.3.0/) ·
[Release notes](https://hussein.awala.fr/async-batcher/release-notes/)

AsyncBatcher combines concurrent Python requests into batches and routes each result back to its caller.
Wrap an existing bulk function, subclass `AsyncBatcher`, or use an adapter for a supported database,
messaging client, or prediction model. The core package has no runtime dependencies.

## Start with your workload

| You need to… | Start here |
| --- | --- |
| Combine concurrent web requests into model predictions or writes | [Function quickstart](#quickstart), then the [FastAPI lifespan recipe](https://hussein.awala.fr/async-batcher/0.3.0/fastapi-recipe/) |
| Import a large file or consume an async stream without keeping all results | [Bounded maps](https://hussein.awala.fr/async-batcher/0.3.0/bulk-processing/), [SQLite imports](https://hussein.awala.fr/async-batcher/0.3.0/cookbook/#import-a-file-or-async-stream-into-sqlite), and [recovery choices](https://hussein.awala.fr/async-batcher/0.3.0/cookbook/#choose-an-import-or-recovery-policy) |
| Use an existing database, messaging, or prediction client | Choose its [adapter](#optional-adapters) and read its failure/ownership rules |
| Send embeddings to a vector store | [Two-stage pipeline](https://hussein.awala.fr/async-batcher/0.3.0/cookbook/#connect-embedding-and-vector-storage-stages) and [Pinecone](https://hussein.awala.fr/async-batcher/0.3.0/pinecone/) |
| Keep tenants, model revisions, or backend destinations separate | [Compatible request groups](https://hussein.awala.fr/async-batcher/0.3.0/grouped-batching/) and the [workload guide](https://hussein.awala.fr/async-batcher/0.3.0/workloads/) |
| Limit bytes, tokens, or estimated work as well as request count | [Cost budgets](https://hussein.awala.fr/async-batcher/0.3.0/cost-aware-batching/) |

Start with a [bounded importer or interactive service](https://hussein.awala.fr/async-batcher/0.3.0/starting-configurations/).
The [workload guide](https://hussein.awala.fr/async-batcher/0.3.0/workloads/) helps choose limits, preserve routing metadata, and decide
whether batching fits your backend. Batching requires a useful bulk operation; it is not a rate
limiter, durable queue, or exactly-once delivery system.

## Install

Python 3.11 or newer is required. This README targets **Async Batcher 0.3.0**;
check the [release history](https://hussein.awala.fr/async-batcher/release-notes/)
for publication status. Install the package into your application's environment:

```bash
python -m pip install 'async-batcher==0.3.0'
```

The core has no runtime dependencies. Add only the extras you need, for example:

```bash
python -m pip install 'async-batcher[aws]==0.3.0'
```

See [installation](https://hussein.awala.fr/async-batcher/0.3.0/installation/) for uv, other extras, and repository recipes,
and [migration from 0.2.2](https://hussein.awala.fr/async-batcher/0.3.0/migration/) before upgrading. Core and adapter usage
does not need a source checkout. Runnable files under `examples/` are repository
recipes, with separate [setup instructions](https://hussein.awala.fr/async-batcher/0.3.0/installation/#run-the-repository-recipes).

## Quickstart

Save this as `demo.py` and run `python demo.py` in the environment where you installed the package:

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

`process()` is a coroutine: submit calls concurrently to form batches. A loop that awaits each
call before submitting the next normally produces single-item batches. Processing starts
automatically. Stop the batcher before closing its clients, model resources, or executor.
Existing [subclasses](https://hussein.awala.fr/async-batcher/0.3.0/api/) and adapters support the same managed lifecycle.

## Guides

- [Compatibility and migration](https://hussein.awala.fr/async-batcher/0.3.0/migration/): published features, Python requirements, and upgrade checks.
- [Bounded starting configurations](https://hussein.awala.fr/async-batcher/0.3.0/starting-configurations/): copyable importer and interactive service examples.
- [Choose by workload](https://hussein.awala.fr/async-batcher/0.3.0/workloads/): request compatibility, safe metadata, and configuration ownership.
- [Batch an existing function](https://hussein.awala.fr/async-batcher/0.3.0/functions/): callable processors and managed cleanup.
- [Bulk and streaming producers](https://hussein.awala.fr/async-batcher/0.3.0/bulk-processing/): bounded maps and waiting for queue space.
- [Cost budgets](https://hussein.awala.fr/async-batcher/0.3.0/cost-aware-batching/): byte, token, or work limits alongside batch counts.
- [Variable-length inference](https://hussein.awala.fr/async-batcher/0.3.0/variable-length-inference/): padding, conservative token budgets, and per-request output shapes.
- [Operational visibility](https://hussein.awala.fr/async-batcher/0.3.0/observability/): snapshots and payload-free completion events.
- [Grouped batching](https://hussein.awala.fr/async-batcher/0.3.0/grouped-batching/): isolate compatible requests with bounded child ownership.
- [In-flight coalescing](https://hussein.awala.fr/async-batcher/0.3.0/coalescing/): share equivalent concurrent requests with bounded ownership.
- [Cookbook](https://hussein.awala.fr/async-batcher/0.3.0/cookbook/): runnable SQLite imports, embedding pipelines, and measured batch sizes.
- [Resumable imports](https://hussein.awala.fr/async-batcher/0.3.0/resumable-import/): immutable JSONL input with atomic SQLite checkpoints.
- [Rejected-record imports](https://hussein.awala.fr/async-batcher/0.3.0/rejected-import/): continue past known input errors and retain a metadata-only audit.
- [Durable embedding handoff](https://hussein.awala.fr/async-batcher/0.3.0/durable-embeddings/): save vectors before delivery and replay unfinished upserts.
- [Provider quota and retry policy](https://hussein.awala.fr/async-batcher/0.3.0/provider-policy/): shared pacing, finite throttling retries, and cancellation.
- [Cohere to Pinecone](https://hussein.awala.fr/async-batcher/0.3.0/cohere-pinecone/): real embeddings into an existing compatible vector index.
- [FastAPI recipe](https://hussein.awala.fr/async-batcher/0.3.0/fastapi-recipe/): lifespan-managed predictions, SQLite writes, and HTTP overload handling.
- [Operational recipes](https://hussein.awala.fr/async-batcher/0.3.0/operations/): separate deadlines, shared backend capacity, thread lifetime, and bounded telemetry.
- [Core API and lifecycle](https://hussein.awala.fr/async-batcher/0.3.0/api/): limits, result mapping, errors, shutdown, restart, and executors.
- [Contributing](https://github.com/hussein-awala/async-batcher/blob/0.3.0/CONTRIBUTING.md): locked development environments, test selection, integration services, and CI.
- [Benchmarks](https://github.com/hussein-awala/async-batcher/blob/0.3.0/BENCHMARK.md): reproducible core measurements and archived application results.
- [Public type contracts](https://hussein.awala.fr/async-batcher/0.3.0/typing/): inferred results, typed options, and installed-package checks.
- [Security reporting](https://github.com/hussein-awala/async-batcher/blob/0.3.0/SECURITY.md) and [Code of Conduct](https://github.com/hussein-awala/async-batcher/blob/0.3.0/CODE_OF_CONDUCT.md).

## Optional adapters

The eight extras are independent; importing `async_batcher` does not import their clients.
All adapters use the core lifecycle, with the limits and resource ownership described in each guide.

| Extra | Operations | Guide |
| --- | --- | --- |
| `aws` | DynamoDB reads/writes; Kinesis and Firehose records | [DynamoDB](https://hussein.awala.fr/async-batcher/0.3.0/dynamodb/), [AWS streams](https://hussein.awala.fr/async-batcher/0.3.0/aws-streams/) |
| `sqlalchemy` | SQLAlchemy inserts and ORM updates | [SQLAlchemy](https://hussein.awala.fr/async-batcher/0.3.0/databases/#sqlalchemy) |
| `scylla` | cqlengine insert/update/delete batches | [ScyllaDB](https://hussein.awala.fr/async-batcher/0.3.0/databases/#scylladb) |
| `mongodb` | Ordered native PyMongo writes | [MongoDB](https://hussein.awala.fr/async-batcher/0.3.0/databases/#mongodb) |
| `kafka` | Native aiokafka producer batches | [Kafka](https://hussein.awala.fr/async-batcher/0.3.0/kafka/) |
| `pinecone` | Vector upsert/fetch/delete | [Pinecone](https://hussein.awala.fr/async-batcher/0.3.0/pinecone/) |
| `keras` | Dense-input Keras inference | [Keras](https://hussein.awala.fr/async-batcher/0.3.0/machine-learning/#keras) |
| `sklearn` | Dense or sparse scikit-learn inference | [scikit-learn](https://hussein.awala.fr/async-batcher/0.3.0/machine-learning/#scikit-learn) |

Services that do not offer a suitable bulk operation need a custom `process_batch` implementation.
Batching alone does not enforce a service's request-rate limit or guarantee exactly-once side effects.
