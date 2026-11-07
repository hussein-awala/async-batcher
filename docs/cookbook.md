# Streaming recipes

These runnable examples target the current source checkout. They need only Python's standard
library and the core package; no server, credentials, model download, or optional SDK is required.
See [installation](../README.md#install) and run the commands from the repository root:

```bash
uv sync --locked --no-default-groups --python 3.12
```

For an HTTP application, the separate [FastAPI recipe](fastapi-recipe.md) uses an optional development
group and demonstrates lifespan-managed prediction and SQLite batchers with real HTTP tests.
The [operational recipes](operations.md) add separate response/backend/deployment deadlines,
shared capacity across groups, caller-owned thread cleanup, and bounded telemetry export.
For token-level models, the [variable-length inference recipe](variable-length-inference.md) shows
collation, attention masks, conservative padding budgets, and output restoration.

## Import a file or async stream into SQLite

[The SQLite recipe](../examples/recipes/sqlite_stream.py) wraps a synchronous bulk writer with
`AsyncBatcher.from_callable`, then consumes results from `map()` without retaining a result for
every input. Each batch uses a real SQLite transaction and a connection created and closed inside
the worker thread. A single batch is atomic; the whole input stream is not.

Run generated demonstration input:

```bash
uv run --no-sync python -m examples.recipes.sqlite_stream \
  --database /tmp/batcher-records.sqlite --rows 1000 --batch-size 32 --max-pending 64
```

Or import a JSONL file containing one record per line:

```json
{"id": "record-1", "text": "First document"}
{"id": "record-2", "text": "Second document"}
```

```bash
uv run --no-sync python -m examples.recipes.sqlite_stream \
  --input /tmp/documents.jsonl --database /tmp/batcher-records.sqlite
```

Each record must contain exactly string `id` and `text` fields. IDs must be nonempty. Repeated
IDs update existing rows in input order, including across batches. Running the demo again updates
the same IDs; use a fresh database path when comparing measurements from fresh imports.

The [file reader](../examples/recipes/support.py) reads at most 32 records per chunk, with a
64 KiB limit per JSONL line, in worker threads. Each read owns and closes its file handle, including
when the awaiting task is cancelled. It reopens the file at a byte offset for each chunk, so do
not modify or replace that file during import. The source buffer adds at most one chunk beyond
`map()`'s pending window. You can instead pass your application's async source directly:

```python
import asyncio
from pathlib import Path

from examples.recipes.sqlite_stream import write_stream
from examples.recipes.support import Document


async def incoming():
    # Replace with a cancellation-cooperative message or network source.
    for index in range(100):
        yield Document(str(index), f"Message {index}")


async def main():
    report = await write_stream(
        incoming(), Path("/tmp/batcher-message-records.sqlite"), max_pending=64, batch_size=32
    )
    print(report)


asyncio.run(main())
```

An error stops iteration and closes its source. Already committed batches remain. Cancellation
can also leave committed work even if a caller did not receive its result; the recipe's context
exits drain accepted work. Stable IDs and upserts make replay useful, but they do not provide a
global transaction, checkpoint, or exactly-once delivery guarantee.

## Connect embedding and vector-storage stages

[The pipeline recipe](../examples/recipes/embedding_vectors.py) connects two bounded maps. The
first embeds text and keeps each document ID attached to its vector. The second persists vectors
and returns IDs. Both stages preserve input order, and storage concurrency is one, so later
duplicate IDs overwrite earlier inputs in a predictable order.

```bash
uv run --no-sync python -m examples.recipes.embedding_vectors \
  --database /tmp/batcher-vectors.sqlite --rows 1000 --batch-size 32 --max-pending 64
```

The included `DemoEmbeddingModel` returns three text features: character count, word count, and
digit count. **It is not a semantic embedding model.** `SQLiteVectorSink` stores JSON vectors
durably; it does not implement similarity search. These components exercise the complete pipeline
without paid services. `--input /tmp/documents.jsonl` accepts the same file format as the writer.

Replace the two small protocols with application-owned clients:

- `EmbeddingModel.embed(texts)` must return one nonempty finite vector per input text, in order.
- `VectorSink.upsert(records)` must persist records and return their IDs in input order. Define
  partial failure and retry behavior in that implementation; the pipeline does not retry it.

Call `await embed_and_store(source, model, sink)` while those clients are open. The function owns
and stops its two batchers, but does not close caller-owned model or storage clients. Use an async
SDK or offload synchronous I/O with `asyncio.to_thread`; never block the event loop in an async
protocol method. See the [Pinecone adapter guide](pinecone.md) when integrating that service.

With `max_pending=64`, each stage retains at most 64 consumed, unyielded items. The pipeline can
therefore have up to 128 such items across its stages, plus the file reader's bounded chunk.
These are item limits, not a total-memory byte limit: vector dimensions and each source item also
matter. A stalled sink stops consumption once those windows fill. `aclosing` ensures that failures
and early cancellation close both stage iterators and the original source.

## Read the measurements and choose limits

Both commands print measurements from their actual run. They do not print a claimed benchmark
speedup or compare unrelated workloads:

| Output | Meaning |
| --- | --- |
| `completed_items` | Results yielded by the final stage; duplicate IDs still count as separate requests. |
| `elapsed_seconds` | Time from starting consumption through batcher drain; database/table initialization is excluded. |
| `items_per_second` | Completed requests divided by that elapsed time. |
| `stats` | Final core counters and gauges; a successful run has no pending items or active batches. |
| `batch_size_histogram` | Actual batch size → number of completed batches, aggregated by the `on_batch` hook. |
| `embedding`, `storage` | Separate stage statistics/histograms in the pipeline report; elapsed time remains end-to-end. |

The observer keeps aggregated counts, not payloads or one event object per batch. Throughput
depends on storage, input sizes, warm caches, and concurrent machine load. Use the retained
measurements to tune your workload; see the [benchmark methodology](../benchmarks/README.md) for
controlled comparisons. Tests check real rows and measurements, with no speed threshold.

| Setting | Starting point and tradeoff |
| --- | --- |
| `batch_size` | 32 here. Larger batches reduce transaction/call overhead but wait for more input and affect more requests on failure. Respect backend limits. |
| `max_pending` | 64 per stage here. Keep it at least as large as the intended batch; smaller windows still work but rely more on partial-batch deadlines. |
| `max_queue_size` | Matches `max_pending` here. Map waits for capacity; do not replace it with an unbounded list of `process()` tasks. |
| `max_queue_time` | 10 ms here. Controls collection delay, not end-to-end latency; a busy processor can delay dispatch further. |
| `concurrency` | One writer here, matching SQLite's serialized-write behavior. Increase remote-service concurrency only within that service's capacity. |
| `max_batch_cost` | 64 KiB of encoded text/ID payload for SQLite writes, and text for embeddings. This illustrates an estimate; it is not serialized request size, an API token count, or total process memory. |

For remote embeddings, replace the byte estimator with your tokenizer's token count and budget
for the actual model's limits. Reserve fixed request overhead separately. Keep source payloads
unchanged after submission because their costs are cached. A row above the configured cost budget
fails before being sent to the processor; route such records explicitly instead of silently
splitting them into a different logical operation.

## Related core guides

- [Functions and managed lifecycle](functions.md)
- [Streaming results and waiting admission](bulk-processing.md)
- [Count and cost limits](cost-aware-batching.md)
- [Statistics and completion events](observability.md)
- [Groups of compatible requests](grouped-batching.md)
- [Complete API and shutdown contracts](api.md)
