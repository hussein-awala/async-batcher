# Two bounded starting configurations

These examples use the repository recipes for Async Batcher 0.3.0. Set up the matching
[recipe checkout](installation.md#run-the-repository-recipes); the `examples` modules are not part
of the installed package.
Choose the shape that matches who controls the producers. The numbers are demonstration limits;
check the backend's count, payload, and concurrency limits before changing them.

## File importer: wait for capacity

The producer owns the input stream and can slow down. This complete example writes generated
records to SQLite without creating a task or retaining a result for every input:

```python
import asyncio
from pathlib import Path

from examples.recipes.sqlite_stream import write_stream
from examples.recipes.support import Document


async def records():
    for index in range(1000):
        yield Document(str(index), f"Record {index}")


async def main():
    report = await write_stream(records(), Path("import.sqlite"), batch_size=32, max_pending=64)
    print(report["completed_items"])


asyncio.run(main())
```

Save it as `import_demo.py` in the recipe checkout and run `uv run --no-sync python import_demo.py`.
The recipe sets a queue limit of 64, one active writer batch, a 10 ms collection delay, and a 64 KiB encoded payload budget.
`map` waits for admission and bounds consumed-but-not-yielded input to 64 items. SQLite commits each
batch independently; cancellation or failure can leave earlier writes committed. Stable IDs update
existing rows on replay. Use the [resumable recipe](resumable-import.md) for atomic byte checkpoints
against immutable input. For a JSONL source, replace `records()` with
`read_jsonl(Path("documents.jsonl"))` from `examples.recipes.support`.

## Interactive service: reject overload explicitly

The service cannot control when clients arrive. Use a finite waiting queue and return an overload
response rather than accumulating unlimited waiting handlers. This complete ASGI application uses
the existing lifespan-managed recipe and its deterministic demonstration predictor:

```python
from pathlib import Path

from examples.recipes.fastapi_app import create_app

app = create_app(
    Path("service.sqlite"),
    batch_size=16,
    queue_size=64,
    max_queue_time=0.01,
    response_timeout=2,
    shutdown_grace=5,
    shutdown_force=1,
)
```

Save it as `service_demo.py` in the recipe checkout. The optional `example-test` group
provides FastAPI and Uvicorn for this demo; it is not needed by the core library:

```bash
uv sync --locked --group example-test --python 3.12
uv run --no-sync uvicorn service_demo:app --host 127.0.0.1 --port 8000 --workers 1
```

`POST /predict` accepts `{"features":[1,2,3]}` and returns `{"score":2.3}`. A full queue returns
HTTP 503 with `Retry-After`; response expiry returns HTTP 504 while accepted work can still finish.
One batch runs at a time per batcher. The two-second response budget includes waiting and processing;
the 10 ms collection delay is not an end-to-end deadline. The shutdown budgets control waiting and
escalation, not a guarantee that cancellation can stop external writes or running threads.
Read the [FastAPI guide](fastapi-recipe.md) before replacing the demo predictor with a model/client.

## What each bound covers

| Limit | Covers | Does not cover |
| --- | --- | --- |
| `max_batch_size` | Items in one dispatched batch | Bytes per item or backend rate |
| `max_queue_size` | Items buffered for dispatch | Active batches or external producer tasks |
| `map(max_pending=...)` | Consumed inputs whose results have not been yielded by that map | Other maps/producers or source buffers |
| `concurrency` | Active batch processors on one batcher | Other processes, groups, or SDK retry attempts |

`max_queue_size=-1` **or `0` means unlimited**. The core keeps these compatibility defaults;
the examples choose finite values explicitly. Item limits do not establish a total memory bound.
Validate input sizes and set server connection/body limits as appropriate. Multiple server workers
have separate queues and batchers; their total backend demand adds up.

Start with one of these paths and measure actual batch sizes and outcomes. Add
[cost budgets](cost-aware-batching.md) for variable payload sizes,
[groups](grouped-batching.md) for incompatible requests, or
[coalescing](coalescing.md) for equivalent concurrent reads only when the workload needs them.
Keep per-item tenant/model metadata explicit. For services that handle sensitive backend errors,
consider `log_exceptions=False`; for a neutral batch context, see `processor_context` in the
[API guide](api.md). Neither option replaces request authorization or application logging policy.
