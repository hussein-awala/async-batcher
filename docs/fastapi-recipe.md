# FastAPI lifespan, predictions, and SQLite writes

[This runnable app](../examples/recipes/fastapi_app.py) creates one prediction batcher and one
SQLite writer during FastAPI startup. The lifespan context drains both before shutdown finishes.
It uses a deterministic weighted sum as a **demo predictor, not a trained model**, and real SQLite
transactions for writes. No model download, server account, or credentials are needed.

The example dependencies are an opt-in development group. They are not added to the core package
or any adapter extra. From the current [source checkout](../README.md#install):

```bash
uv sync --locked --group example-test --python 3.12
BATCHER_DEMO_DATABASE=/tmp/batcher-web.sqlite \
  uv run --no-sync uvicorn examples.recipes.fastapi_app:demo_app --factory \
  --host 127.0.0.1 --port 8000 --workers 1
```

Keep that process running and use another terminal:

```bash
curl --fail-with-body http://127.0.0.1:8000/predict \
  -H 'Content-Type: application/json' -d '{"features":[1,2,3]}'
curl --fail-with-body http://127.0.0.1:8000/documents \
  -H 'Content-Type: application/json' -d '{"id":"example-1","text":"Stored in SQLite"}'
curl --fail-with-body http://127.0.0.1:8000/metrics
```

`/predict` returns `{"score":2.3}` for this input. `/documents` returns HTTP 201 and the stored ID.
The same ID updates its previous value. Each write batch has its own transaction; several HTTP
requests are not one global transaction. The database path persists across restarts; choose a new
path when you need an empty demo database.

The input models require three finite numeric features between -1,000,000 and 1,000,000, and
document IDs of 1–128 characters with text up to 4,096 characters. Invalid bodies return HTTP 422
before reaching a batcher. These are demonstration bounds, not a limit on the entire HTTP body:
set appropriate request-body and connection limits in your serving infrastructure too.

## Batch concurrent requests and inspect measurements

Sequential curl commands normally form single-item batches. This bounded cohort sends 32 requests
concurrently and prints actual client elapsed time plus the server's aggregated batch sizes.
Run it with `uv run --no-sync python` from the same checkout:

```python
import asyncio
import time

import httpx2


async def main():
    async with httpx2.AsyncClient(base_url="http://127.0.0.1:8000", timeout=5) as client:
        started = time.perf_counter()
        responses = await asyncio.gather(
            *(client.post("/predict", json={"features": [1, 2, 3]}) for _ in range(32))
        )
        for response in responses:
            response.raise_for_status()
        elapsed = time.perf_counter() - started
        print({"completed_requests": len(responses), "elapsed_seconds": elapsed})
        metrics = await client.get("/metrics")
        metrics.raise_for_status()
        print(metrics.json())


asyncio.run(main())
```

`/metrics` reads `stats()` on the owning event loop and aggregates `on_batch` completion events.
It returns counts and actual batch-size histograms, with no request payloads. Values accumulate
since this app's lifespan started, including earlier requests. They are JSON diagnostics, not a
Prometheus endpoint. See [observability](observability.md) for exporter integration.

Client elapsed time includes HTTP overhead and the chosen demonstration workload; it is not a
benchmark speedup claim. There are no performance thresholds in CI. The tests make real in-process
HTTP requests to FastAPI and verify SQLite rows, batched responses, overload, failures, and cleanup.

## Capacity, failures, and ownership

The defaults are 16 items per batch, 64 waiting items per queue, one active batch per batcher,
and a 10 ms collection delay. The SQLite writer also has a 64 KiB encoded ID/text cost budget.
The budget is an estimate of submitted payload, not serialized request size or process memory.

HTTP handlers use the default immediate queue-admission policy. A full queue returns HTTP 503
with `Retry-After: 1` and does not enqueue that request. The header is an illustrative retry delay;
clients should bound retries and apply backoff. Accepted requests continue independently. A backend
exception, including a backend `TimeoutError`, remains HTTP 500 instead of being described as a
full queue. Use stable IDs and understand backend side effects before retrying a failed write.

Waiting admission is available through [`process(..., wait_for_space=True)`](bulk-processing.md),
but waiting HTTP handlers still occupy server resources. Choose server concurrency limits and a
clear timeout policy before enabling it; `max_queue_size` alone does not bound external waiting
producers. `max_queue_time` is a collection delay, not a handler deadline.

When lifespan shutdown starts, the app rejects late submissions instead of restarting a drained
child. It waits for accepted work to finish before clearing application state. Cancelling an HTTP
handler does not retract a backend write; caller cancellation and shutdown are not rollback
guarantees. The normal context exit is graceful and has no implicit timeout. Configure your server's
shutdown budget and use an explicit application force policy when required; running synchronous
functions cannot be interrupted by cancelling their asyncio tasks.

`create_app(database, prediction_processor=...)` can inject a real bulk function. Regular functions
run in the thread executor; async functions must avoid blocking the event loop. Load application-owned
models/clients in lifespan before entering the batcher contexts, then close them after those contexts
finish. The example does not close a separately supplied model/client callable on the caller's behalf.
Each SQLite operation owns and closes its own connection in its worker thread.

These limits and metrics apply **per application process**. Multiple Uvicorn workers create separate
batchers, while SQLite still serializes database writes. Use the single-worker command above for the
demo; choose a database and worker configuration appropriate to the deployment. The
[streaming cookbook](cookbook.md) shows bounded background producers and a two-stage embedding pipeline.

## Run the HTTP checks

```bash
uv sync --locked --group example-test --python 3.12
uv run --no-sync python -m pytest tests/examples -ra
```

Core-only test discovery does not import FastAPI. CI installs the optional group explicitly and
checks imports before running the HTTP suite, so an unexpectedly missing dependency cannot silently
skip the expected job. `TestClient` is used as a context manager for lifespan startup/shutdown;
async HTTPX2 tests explicitly enter the registered lifespan because ASGITransport does not do so.
See the official [FastAPI lifespan guide](https://fastapi.tiangolo.com/advanced/events/),
[lifespan testing guide](https://fastapi.tiangolo.com/advanced/testing-events/), and
[HTTPX ASGI transport lifecycle](https://www.python-httpx2.org/advanced/transports/#asgi-startup-and-shutdown).
