# Deadlines, shared capacity, and telemetry

The [operational helpers](../examples/recipes/operations.py) are source-checkout recipes, not new
library APIs. They use the standard library and existing core contracts. Run core examples after
the [minimal source install](../README.md#install), using `uv run --no-sync python`.

## Give each deadline one job

| Budget | What expires | What it does not guarantee |
| --- | --- | --- |
| Collection: `max_queue_time` | Waiting to form a batch | An end-to-end response or backend deadline |
| Admission: `enqueue_timeout` with waiting enabled | Waiting for queue capacity | A timeout after the request has been accepted |
| HTTP response: `Intake.process(response_timeout=...)` | This caller's wait, including queueing and processing | Retraction of accepted work, rollback, or interruption of a worker thread |
| Async backend: `backend_call(..., timeout=...)` | One cancellation-cooperative async bulk call | Backend rollback, a hard execution limit for blocking code, or a timeout per item |
| Deployment: `Intake.shutdown(grace_timeout=..., force_timeout=...)` | Graceful drain, then waiting for forced cleanup | Termination of threads or the whole process within that sum |

`ResponseDeadlineExceeded` and `BackendDeadlineExceeded` are distinct. The helper checks whether
its own timeout context expired; an ordinary `TimeoutError` raised by the backend is preserved.
Timeout cancellation requires cooperative async code and can wait for that code's cleanup.
See Python's [timeout contract](https://docs.python.org/3/library/asyncio-task.html#asyncio.timeout).

This runnable example closes intake before drain and keeps the exporter alive until batch callbacks
have finished. The backend and exporter are explicit local demonstrations, with no network calls:

```python
import asyncio
from dataclasses import asdict

from async_batcher import AsyncBatcher
from examples.recipes.operations import BoundedExporter, Intake, backend_call


async def demo_backend(items):
    await asyncio.sleep(0)
    return [item * 2 for item in items]


async def processor(items):
    return await backend_call(demo_backend, items, timeout=0.5)


async def demo_export(event):
    await asyncio.sleep(0)  # Replace with a bounded async monitoring-client call.


async def main():
    async with BoundedExporter(demo_export, max_events=16) as exporter:
        batcher = AsyncBatcher.from_callable(
            processor, max_batch_size=4, max_queue_size=16, on_batch=exporter.observe
        )
        intake = Intake(batcher)
        try:
            results = await asyncio.gather(*(intake.process(item, response_timeout=1) for item in range(8)))
            print(results)
        finally:
            await intake.shutdown(grace_timeout=2, force_timeout=1)
    print(asdict(batcher.stats()))
    print({"telemetry_sent": exporter.sent, "failed": exporter.failed, "dropped": exporter.dropped})


asyncio.run(main())
```

Choose positive finite timeout values from your own backend and deployment budgets. `None` disables
the intake/backend/shutdown timeout; the telemetry exporter instead requires finite positive budgets.
If force cleanup also times out, `Intake.shutdown` raises and keeps intake closed. Cleanup can still
be running. Keep references to the batcher and its resources; retry `stop(force=True)` when
appropriate, or let the process supervisor enforce its separate termination policy. Do not
silently reopen intake or close a client while surviving work still needs it.

## Apply the policy to HTTP

The [FastAPI app](../examples/recipes/fastapi_app.py) exposes separate response and shutdown settings:

```python
from pathlib import Path

from examples.recipes.fastapi_app import create_app
from examples.recipes.operations import backend_call


async def demo_predict(rows):
    return [sum(row) for row in rows]  # Demo only, not a trained model.


async def timed_predict(rows):
    return await backend_call(demo_predict, rows, timeout=0.5)


app = create_app(
    Path("/tmp/batcher-deadlines.sqlite"),
    prediction_processor=timed_predict,
    response_timeout=1,
    shutdown_grace=5,
    shutdown_force=1,
)
```

This HTTP example requires `uv sync --locked --group example-test`. Serve the app as described in
the [FastAPI guide](fastapi-recipe.md). Full queues return 503 without accepting the item; a configured
response/backend deadline returns 504 with a distinct explanation. An unrelated backend exception,
including its own `TimeoutError`, remains 500. Stable IDs and backend-specific retry rules still
matter after any ambiguous failure.

Both child intakes close before either child starts draining. Their shutdown attempts run together;
errors are surfaced as an exception group after both attempts finish. The CLI demo uses a two-second
response wait, five-second graceful drain, and one-second force-cleanup wait. `create_app` itself
defaults those settings to `None`, so callers choose their own policy.

The ASGI server's connection drain and your supervisor's process-termination grace are additional
budgets. Account for startup/shutdown hooks, exporter drain, and backend/client cleanup too. Setting
an HTTP response deadline alone does not bound application shutdown. Use backend-native socket,
query, and transaction deadlines as applicable; cancelling an executor await does not add them.
See [Uvicorn's shutdown behavior](https://uvicorn.dev/server-behavior/#graceful-process-shutdown)
when configuring that server alongside the application policy.

## Share backend capacity across groups

A child's `concurrency` applies only to that child. Eight groups at concurrency two can attempt
sixteen calls. Pass the same semaphore to `async_groups` to cap cancellation-cooperative async
backend calls across every child, or across multiple managers that share it:

```python
import asyncio

from examples.recipes.operations import async_groups


async def demo_bulk(tenant, items):
    await asyncio.sleep(0)
    return [(tenant, value * 2) for _, value in items]


async def main():
    backend_slots = asyncio.Semaphore(2)
    async with async_groups(
        demo_bulk,
        group_key=lambda item: item[0],
        backend_slots=backend_slots,
        max_groups=4,
        batch_size=2,
    ) as groups:
        cohort = [("a", 1), ("b", 2), ("a", 3), ("b", 4)]
        print(await asyncio.gather(*(groups.process(item) for item in cohort)))


asyncio.run(main())
```

This finite cohort is a demonstration. Keep real producers bounded too: a semaphore limits calls
inside the backend section, not the number of waiting producer tasks or per-group queue memory.
Each child queue here holds at most 64 waiting items and `max_groups` bounds children. A semaphore
is a concurrency limit, not a rate limiter. It does not itself implement fairness between tenants.

For synchronous processors, share a bounded **caller-owned executor** instead. Keep its context
outside `asyncio.run`, so its actual threads join before the surrounding client resources close:

```python
import asyncio
from concurrent.futures import ThreadPoolExecutor

from examples.recipes.operations import thread_groups


def demo_bulk(tenant, items):
    return [(tenant, value * 2) for _, value in items]


async def main(pool):
    async with thread_groups(
        demo_bulk, group_key=lambda item: item[0], executor=pool, batch_size=2
    ) as groups:
        print(await asyncio.gather(*(groups.process(item) for item in [("a", 1), ("b", 2)])))


with ThreadPoolExecutor(max_workers=2) as pool:
    asyncio.run(main(pool))
# A caller-owned backend client can close after the pool has joined.
```

Forced batcher shutdown can return while a submitted synchronous function still runs. Never
release a backend semaphore immediately on coroutine cancellation and assume its thread stopped:
that can permit more actual backend work than intended. The shared executor caps simultaneously
executing threads even in that case. Its internal submission queue is not a producer backpressure
policy; retain group, queue, and pending limits. Joining the pool may outlast a deployment budget if
its functions do not finish. Use backend-native timeouts/cooperative cancellation, or a separately
supervised process for work that must be terminable; the batcher does not own this decision.

## Export metadata without blocking batch completion

`BoundedExporter.observe` only enqueues an immutable, payload-free `BatchEvent` with `put_nowait`.
It does not do network I/O, wait for capacity, or create a task per event. One worker drains at most
`max_events` queued events plus one in-flight event. Keep the exporter context outside batchers so
their final completion callbacks can still enqueue.

- `sent` counts successful callback returns, not an independently verified durable delivery.
- `failed` counts send exceptions and send deadlines; the worker continues with later events.
- `dropped` counts overflow, events offered outside its active lifetime, and unsent events discarded
  when shutdown expires. Counted loss is intentional: these diagnostics are not a durable audit log.

The async send callback must cooperate with cancellation and use its own bounded I/O operations.
Exporter shutdown first attempts a bounded drain, then cancels the sender and counts queued losses.
A client that suppresses cancellation or blocks the event loop defeats cooperative deadlines.
Expose the counters through your application monitoring without adding request payloads, secrets,
or high-cardinality tenant IDs to labels. See [core observability](observability.md).

## Verify the recipes

```bash
uv sync --locked --group example-test
uv run --no-sync python -m pytest tests/core/test_operational_recipes.py tests/examples -ra
```

Tests use gated async calls, a real borrowed thread, and HTTP requests. They check distinct deadline
outcomes, closed intake during forced cleanup, shared capacity, overflow accounting, and task cleanup.
They do not use credentials, network services, or wall-clock performance thresholds.
