# Operational statistics and batch events

The core exposes immutable `BatcherStats` and `BatchEvent` dataclasses. It does not depend on a
monitoring client, collect item payloads, or start an exporter. Read these APIs from the same
event-loop thread that owns the batcher. This guide describes the current source checkout;
see the [installation instructions](../README.md#installation) for release/source differences.

## Read a snapshot

```python
import asyncio

from async_batcher import AsyncBatcher, BatcherStats


class Doubler(AsyncBatcher[int, int]):
    async def process_batch(self, batch: list[int]) -> list[int]:
        return [item * 2 for item in batch]


async def main():
    batcher = Doubler(max_batch_size=8)
    try:
        assert await batcher.process(3) == 6
    finally:
        await batcher.stop()
    stats: BatcherStats = batcher.stats()
    assert stats.accepted == stats.succeeded == 1
    assert stats.pending_items == stats.active_batches == 0


asyncio.run(main())
```

`stats()` returns a new snapshot. Reading it does not bind an event loop, reset counters, or
change an earlier snapshot. It examines outstanding accepted futures, so its cost grows with
outstanding work; use periodic sampling instead of reading it for every item. Futures that
completed but whose bookkeeping callbacks have not run yet are included in terminal totals.

| Field | Meaning |
| --- | --- |
| `queued_items` | Entries waiting for collection, including a retained item that would exceed the preceding batch's cost limit. Includes already-cancelled requests until the dispatcher removes their entries. |
| `pending_items` | Accepted requests whose result futures are not terminal, including items currently collected into a partial batch. |
| `in_flight_items` | Items in dispatched batch workers until their completion cleanup finishes. Includes requests whose callers have already cancelled. |
| `active_batches` | Dispatched batch workers awaiting completion cleanup. |
| `accepted` | Lifetime requests admitted to the input queue. |
| `rejected` | Lifetime submissions refused because the queue is full, an admission wait times out, or shutdown interrupts/refuses admission on the owning loop. Invalid arguments/costs and foreign-loop calls are excluded. |
| `succeeded`, `failed`, `cancelled` | Lifetime terminal outcomes of accepted result futures, counted once each. |
| `batches_dispatched`, `batches_completed` | Lifetime batch workers created and batch completion cleanups finished. |

Totals survive graceful stop, forced stop, and automatic restart. They reset only when a new
batcher is constructed. Every snapshot satisfies:

```text
accepted = pending_items + succeeded + failed + cancelled
batches_dispatched = active_batches + batches_completed
```

The gauges describe different stages and must not be added together. A partially collected batch
can have pending items but no queued or in-flight items. A running batch can have in-flight items
but no pending results after its callers cancel. Cancellation describes the result delivered to
the caller: a synchronous backend function can still finish after forced shutdown. A cancelled
request does not later become a success in these statistics. Work cancelled before admission is
not accepted and contributes no terminal outcome or rejection. A producer waiting for capacity
is not yet accepted and contributes to none of the work gauges.

## Receive completion events

Pass `on_batch` at construction. It receives one event for each dispatched batch, after resource
cleanup and request outcomes are settled. No event is emitted for items cancelled while still
queued or collected if they never become a dispatched batch. Events can arrive in completion
order rather than dispatch order when concurrency exceeds one.

```python
from async_batcher import BatchEvent

completed_batches: list[BatchEvent] = []


def on_batch(event: BatchEvent) -> None:
    completed_batches.append(event)


# Using the Doubler subclass above:
batcher = Doubler(max_batch_size=8, on_batch=on_batch)
```

The list is a small demonstration; use a bounded sink or aggregate metrics in a long-running
application. Every event contains only these fields:

- `batch_id`: a monotonically increasing dispatch identifier for this batcher, retained across restarts.
- `size`: actual number of input entries passed to the batch worker.
- `queue_wait_seconds`: oldest item's time from acceptance to processing start. If the worker never
  starts, this is its time from acceptance to completion cleanup instead. Busy processors can make
  residence exceed `max_queue_time`; it is not a per-item latency distribution.
- `processing_seconds`: processor execution and result-validation time, excluding the observer.
  It is `None` if the worker was cancelled before it started. For a cancelled executor await, this
  measures the batch worker's wait, not the eventual duration of the still-running backend function.
- `succeeded`, `failed`, `cancelled`: per-item result-future outcomes, summing to `size`.

Events contain no items, results, exception objects, request IDs, or user labels. Their outcome
totals cover dispatched batches only; use `stats()` for all accepted requests, including queued
cancellations. A returned per-item exception counts as failure, a returned `CancelledError` as
cancellation, and a whole-batch processor/result-validation exception fails unresolved requests.

The callback runs synchronously on the event-loop thread and must finish quickly. Do not perform
network I/O, block, await shutdown, or create an unbounded task for every event. Ordinary callback
exceptions are logged without changing request results. `KeyboardInterrupt`, `SystemExit`, and
other `BaseException` subclasses are not swallowed. Cleanup happens before invocation.

Async functions, async generators, and objects with async `__call__` are rejected at construction.
If a synchronous wrapper unexpectedly returns an awaitable, it is logged as misuse: a coroutine
is closed, and an explicit synchronous `close()` on a custom awaitable is called when available.
The library never invokes a custom `__await__()` or awaits asynchronous cleanup. Custom resources
without that synchronous cleanup protocol remain the callback owner's responsibility. Returned
asyncio futures/tasks are left running because the observer might have borrowed them; their
eventual exceptions are retrieved to avoid unobserved-error warnings. They remain the callback
owner's responsibility. Such return values are unsupported; the hook must return `None`.

## Optional Prometheus recipe

Install `prometheus-client` in your application, then connect an inexpensive in-process callback.
Keep exporter/server setup in the application's lifecycle. Choose histogram buckets for the
actual workload; the following are illustrative. See the official [instrument types](https://prometheus.github.io/client_python/instrumenting/)
and [histogram API](https://prometheus.github.io/client_python/instrumenting/histogram/).

```python
from prometheus_client import Counter, Gauge, Histogram

from async_batcher import BatchEvent

queue_depth = Gauge("batcher_queue_items", "Waiting input entries")
duration = Histogram(
    "batcher_processing_seconds",
    "Batch worker processing time",
    buckets=(0.001, 0.01, 0.1, 1, 10),
)
outcomes = Counter(
    "batcher_dispatched_item_outcomes_total",
    "Outcomes in completed dispatched batches",
    ["outcome"],
)


def prometheus_batch(event: BatchEvent) -> None:
    if event.processing_seconds is not None:
        duration.observe(event.processing_seconds)
    for outcome in ("succeeded", "failed", "cancelled"):
        outcomes.labels(outcome=outcome).inc(getattr(event, outcome))


def sample_queue_on_owner_loop(batcher) -> None:
    queue_depth.set(batcher.stats().queued_items)


# Construct once: Doubler(on_batch=prometheus_batch).
# Call sample_queue_on_owner_loop periodically from your application's event loop.
```

Do not call `stats()` from a Prometheus HTTP server's scraping thread. Sample on the owning loop,
then let the metrics client expose the recorded values. Use a small fixed set of labels, not
payload values or batch IDs. Recreating instruments repeatedly in one registry can cause duplicate
registration errors; define them once for the application or use separate registries.

## Optional OpenTelemetry recipe

Configure an application-owned SDK `MeterProvider` and exporter before obtaining the meter;
the API alone does not export data. The [official Python instrumentation guide](https://opentelemetry.io/docs/languages/python/instrumentation/#metrics)
and [metrics API](https://opentelemetry-python.readthedocs.io/en/latest/api/metrics.html)
describe provider setup and instruments. No OpenTelemetry package is required by this library.

```python
from opentelemetry import metrics

from async_batcher import BatchEvent

meter = metrics.get_meter("example.batcher")
processing = meter.create_histogram("batcher.processing", unit="s")
completed = meter.create_counter("batcher.dispatched.items", unit="1")


def otel_batch(event: BatchEvent) -> None:
    if event.processing_seconds is not None:
        processing.record(event.processing_seconds)
    for outcome in ("succeeded", "failed", "cancelled"):
        completed.add(getattr(event, outcome), {"outcome": outcome})


# Construct once: Doubler(on_batch=otel_batch).
```

Keep export work off the hook's critical path through your SDK's configured reader/exporter.
If collecting batcher gauges through an SDK callback running on another thread, first copy a
snapshot on the owning loop and let that callback read the immutable copy. The same outcome
scope and cancellation limitations apply to both recipes.
