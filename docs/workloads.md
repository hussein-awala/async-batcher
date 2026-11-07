# Choose batching for your workload

Start with the current [source installation](../README.md#install). The core has no runtime
dependencies; install an adapter extra only when you use that backend.

## Check the backend contract first

Batching helps when a bulk call amortizes network, transaction, or model overhead. A function that
simply performs the same independent calls in sequence may add latency without reducing that work.
Measure your own end-to-end latency and throughput; the included [benchmarks](../BENCHMARK.md) do
not predict a particular database or model's performance.

Before choosing limits, identify:

- The backend's maximum record count and encoded byte/token limits, including fixed request overhead.
- Which items can share one call: destination, operation, model revision, schema, and authentication.
- Whether results preserve input order, and how missing, duplicate, or failed results are identified.
- Which effects can already have happened when a request fails or its caller is cancelled.
- Who owns the client, model, executor, and any durable replay/checkpoint state.

Use a built-in [adapter](../README.md#optional-adapters) when its operation and failure contract fit.
Otherwise, wrap a bulk function with [`from_callable`](functions.md), return one result per item,
and use [`map`](bulk-processing.md) for bounded producers. A batcher does not make a backend
transactional, enforce a request-rate limit, or persist queued work across a process crash.

## Choose a starting shape

| Workload | First configuration decisions |
| --- | --- |
| Interactive HTTP/RPC | Choose a short collection delay from the latency budget, a finite waiting queue, and an explicit overload response. Account for backend time separately. See [FastAPI](fastapi-recipe.md) and [operational deadlines](operations.md). |
| File or message stream | For broker deliveries, see [acknowledgement and replay](acknowledged-consumer.md). Bound `map(max_pending=...)`, keep the source lazy, and consume results as they arrive. Choose ordered results only if needed. See the [cookbook](cookbook.md). |
| Database writes | Respect the adapter's transaction boundaries. Use stable record IDs and define replay behavior; concurrency can reorder commits. |
| Model inference | Group compatible model/input shapes and reserve memory for padding and outputs. A token count is an estimate, not a GPU memory measurement. |
| Embedding pipeline | Bound both stages and keep IDs attached to vectors. Validate model dimensions and the destination namespace before writing. |

Keep `max_pending` large enough for the intended batch if latency allows. Smaller windows still
work, but normally dispatch partial batches at the collection deadline. Queue and pending limits
count items; they do not bound each item's size or all upstream HTTP handlers.

Set a backend-safe count limit first. Add a conservative [cost budget](cost-aware-batching.md)
when item sizes vary, then choose finite queue/pending limits and backend concurrency. Observe
[actual batch sizes and outcomes](observability.md) before tuning. `max_queue_time` measures
collection delay from the oldest item; time waiting for an active batch, backend I/O, and response
serialization can make total latency longer.

## Preserve immutable routing metadata

Put per-request information in each submitted item. For example:

```python
from dataclasses import dataclass


@dataclass(frozen=True)
class PredictionRequest:
    tenant_id: str
    model_revision: str
    text: str


def compatibility_key(request: PredictionRequest) -> tuple[str, str]:
    return request.tenant_id, request.model_revision
```

The strings above are immutable. `frozen=True` alone does not freeze a nested list, dictionary,
or client object: copy or normalize mutable input before submitting it, or retain exclusive
ownership until processing finishes. Cost estimates are cached once per submission; changing
the payload afterward can invalidate the budget and change what the backend receives.

Derive a [`GroupedBatcher`](grouped-batching.md) key from every setting that must be shared within
one backend call. Depending on the API, that can include tenant, namespace, model revision,
normalization policy, vector dimension, operation, or schema version. Bind that configuration
into the child factory instead of changing a shared client's destination between requests.
Bound the number of groups; each child has its own queue and concurrency limit.

Validate authorization before submission. A tenant key separates work; it does not authenticate
the caller. Avoid putting credentials or mutable request objects in group keys, exceptions,
or monitoring labels. Keep high-cardinality IDs out of metric labels.

A processor runs once for a batch. Ambient `ContextVar` values are not a per-item metadata channel:
requests with different tenant or tracing values can otherwise observe one batch's context.
Carry essential metadata explicitly, even when using a separate processor context policy.

## Own configuration and cleanup

Treat constructor options and a child factory's backend configuration as construction-only
settings. Direct attribute assignment is not a supported reconfiguration API; queues and
semaphores have already been created. To change settings, close application intake, drain the
old batcher, create its replacement, and then reopen intake. Coordinate that switch with producers
instead of mutating a live shared instance.

Keep client/model/executor contexts outside the batcher context so they outlive its work. A normal
context exit drains without an implicit timeout. Forced cancellation cannot interrupt an already
running Python thread or prove that an external write did not happen; preserve those resources
until their actual work has ended. See [lifecycle and executors](api.md).
The [operational recipes](operations.md) demonstrate bounded drain/escalation, one backend capacity
limit across groups, and a telemetry exporter with explicit overflow accounting.

For an unsupported workload, open a [use-case issue](https://github.com/hussein-awala/async-batcher/issues/new?template=use-case.md)
with a small payload example, backend limits, failure semantics, and measured requirements.
Remove secrets and private customer data before sharing examples.
