# Variable-length token inference

[This runnable recipe](../examples/recipes/variable_inference.py) groups compatible token rows,
pads each batch, calls a synchronous model once, and restores each request's original output length.
It needs only the core package and standard library. The included model **doubles token IDs for a
demonstration; it is not trained and does not produce semantic predictions**.

```bash
uv sync --locked --no-default-groups --python 3.12
uv run --no-sync python -m examples.recipes.variable_inference
```

The finite 32-request cohort prints actual model-call shapes, completed requests, and elapsed time
through group drain. These are observations of the demo, not an inference speedup claim. The
histogram retains counts for bounded `(rows, padded_width)` combinations, not token payloads.

## Preserve inputs and restore outputs

`TokenRequest` snapshots a list/tuple of nonnegative integer token IDs into an immutable tuple.
It also carries immutable request ID, tenant, and model revision. Empty sequences are rejected;
choose an explicit application policy for them before submission. No tokenizer is downloaded or
implied: use the vocabulary and special-token policy for your actual model.

`collate` preserves row order and pads to the longest row **in that batch**, using zero padding.
Its attention mask uses original lengths, so a real zero token is still marked as input. Change
the padding convention before using a model that requires a different pad token.

For example, token rows `(0, 2)` and `(7,)` become:

| Field | First row | Second row |
| --- | --- | --- |
| `input_ids` | `(0, 2)` | `(7, 0)` |
| `attention_mask` | `(1, 1)` | `(1, 0)` |
| Demo model output | `(0, 4)` | `(14, 0)` |
| Routed `TokenPrediction.values` | `(0, 4)` | `(14,)` |

The synchronous model receives one immutable `ModelBatch`. It must return a list/tuple with one
list/tuple of finite numeric values for every padded row. This example is token-level scalar
prediction; adapt another output head explicitly. Convert arrays/tensors with the appropriate
`.tolist()` operation before returning, and validate the expected dimensions in that adapter.

The recipe validates the complete padded output, including discarded padding positions, before
returning any result. Wrong row counts, wrong widths, nonnumeric or nonfinite values fail the whole
batch. `unbatch` then trims to original lengths and attaches each original request ID. A borrowed
async result is not awaited or cancelled; use a separate async-model recipe instead of returning
awaitables from this synchronous callback.

## Budget for padding, not just input token totals

Summing raw lengths can underestimate padded work: lengths 1 and 16 contain 17 tokens, but a dense
two-row batch occupies 32 token positions. The recipe uses increasing length buckets `(16, 64, 256)`
and charges **the bucket ceiling for every item**, using the core `item_cost` callback.

For a group with ceiling `B` and `N` requests, the actual padded width is at most `B`. The cost
check enforces `N × B <= padded_token_budget`, so actual padded positions also fit that budget.
With the default 256-position budget and count limit 16:

| Bucket ceiling | Maximum rows from the cost budget | Maximum rows after the count limit |
| --- | --- | --- |
| 16 | 16 | 16 |
| 64 | 4 | 4 |
| 256 | 1 | 1 |

All configured buckets must fit the budget. An item above the largest bucket fails before creating
a group or calling the model. Split oversized documents upstream only if you can define how their
chunk predictions should be recombined; the recipe does not silently truncate or change one request
into several independent results.

The estimate is deliberately conservative: a short row near the bottom of a bucket consumes its
full ceiling. Choose finer buckets to reduce waste, while accounting for more partially filled
groups and the `max_groups` limit. The 10 ms collection delay still dispatches partial batches.
Count, cost, queue age, and per-group concurrency limits apply together.

Padded token positions are **not a GPU-memory limit**. Input tensors, attention masks, output heads,
dtype, hidden activations, caches, and model weights also use memory. Attention work may scale
nonlinearly with sequence length. Profile the actual model and device before choosing ceilings,
budgets, and concurrency; leave room for fixed overhead and other workloads.

## Keep incompatible work and resources separate

The group key contains tenant, model revision, and length bucket. Add other compatibility settings
when they affect a shared call, such as vocabulary, normalization, output-head selection, or a
backend destination. Authenticate requests before submission; group keys are not authorization.
Bind immutable model/client configuration instead of changing shared state between tenants.
Children use an explicit empty processor context and metadata-only failure logging, so the first
request's ambient context and exception text do not become shared model context or log payloads.
Pass required service information explicitly in model configuration or `ModelBatch`.

All child batchers borrow the **same caller-owned `ThreadPoolExecutor`**, so `max_workers` bounds
simultaneously executing synchronous calls across groups. The demo uses one worker. Keep the model
alive until that executor has joined; forced batcher shutdown cannot interrupt a running inference
thread. The helper neither closes the model nor shuts down the pool.

```python
import asyncio
from concurrent.futures import ThreadPoolExecutor

from examples.recipes.variable_inference import DemoTokenModel, TokenRequest, inference_groups


async def main(pool):
    model = DemoTokenModel()  # Replace with a synchronous ModelBatch -> dense rows callable.
    async with inference_groups(model, executor=pool, padded_token_budget=256) as groups:
        cohort = [
            TokenRequest("first", "tenant-a", "demo-v1", [1, 2, 3]),
            TokenRequest("second", "tenant-a", "demo-v1", [4]),
        ]
        print(await asyncio.gather(*(groups.process(item) for item in cohort)))


with ThreadPoolExecutor(max_workers=1) as pool:
    asyncio.run(main(pool))
```

This uses a small finite cohort. Do not create one task per item for an unbounded source. Bound
producers and admission, and remember that each child here has a 64-item waiting queue. Group slots,
queues, and a shared executor limit different resources; the executor's submission queue is not
a complete backpressure policy. See [group ownership](grouped-batching.md),
[cost budgets](cost-aware-batching.md), and [the workload guide](workloads.md).

## Test locally

```bash
uv sync --locked
uv run --no-sync python -m pytest tests/core/test_variable_inference_recipe.py -ra
```

Tests cover immutable input snapshots, zero-token masks, length restoration, conservative padding
budgets, tenant/revision separation, rejected oversized items, malformed outputs, and executor
ownership. Full batches in the cost-budget test use a long collection deadline so correctness
does not depend on machine speed.
