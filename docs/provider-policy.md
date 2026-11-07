# Compose provider pacing and finite retries

The [provider-policy recipe](../examples/recipes/provider_policy.py) is application-owned policy around
an async bulk call. AsyncBatcher still owns count/cost collection, bounded producer admission, and
batch concurrency. It does not gain automatic retry or rate-limit behavior.

Run a local, credential-free example from the current [source checkout](../README.md#install):

```bash
uv sync --locked --no-default-groups --python 3.12
uv run --no-sync python -m examples.recipes.provider_policy
```

The demo doubles integers and deliberately rejects its first attempt. It reports actual results,
provider attempts, retry attempts, and throttled responses; these are not billing measurements.

## One shared budget for the actual provider attempts

Create one `Pacer(requests_per_second=..., units_per_second=...)` per provider quota scope and share it
across every relevant child/group/caller. A `ProviderPolicy` borrows that pacer and wraps an async
operation with `await policy.run(operation, units=estimated_work, total_timeout=60)`.

The pacer spaces call starts by the larger of `1 / requests_per_second` and
`estimated_work / units_per_second`. The optional units can represent conservatively estimated tokens
or other work. The first call can start immediately; it charges the interval before the next call.
There is no accumulated burst credit. Every attempt spends its interval, including rejected, failed,
and subsequently cancelled calls. A cancelled waiter does not reserve a new slot.

This is conservative application pacing, not a model of every provider's rolling/calendar windows,
actual token accounting, daily limits, or other clients using the same account. Enforce per-request
count/token/byte caps separately and leave headroom for estimation error. An oversized single request
is not made valid by a longer interval. A shared pacer controls starts, not simultaneously running
calls, memory, tenant fairness, or cross-process quotas. Keep bounded maps/queues and shared backend
capacity limits from the [operations guide](operations.md).

## Classify retryable rejections explicitly

The policy catches only `Throttled`. Translate a provider's documented rejection to that type only
when your application permits replay of that operation. It does not catch arbitrary HTTP errors,
transport errors, backend timeouts, cancellation, or failed writes.

For an HTTPX2 client, an application bulk operation can classify a response like this:

```python
from examples.recipes.provider_policy import Pacer, ProviderPolicy, Throttled

pacer = Pacer(requests_per_second=2, units_per_second=1000)
policy = ProviderPolicy(pacer, max_attempts=3, base_delay=0.5, max_delay=30)


async def bulk_call(client, payload, estimated_tokens):
    async def attempt():
        response = await client.post("/your-bulk-endpoint", json=payload)
        if response.status_code == 429:
            raise Throttled(response.headers.get("Retry-After"))
        response.raise_for_status()
        return response.json()

    return await policy.run(attempt, units=estimated_tokens, total_timeout=60)
```

This snippet borrows an already configured client; the endpoint/payload and response-to-input mapping
remain application-specific. Do not treat every 429 from an arbitrary service as proof that no billable
work or side effect occurred. For saved embeddings and replayable vector writes, see the
[durable handoff recipe](durable-embeddings.md); ambiguous writes still need backend-specific recovery.

`max_attempts` includes the first call. Missing or invalid `Retry-After` uses exponential full jitter,
with a ceiling of `max_delay`. A valid integer-seconds or HTTP-date header supplies a minimum delay,
following [HTTP Retry-After semantics](https://www.rfc-editor.org/rfc/rfc9110.html#name-retry-after).
The policy uses the larger of that minimum and local jitter. Normal cooldowns extend the shared pacer,
so other waiting calls recheck them too, even after the last allowed attempt. Already running calls
cannot be recalled. An HTTP-date depends on the machine's wall clock being reasonably synchronized.

If a server requests more than `max_delay`, the call fails immediately with `Throttled`; its request
is never retried sooner by shortening that header to the cap. Stop or defer application intake as
appropriate for such a response; this refusal does not install a permanent account-wide circuit breaker.
The `Throttled` message omits provider payloads, but its header attribute and caller-owned client
exceptions still need your logging policy.

`total_timeout` covers pacing, retries, backoff, and the async operation together. It relies on
cooperative cancellation and is not a hard process deadline. Client-native I/O timeouts remain useful.
Cancelling during backoff sends no new attempt; cancelling after a provider call starts does not undo
its work. `attempts`, `retry_attempts`, and `throttled_responses` count actual entered attempts across
this policy instance, not map results, all SDK-internal requests, or charged tokens.

## Give retries one owner

For this HTTPX2 pattern, configure `AsyncHTTPTransport(retries=0)` so transport attempts are not hidden
inside the application retry loop. Keep client contexts outside the batcher and finish batcher drain
before closing the client. Bound the total operation budget as well as individual I/O budgets.

An SDK may retry by itself. The [Cohere/Pinecone recipe](cohere-pinecone.md) already documents Pinecone's
native retries. Keep those SDK retries as their sole owner, or explicitly configure/verify the SDK to
perform one attempt before adding application retries. Do not blindly wrap a retrying SDK and multiply
its retry count. This recipe changes neither that provider example nor any adapter's retry policy.

```bash
uv sync --locked
uv run --no-sync python -m pytest tests/core/test_provider_policy_recipe.py -q
uv sync --locked --group example-test --extra pinecone
uv run --no-sync python -m pytest tests/provider_recipes/test_provider_policy.py -q
```

The HTTP tests start local-only servers with dummy data. They exercise real 429 responses, identical
retry payloads, server minimum delays, and cancellation during backoff without credentials or paid calls.
