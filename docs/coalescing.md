# Coalesce duplicate in-flight requests

`CoalescingBatcher` lets concurrent callers share one request to a borrowed `AsyncBatcher` when
an explicit key says their inputs are interchangeable. It retains no completed-result cache:
after an operation finishes, the next call for that key submits a new request.

```python
import asyncio

from async_batcher import AsyncBatcher, CoalescingBatcher


async def lookup_many(ids: list[int]) -> list[str]:
    # Replace with your bulk lookup, preserving input order.
    return [f"record-{identifier}" for identifier in ids]


async def main():
    async with AsyncBatcher.from_callable(lookup_many, max_batch_size=16) as batcher:
        async with CoalescingBatcher(
            batcher, key=lambda identifier: identifier, max_keys=64, max_waiters=256
        ) as shared:
            print(await asyncio.gather(shared.process(7), shared.process(7)))


asyncio.run(main())
```

The first input for an in-flight key is submitted. Joiners receive the same result object or
ordinary exception; inputs/results are not copied. Choose a synchronous, hashable key containing
all relevant request options, destination and authorization context. Invalid awaitable keys are rejected. Native coroutines are closed; borrowed Futures/Tasks are
left alive and their late failures observed. Custom awaitables remain callback-owned and are
never driven by this wrapper. Sharing a key is an explicit
claim that the requests may share one execution. This wrapper is not an authorization boundary,
and it should not deduplicate writes that must each execute independently. Do not await a request
for the same key from the processor already handling that key.

## Capacity and admission

`max_keys` (default 128) bounds in-flight underlying requests. `max_waiters` (default 1024) bounds
currently waiting callers across **all** keys, including the first caller of each key. Both must
be positive integers. `in_flight_keys` and `waiting_callers` expose their current counts on the
owning event-loop thread. Limits apply to this wrapper, not other users of its borrowed batcher.

Either full bound raises `QueueFullException` immediately. A full key bound still permits a caller
to join an existing key when waiter capacity is available. Cancelling a caller releases its waiter
slot, but does not release the in-flight key or cancel the underlying request, even when no waiters
remain. A later caller can join that still-active operation.

New keys use the borrowed batcher's normal `process(item)` admission policy. A full underlying
queue therefore fails that shared operation; it is not retried automatically. The wrapper's bounds
and the underlying batcher's queue/concurrency limits control different resources. The underlying
batcher's item statistics count submitted operations, not every coalesced caller.

## Failures and ownership

Ordinary operation failures reach all current joiners. Failures are observed even if every caller
has detached, and are not cached; a later call can retry. Cancellation of one caller does not cancel
its peers. Explicitly force-stopping the borrowed batcher can cancel the shared request and all
joiners; that is different from cancelling an individual caller.

`await shared.aclose()` permanently closes this wrapper, rejects new calls and drains only its
owned underlying request tasks. It does not call `batcher.stop()` or close the batcher's clients.
Request errors stay with their callers and are not re-raised by `aclose()`. A cancelled close waiter
leaves draining alive; await `aclose()` again to join it. No implicit close timeout or force policy
is provided. Use the borrowed batcher's explicit shutdown policy if an operation will not finish.

The async context manager calls `aclose()` on exit, including when the body raises. Keep the
borrowed batcher's context outside the wrapper's context, as above. Nested wrapper context entry
is rejected; a closed wrapper cannot restart. Use one wrapper on one event loop. A processor or its
descendant cannot await this wrapper's `aclose()`, which would otherwise wait on its own request.

Read [grouped batching](grouped-batching.md) for separate batches by compatibility key. Grouping
controls which different inputs may share a batch; coalescing controls which equivalent requests
may share one input operation. Neither is a completed-value cache.
