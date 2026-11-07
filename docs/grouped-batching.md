# Group compatible requests

`GroupedBatcher` routes each item to a child batcher chosen by a synchronous,
hashable `group_key`. Use separate groups for different models, destinations,
or processing options. A child only receives items with the same key.

```python
import asyncio
from async_batcher import AsyncBatcher, GroupedBatcher


def make_batcher(destination: str) -> AsyncBatcher[tuple[str, int], int]:
    async def write_many(items: list[tuple[str, int]]) -> list[int]:
        # Replace with the destination's real bulk operation.
        assert all(item[0] == destination for item in items)
        return [item[1] for item in items]

    return AsyncBatcher.from_callable(write_many, max_batch_size=100)


async def main():
    async with GroupedBatcher(
        make_batcher, group_key=lambda item: item[0], max_groups=16, idle_timeout=30
    ) as grouped:
        print(
            await asyncio.gather(
                grouped.process(("orders", 1)),
                grouped.process(("events", 2)),
                grouped.process(("orders", 3)),
            )
        )


asyncio.run(main())
```

The factory runs on the event loop and must return a fresh, unused `AsyncBatcher`
for each key. It can return an adapter or use `from_callable()` as above. Both
callbacks must be synchronous and quick. Reusing a child across keys is rejected.
Groups isolate batches, not credentials or authorization; choose keys that fully
capture compatibility requirements.

## Resource bounds

`max_groups` is a positive integer (default 128). It counts children that are
processing, idle, or still shutting down. When the cap is reached, the least
recently idle group is stopped before its slot is reused. If every group has
outstanding work, `process()` raises `GroupLimitError`; it does not queue an
unbounded number of new groups. Applications can wait and resubmit or reject.

`idle_timeout` is a finite nonnegative number of seconds (default 60); zero
starts cleanup as soon as a group becomes idle. Idle groups are automatically
stopped and removed. A new request for a group currently closing waits for that
cleanup before creating a replacement. Cancellation of that wait leaves cleanup
running. `group_count` includes children still closing.

Batch size, queue size, and concurrency are configured by the factory **per
group**. With 16 groups and child concurrency 2, up to 32 batches may run. Use
finite child queue limits as well as the group cap to bound application memory.

## Cancellation and shutdown

Cancelling a grouped caller stops its wait; its child request continues and keeps
the group busy until it settles. This prevents eviction from losing track of
accepted work. It does not undo side effects. This differs from cancelling a
direct `AsyncBatcher.process()` caller, which cancels its result future.

`await grouped.stop()` permanently closes the manager, rejects new submissions,
and drains all children. A custom child shutdown failure leaves its group tracked
and keeps its capacity occupied; a later `stop()` can retry cleanup. `stop(force=True)` requests child cancellation and waits
for cleanup. A later forced call can escalate an ongoing graceful stop. Timeouts
and cancellation of stop waiters leave shutdown running; await `stop()` again to
join it. Running thread functions retain the core's cancellation limitations.

The async context manager drains on exit, including after a body exception, and
does not suppress that exception. Nested entry is rejected. Unlike an individual
batcher, a stopped group manager cannot restart: create a new manager. Factories
and their clients remain caller-owned; close shared clients after the manager.
Do not stop the manager from a child processor or its descendant tasks.

## Flush accepted child work without closing the manager

The current source checkout provides `await grouped.flush(timeout=None)`. It dispatches
partial batches and waits for work already accepted by every currently owned child,
including children already closing. Intake stays open. Each child's acceptance boundary
is captured when the grouped flush coroutine starts, before its child waiters are scheduled.
Later submissions to existing or new keys cannot extend those boundaries.

```python
await grouped.flush(timeout=5)
```

This is the same acceptance boundary as [core flush](api.md#flush-without-stopping-intake).
Manager reservations still waiting for group eviction or child queue admission are **excluded**,
even if `grouped.pending_items` already counts them. Scheduling a `process()` coroutine also
does not prove child acceptance. A flush does not create groups, cancel eviction, or free group
slots early. It keeps references to its selected children while waiting; replacement children
and later work do not enter the boundary. Use `stop()` to close intake and drain the manager's
owned requests, including admission waits, when ending the service lifecycle.

Completion includes child batch cleanup and completion hooks, even if a grouped caller stopped
waiting. It does not assert that each request succeeded or that an external write is durable;
inspect the individual results or `map_outcomes()` for failures. Ordinary item errors do not
fail flush. `timeout` must be finite and nonnegative; its expiration raises `TimeoutError`.
Timeout or cancellation abandons only this flush wait and leaves accepted work running.
Forced shutdown of an included child interrupts flush with `RuntimeError`, including if that
shutdown occurs between snapshotting and starting the child waiters. Other child work is not
cancelled by this interruption. Graceful shutdown can finish a flush normally.

An empty flush binds the manager's event loop without creating children. A flush after completed
shutdown returns immediately; it does not reopen the manager. A processor or its active
descendants cannot flush their own manager because that would wait for themselves.

## Bounded grouped streams and child admission

`grouped.process(item, wait_for_space=True, enqueue_timeout=0.5)` forwards those admission
options to the selected child. The timeout covers that child's queue admission only, not group
eviction or processing. By default a full child queue still raises `QueueFullException`. If all
group slots are busy, a new key still raises `GroupLimitError` immediately even when waiting
admission is enabled; this option does not create a queue of unbounded new groups.

`grouped.map(source, max_pending=128, ordered=True)` and `grouped.map_outcomes(...)` provide the
same bounded input consumption, ordering and source cleanup as the corresponding
[core stream methods](bulk-processing.md). They use waiting admission for child queues. A busy
group limit fails `map()` promptly and becomes a correlated error in `map_outcomes()`. Choose
`max_groups` for the number of concurrently active keys or handle rejected outcomes explicitly.

Use `contextlib.aclosing` when breaking early. Closing or cancelling a grouped stream closes its
source and joins its own mapping tasks, but leaves the manager and unrelated users open. Once
a request is handed to a child, the group manager keeps owning its child task through completion,
including an admission wait. Thus cancellation does not withdraw that child submission: it may
still be admitted and executed later. Stop the group manager to drain or force-stop all its work.

## Aggregate accepted-item cap

Set `max_pending_items` to a positive integer to bound owned submissions across all groups;
`None` (the default) leaves this aggregate bound disabled. `pending_items` reports the current
count. A reservation starts when the manager accepts a call, before key lookup/group eviction,
and continues through child admission and processing until that child task completes. These
manager reservations are not the child's accepted-item statistics. Pre-child failure or
cancellation returns its reservation; a cancelled caller with an owned child does not.

An exhausted aggregate cap raises `QueueFullException` immediately, even with
`wait_for_space=True`. Waiting remains child-queue-only; `enqueue_timeout` does not wait for
an aggregate reservation. In a grouped `map_outcomes`, rejected reservations are ordinary
indexed failures; in `map`, they terminate iteration. Set the cap for all concurrent users,
not just one map's window, or explicitly handle overload. `max_groups` still independently
bounds child instances. Closing/cancelling a map does not free permits for its still-owned
child requests; `stop()` drains those requests and `stop(force=True)` requests cancellation.
