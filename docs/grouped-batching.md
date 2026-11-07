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
