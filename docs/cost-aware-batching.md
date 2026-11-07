# Batch by count and cost

Some bulk operations have a byte, token, or estimated-work budget as well as an item limit.
Configure `max_batch_cost` and `item_cost` together to enforce both limits:

```python
import asyncio

from async_batcher import AsyncBatcher


class ByteBatcher(AsyncBatcher[bytes, int]):
    async def process_batch(self, batch: list[bytes]) -> list[int]:
        # Replace this example with a bulk operation that accepts these payloads.
        return [len(payload) for payload in batch]


async def main():
    batcher = ByteBatcher(
        max_batch_size=100,
        max_batch_cost=1024,
        item_cost=len,
        max_queue_time=0.01,
    )
    try:
        sizes = await asyncio.gather(batcher.process(b"hello"), batcher.process(b"world"))
        assert sizes == [5, 5]
    finally:
        await batcher.stop()


asyncio.run(main())
```

The budget must be a positive, finite real number. `item_cost` must be a synchronous callable
that returns a nonnegative, finite real number; booleans are rejected. Without either option,
batching keeps its existing count and collection-time behavior. `max_batch_size=-1` leaves only
the cost and time limits. Neither option adds a dependency.

Each submission evaluates `item_cost` once, before enqueueing, and keeps that cost for its
lifetime. A callback exception, invalid cost, or cost above the budget rejects only that
submission. Oversized items raise `ValueError`; they are never sent to `process_batch`. A full
queue still raises `QueueFullException`. The callback can run for a rejected submission, so
keep it cheap and free of side effects. It runs on the event-loop thread, not an executor.

Accepted items retain FIFO order. A request that would exceed the current batch budget becomes
the first item of the next batch; it is not moved behind newer requests. It still counts toward
bounded queue capacity, and keeps its original arrival time for the collection deadline. A batch
dispatches when its count or cost limit is reached, the next item would exceed its budget, its
collection deadline expires, or graceful shutdown flushes it. Concurrent batches can complete
out of order, while each caller still receives its own result.

Zero-cost items are valid. Use a count limit if they should not accumulate into a large batch.
Costs use normal Python numeric arithmetic: floating-point rounding can split a batch earlier
than expected, so integer byte/token counts are preferable when available. This is an additive
budget for submitted items, not automatic serialization sizing or a limit on total concurrent
work. Include per-item overhead in your estimator and reserve fixed request overhead by reducing
the configured budget. Do not mutate a payload after submitting it if that would invalidate its
cached cost.

Graceful shutdown drains every accepted item, including an item held for the next batch. Forced
shutdown cancels pending results and clears that retained item before a later restart. Cancelling
a caller does not retract its already accepted item from processing; its cached cost still applies.
The existing concurrency, shutdown, and executor rules also apply.
