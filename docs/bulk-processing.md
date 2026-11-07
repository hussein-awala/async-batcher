# Bulk and streaming producers

`process(item)` is convenient when concurrent callers already exist, such as HTTP handlers.
For a list, generator, or async source, `map()` manages a bounded group of requests:

```python
import asyncio
from contextlib import aclosing

from async_batcher import AsyncBatcher


class DoubleBatcher(AsyncBatcher[int, int]):
    async def process_batch(self, batch: list[int]) -> list[int]:
        return [item * 2 for item in batch]


async def main():
    batcher = DoubleBatcher(max_batch_size=32, max_queue_size=64)
    try:
        async with aclosing(batcher.map(range(1000), max_pending=128)) as results:
            async for result in results:
                print(result)
                if result >= 20:
                    break
    finally:
        await batcher.stop()


asyncio.run(main())
```

## Consumption and result order

`batcher.map(source, *, max_pending=128, ordered=True)` returns an async iterator.
The source may be an `Iterable` or `AsyncIterable`; it is consumed only after iteration starts.
`max_pending` must be a positive integer. It bounds items consumed from this source whose
results have not yet been yielded, including items waiting for queue space and completed
results waiting behind an earlier result. A slow first result or a paused consumer therefore
cannot cause unbounded prefetching. Other users of the batcher have their own independent work.

The default yields in input order. `ordered=False` yields in completion order; simultaneous
completions have no guaranteed relative order. A slow async source does not delay results
that are already available. Synchronous iterators run on the event-loop thread, so their
`__next__` methods must not perform blocking I/O.

Map uses waiting queue admission, so a bounded queue pauses its submissions instead of raising
`QueueFullException`. `max_pending` bounds source consumption; `max_queue_size` bounds the
batcher's shared queue; `concurrency` bounds active batches. They control different resources.
Choose a pending window large enough to fill the intended batch. A smaller window still works,
but partial batches may wait for the collection delay before dispatching.

## Failures, cancellation, and ownership

Iteration is fail-fast: a source error or any request error is raised on the next iteration
step as soon as it is observed, even if an earlier ordered result is still pending. This can
omit successful results already buffered. A per-request `CancelledError` cancels iteration.
Use individual `process()` calls when you need to collect and handle every item's outcome.

When iteration finishes, fails, is cancelled during an iteration step, or is explicitly closed,
map cancels and joins its own feeder/request tasks and closes the source iterator if it exposes
`close()` or `aclose()`. It does not stop the shared batcher or cancel unrelated callers.
The source iterator belongs to that map operation; do not consume or close it concurrently.
Source cleanup must cooperate with cancellation; a source that refuses to finish cleanup can
delay closing the map.

A plain `break` does not guarantee immediate async-generator cleanup. Use
`contextlib.aclosing(batcher.map(...))`, as above, or call `await results.aclose()` in a `finally`
block, including when consumer code raises outside an iteration step. Accepted work may still
execute after request cancellation, so early closure is not a rollback of external writes.
Stop the batcher separately when all of its users are finished.

## Waiting for queue capacity

The existing `await batcher.process(item)` behavior is unchanged: a full queue raises
`QueueFullException` immediately. To wait for capacity instead:

```python
result = await batcher.process(item, wait_for_space=True, enqueue_timeout=0.5)
```

`enqueue_timeout` is optional, finite, and nonnegative, and requires `wait_for_space=True`.
It limits only time waiting for queue admission; it is not a result timeout or a batching
collection deadline. Zero accepts immediately available capacity and otherwise raises
`TimeoutError`. Without a timeout, admission waits until space is available or shutdown starts.

A cancelled or timed-out admission attempt never enters the queue. Shutdown wakes blocked
producers with `RuntimeError("Batcher is stopping")`; those attempts cannot slip into a later
restart. Once accepted, existing result/cancellation semantics apply. Waiting producers are
not included in the shared queue limit. Avoid creating an unbounded number of them: use `map()`
for a bounded producer. Waiting admission does not promise priority over other submissions.
