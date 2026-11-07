# Batch an existing function

These APIs describe this repository checkout; see [installation](../README.md#install).
`AsyncBatcher.from_callable()` accepts a bulk function and all normal batcher options.
There is no need to subclass when you already have a function that processes a list.

```python
import asyncio
from async_batcher import AsyncBatcher


async def double_many(values: list[int]) -> list[int]:
    return [value * 2 for value in values]


async def main():
    async with AsyncBatcher.from_callable(double_many, max_batch_size=8) as batcher:
        results = await asyncio.gather(*(batcher.process(i) for i in range(20)))
        print(results)


asyncio.run(main())
```

The function receives one positional list argument. Async functions, bound async
methods, partials of async functions, and objects with an async `__call__` execute
on the event loop. Regular functions execute in the configured thread executor,
with the same context propagation as synchronous subclass processors. If a regular
function returns an awaitable (for example, a decorator wraps an async function),
the wrapper awaits that result on the event loop. Async generators are not supported.

Return one result per input in order, or `None` for a write-only operation. Existing
[error and result rules](api.md#results-and-errors) apply, including per-item exceptions.
The wrapper owns neither an explicitly provided executor nor clients used by your function.

## Managed lifecycle

Every batcher, including existing subclasses and adapters, supports `async with`.
Entering binds the event loop but does not start processing. Exiting drains accepted
work even if the body raised an exception or was cancelled, then propagates that
exception. It does not retry failed requests or roll back external writes. Await
request tasks to observe their errors; shutdown itself does not re-raise per-item failures.

Nested or concurrent context entries for the same batcher are rejected. A completed
context can be entered again on the same event loop. Exit has no implicit timeout;
for custom timeout or forced cancellation policies, use explicit
[`stop()` calls](api.md#lifecycle). If the exit wait itself is cancelled, accepted
work continues draining under the existing `stop()` contract.

Keep client or executor contexts outside the batcher context, so resources remain
available until its work has finished. As with explicit shutdown, do not exit the
batcher's context from its own processor or one of that processor's child tasks.
