# AsyncBatcher - Asynchronous Batching for Python

## Overview
AsyncBatcher is a generic, asynchronous batch processor for Python that efficiently groups incoming items into batches
and processes them asynchronously. It is designed for scenarios where multiple requests or tasks need to be handled in
batches to improve efficiency and throughput.

## Key Features
- Asynchronous processing: Uses asyncio for non-blocking execution.
- Batching mechanism: Groups items into batches based on size or time constraints.
- Concurrency control: Limits the number of concurrent batch executions.
- Custom processing logic: Users must implement the `process_batch` method to define batch behavior.
- Queue management: Uses an `asyncio.Queue` to manage incoming items.
- Error handling: Ensures robust error reporting and handling.

## How it works

### 1. Receiving Items for Processing
- Users call `process(item)`, which adds the item to an internal queue.
- `process(item)` is a coroutine: awaiting it enqueues the item and waits for its result.
- Use `asyncio.gather()` or `asyncio.create_task()` to submit multiple items concurrently.

### 2. Queue Management and Batching
- A background task (`run()`) continuously monitors the queue.
- Items are collected into batches based on:
  - `max_batch_size`: Maximum items per batch.
  - `max_queue_time`: Time to collect a partial batch, measured from its oldest queued item.
    Busy processors can delay dispatch beyond this limit; queued items do not receive a fresh waiting period.
- Once a batch is ready, it is passed to the processing function.

### 3. Processing the Batch
- If `process_batch` is asynchronous, it is awaited directly.
- If `process_batch` is synchronous, it runs inside an `Executor`.
- Each item’s future is resolved with the corresponding processed result.

### 4. Concurrency Control
- If `concurrency > 0`, a semaphore ensures that only a limited number of batches are processed simultaneously.
- Otherwise, all batches run concurrently.

### 5. Stopping the Batcher
- `await stop()` immediately rejects new items, flushes partial batches, and waits for queued and active work.
- `await stop(timeout=seconds)` raises `asyncio.TimeoutError` if shutdown takes longer; accepted work continues draining.
  You can await `stop()` again or force cancellation. Canceling a caller awaiting `stop()` also leaves draining intact.
- `await stop(force=True)` cancels pending requests and waits for batch-task cleanup. It is terminal and repeatable.
  Synchronous functions already running in an executor cannot be interrupted and may still finish afterward.
- Canceling one `process()` caller does not prevent other items in its batch from receiving results.
  Already accepted work may still execute, so cancellation does not guarantee that side effects are prevented.

```mermaid
sequenceDiagram
    participant User
    participant AsyncBatcher
    participant Queue as asyncio.Queue
    participant RunLoop
    participant Semaphore
    participant BatchProcessor

    User->>AsyncBatcher: process(item)
    activate AsyncBatcher
    AsyncBatcher->>Queue: put(QueueItem(item, future))
    Note over User,AsyncBatcher: process() awaits the item result
    deactivate AsyncBatcher

    Note over AsyncBatcher: Starts RunLoop on first process()

    loop Run Loop (run() method)
        RunLoop->>Queue: Collect items (max_batch_size/max_queue_time)
        activate Queue
        Queue-->>RunLoop: Batch [QueueItem1, QueueItem2...]
        deactivate Queue

        alt Concurrency Limited (concurrency > 0)
            RunLoop->>Semaphore: acquire()
            activate Semaphore
            Semaphore-->>RunLoop: acquired
            deactivate Semaphore
        end

        RunLoop->>BatchProcessor: create_task(_batch_run(batch))
        activate BatchProcessor

        alt Async process_batch
            BatchProcessor->>AsyncBatcher: await process_batch(batch)
        else Sync process_batch
            BatchProcessor->>Executor: run_in_executor(process_batch)
        end

        AsyncBatcher-->>BatchProcessor: results [S1, S2...]
        BatchProcessor->>QueueItem1.future: set_result(S1)
        BatchProcessor->>QueueItem2.future: set_result(S2)
        deactivate BatchProcessor

        alt Concurrency Limited
            RunLoop->>Semaphore: release()
        end
    end

    Note over User, BatchProcessor: User's await future gets resolved
```

## How to use

To use the library, you need to install the package in your environment. You can install the package using pip:

```bash
pip install async-batcher
```

Then, you can create a new `AsyncBatcher` class by implementing the `process_batch` method:

```python
import asyncio
import logging

from async_batcher.batcher import AsyncBatcher

class MyBatchProcessor(AsyncBatcher[int, int]):
    async def process_batch(self, batch: list[int]) -> list[int]:
        await asyncio.sleep(1)  # Simulate processing delay
        return [x * 2 for x in batch]  # Example: Doubling each item

async def main():
    batcher = MyBatchProcessor(max_batch_size=5, max_queue_time=2.0, concurrency=2)
    results = await asyncio.gather(*[batcher.process(i) for i in range(10)])
    print(results)  # Output: [0, 2, 4, 6, 8, 10, 12, 14, 16, 18]
    await batcher.stop()

# Set logging level to DEBUG if you want to see more details and understand the flow
logging.basicConfig(level=logging.DEBUG)
asyncio.run(main())
```

## Configuration and results

- `max_batch_size` accepts a positive integer (including `1`) or `-1` for no limit.
- `concurrency` accepts a positive integer or `-1` for no limit.
- `max_queue_time` must be finite and nonnegative; `0` dispatches available items without a collection delay.
- `max_queue_size` limits buffered items; `-1` and `0` mean unlimited. A full queue raises `QueueFullException`.
- Each processor returns one result per input, in the same order. Returning `None` resolves every item with `None`.
  An exception raised by the processor fails the entire batch; an exception in the result list fails only that item.
- A batcher belongs to one event loop and cannot be restarted after stopping. Prefer `try/finally` to ensure
  `await batcher.stop()` runs before closing application resources. Call `stop()` from outside `process_batch`.
  Invalid limits and unknown options raise during construction rather than being silently accepted.

## Benchmark

The benchmark is available in the [BENCHMARK.md](https://github.com/hussein-awala/async-batcher/blob/main/BENCHMARK.md)
file.

## When to Use AsyncBatcher?
The AsyncBatcher library is ideal for applications that need to efficiently handle asynchronous requests in batches,
such as:

### Machine Learning Model Serving
- Batch-processing requests to optimize inference performance (e.g., TensorFlow, PyTorch, Scikit-learn).

### Database Bulk Operations
- Inserting multiple records in a single query to improve I/O efficiency and reduce costs (e.g., PostgreSQL, MySQL,
  AWS DynamoDB). 

### Messaging and Network Optimization
- Sending multiple messages in a single API call to reduce latency and costs (e.g., Kafka, RabbitMQ, AWS SQS, AWS SNS).

### Rate-Limited API Calls
- Aggregating requests to comply with API rate limits (e.g., GitHub API, Twitter API, OpenAI API).

## Final Notes
- Implement `process_batch` according to your needs.
- Ensure `max_batch_size` and `max_queue_time` are configured based on performance requirements.
- Return per-item exceptions when only individual items should fail; otherwise a raised exception fails the batch.
