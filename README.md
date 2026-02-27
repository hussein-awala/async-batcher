# AsyncBatcher - Asynchronous Batching for Python

## Overview
AsyncBatcher is a generic, asynchronous batch processor for Python that efficiently groups incoming items into batches
and processes them asynchronously. It is designed for scenarios where multiple requests or tasks need to be handled in
batches to improve efficiency and throughput.

## Key Features
- Generic typing: `AsyncBatcher[T, S]` where `T` is the input type and `S` is the output type.
- Asynchronous processing: Uses asyncio for non-blocking execution.
- Flexible `process_batch`: Supports both async and sync implementations (sync runs in an `Executor`).
- Batching mechanism: Groups items into batches based on size or time constraints.
- Concurrency control: Limits the number of concurrent batch executions via semaphore.
- Queue management: Uses an `asyncio.Queue` with optional capacity limits (`max_queue_size`).
- Error handling: Exceptions from `process_batch` are propagated to individual item futures.

## How it works

### 1. Receiving Items for Processing
- Users call `process(item)`, which adds the item to an internal queue.
- A `Future` object is returned immediately and the result is awaited asynchronously.

### 2. Queue Management and Batching
- A background task (`run()`) continuously monitors the queue.
- Items are collected into batches based on:
  - `max_batch_size`: Maximum items per batch (-1 for unlimited).
  - `max_queue_time`: Maximum time an item can wait before being processed.
- The queue can be bounded via `max_queue_size` — when full, `process()` raises `QueueFullException`.
- Once a batch is ready, it is passed to the processing function.

### 3. Processing the Batch
- If `process_batch` is asynchronous, it is awaited directly.
- If `process_batch` is synchronous, it runs inside an `Executor` (configurable, defaults to the asyncio default executor).
- Each item's future is resolved with the corresponding result.
- If `process_batch` raises an exception, it is propagated to all futures in that batch.

### 4. Concurrency Control
- If `concurrency > 0`, a semaphore ensures that only a limited number of batches are processed simultaneously.
- Otherwise, all batches run concurrently.

### 5. Stopping the Batcher
- Calling `stop(force=True)` cancels all ongoing tasks immediately.
- Calling `stop(force=False)` waits for pending items to be processed before shutting down.
- An optional `timeout` parameter limits how long the graceful shutdown waits.

```mermaid
sequenceDiagram
    participant User
    participant AsyncBatcher
    participant Queue as asyncio.Queue
    participant RunLoop as RunLoop (run())
    participant Semaphore
    participant BatchTask as BatchTask (_batch_run)
    participant Executor

    User->>AsyncBatcher: process(item)
    activate AsyncBatcher
    AsyncBatcher->>Queue: put_nowait(QueueItem(item, future))
    AsyncBatcher-->>User: awaits future
    deactivate AsyncBatcher

    Note over AsyncBatcher: Starts RunLoop on first process() call

    loop Run Loop
        RunLoop->>Queue: _fill_batch_from_queue(max_batch_size, max_queue_time)
        activate Queue
        Queue-->>RunLoop: batch [QueueItem1, QueueItem2...]
        deactivate Queue

        alt Concurrency Limited (concurrency > 0)
            RunLoop->>Semaphore: acquire()
            activate Semaphore
            Semaphore-->>RunLoop: acquired
            deactivate Semaphore
        end

        RunLoop->>BatchTask: create_task(_batch_run(batch))
        activate BatchTask
        Note over RunLoop: RunLoop continues immediately<br/>to collect next batch

        alt Async process_batch
            BatchTask->>AsyncBatcher: await process_batch(batch_items)
            AsyncBatcher-->>BatchTask: results [S1, S2...]
        else Sync process_batch
            BatchTask->>Executor: run_in_executor(process_batch, batch_items)
            Executor-->>BatchTask: results [S1, S2...]
        end

        alt Success
            BatchTask->>User: future.set_result(S1), future.set_result(S2)...
        else Exception
            BatchTask->>User: future.set_exception(error)
        end

        alt Concurrency Limited
            BatchTask->>Semaphore: release()
        end
        deactivate BatchTask
    end

    Note over User: User's awaited future resolves with result
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
- Exceptions raised by `process_batch` are propagated to all futures in that batch, but do not affect other batches.