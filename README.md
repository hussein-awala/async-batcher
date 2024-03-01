# Async Batcher

This project provides a Python library to batch the asynchronous requests and handle them in batches.

## How to use

To use the library, you need to install the package in your environment. You can install the package using pip:

```bash
pip install async-batcher
```

Then, you can create a new `AsyncBatcher` class by implementing the `process_batch` method:

```python
from async_batcher.batcher import AsyncBatcher

class MyAsyncBatcher(AsyncBatcher):
    async def process_batch(self, batch):
        # Process the batch
        print(batch)

# Create a new instance of the `MyAsyncBatcher` class
async_batcher = MyAsyncBatcher(max_batch_size=20)
async_batcher.start()
```

## Benchmark

The benchmark is available in the [BENCHMARK.md](https://github.com/hussein-awala/async-batcher/blob/main/BENCHMARK.md)
file.

## Use cases

The `AsyncBatcher` library can be used in any application that needs to handle asynchronous requests in batches,
such as:
- Serving machine learning models that optimize the batch processing (e.g. TensorFlow, PyTorch, Scikit-learn, etc.)
- Storing multiple records in a database in a single query to optimize the I/O operations (or to reduce the cost of the
  database operations, e.g. AWS DynamoDB)
- Sending multiple messages in a single request to optimize the network operations (or to reduce the cost of the network
  operations, e.g. Kafka, RabbitMQ, AWS SQS, AWS SNS, etc.)

