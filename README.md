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
async_batcher = MyAsyncBatcher(batch_size=20)
async_batcher.start()
```

## Benchmark

To evaluate the performance of the `AsyncBatcher` library, we used the [Keras example](examples/keras)
with [locust](https://locust.io/) to simulate multiple users making requests to the server.

In this example, we have a tensorflow model that takes ~11ms to make a single prediction and ~13ms to process
a batch of 200 predictions.

Predict endpoint:

![predict__total_requests_per_second.png](assets%2Fpredict__total_requests_per_second.png)

Optimized Predict endpoint:

![optimized_predict__total_requests_per_second.png](assets%2Foptimized_predict__total_requests_per_second.png)

As we can see from the graphs, for the `/predict` endpoint, the server was able to handle ~15 simultaneous requests per
second, where ~130 requests/second were failing. For the `/optimized_predict` endpoint, the server was able to handle
all the requests (~170 requests/second) without any failure, with a response time of ~49ms in average and ~62ms in
95th percentile.

## Use cases

The `AsyncBatcher` library can be used in any application that needs to handle asynchronous requests in batches,
such as:
- Serving machine learning models that optimize the batch processing (e.g. TensorFlow, PyTorch, Scikit-learn, etc.)
- Storing multiple records in a database in a single query to optimize the I/O operations (or to reduce the cost of the
  database operations, e.g. AWS DynamoDB)
- Sending multiple messages in a single request to optimize the network operations (or to reduce the cost of the network
  operations, e.g. Kafka, RabbitMQ, AWS SQS, AWS SNS, etc.)

