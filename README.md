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

For this benchmark, we ran the FastAPI server with Uvicorn with 12 workers,
on a MacBook Pro with Apple M2 Max chip (12 cores).

Predict endpoint:

![predict__total_requests_per_second.png](assets%2Fpredict__total_requests_per_second.png)

Optimized Predict endpoint:

![optimized_predict__total_requests_per_second.png](assets%2Foptimized_predict__total_requests_per_second.png)

As we can see from the graphs, for the `/predict` endpoint, the failure rate started to increase linearly at the RPS
of ~284 requests/second, and the 95th percentile response time was ~1100ms at this point. Then with the increase of
the RPS, the number of successful requests was ~130 requests/second in average, with a high failure rate
(~300 from ~400 requests/second).

For the `/optimized_predict` endpoint, the failure was smaller than 3 requests/second (0.00625%) during the whole test,
the average response time was increasing slightly with the increase of the RPS, and it reached ~120ms in average at
the end of the test (> 480 requests/second), with a 95th percentile response time almost stable and smaller than 500ms.

You can find check the reports for more details:
- [predict__report.html](assets/predict__report.html)
- [optimized_predict__report.html](assets/optimized_predict__report.html)
## Use cases

The `AsyncBatcher` library can be used in any application that needs to handle asynchronous requests in batches,
such as:
- Serving machine learning models that optimize the batch processing (e.g. TensorFlow, PyTorch, Scikit-learn, etc.)
- Storing multiple records in a database in a single query to optimize the I/O operations (or to reduce the cost of the
  database operations, e.g. AWS DynamoDB)
- Sending multiple messages in a single request to optimize the network operations (or to reduce the cost of the network
  operations, e.g. Kafka, RabbitMQ, AWS SQS, AWS SNS, etc.)

