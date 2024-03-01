# Benchmark

## v0.1.0

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

## v0.2.0
In the version 0.2.0, we replaced the threading implementation with a full asyncio implementation, and we added support
for multiple concurrent batch processing.

These two changes improved the performance of the `AsyncBatcher` package, and to evaluate the performance, we re-ran the
load tests with the new version, the new configuration, and a faster increase in the number of requests.
```python
from concurrent.futures import ThreadPoolExecutor

executor = ThreadPoolExecutor(max_workers=12)
batcher = MlBatcher(
    model=model,
    max_queue_time=0.01,
    concurrency=12,
    executor=executor
)
```
And the results were impressive:

![optimized_predict__total_requests_per_second_v0.2.0.png](assets%2Foptimized_predict__total_requests_per_second_v0.2.0.png)

The average response time was increasing very slowly with the increase of the RPS, and it reached ~96ms in average at
the end of the test with almost ~500 requests/second (-20% of the first version), with a 95th percentile response time
almost stable and smaller than 300ms for the whole test (-40% of the first version).
