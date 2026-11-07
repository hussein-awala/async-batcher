# Async Batcher with Keras model and FastAPI

Use Python 3.11–3.13 for this TensorFlow example. TensorFlow 2.21 does not publish wheels for
Python 3.14 or 3.15; the core library supports those versions independently.
The requirements file installs TensorFlow 2.21+ and Keras 3.15.1+.


This example shows how to use the `async_batcher` library with FastAPI to process data
in batches by a Keras (Tensorflow) model.

In this example, we serve a TensorFlow model using FastAPI and create two endpoints:
- `/predict` to make predictions using the TensorFlow model directly
- `/optimized_predict` to make predictions using the TensorFlow model with the `async_batcher` library

## How to use
```bash
# Install from the repository root
pip install -e . -r examples/keras/requirements.txt

# Run the FastAPI server with Uvicorn
cd examples/keras
uvicorn main:app --reload
```

## Load testing

To evaluate the performance of the `/predict` and `/optimized_predict` endpoints, we can use the `locust` library to
simulate multiple users making requests to the server.

You can run one of the provided Locust files to simulate the load testing:
- [predict_load_test.py](predict_load_test.py) to test the `/predict` endpoint
- [optimized_predict_load_test.py](optimized_predict_load_test.py) to test the `/optimized_predict` endpoint

To run the load test, you can use the following command:
```bash
locust -f predict_load_test.py
```
Then, open the web interface at http://localhost:8089 and start the load test.
