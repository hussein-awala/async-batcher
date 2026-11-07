# Keras inference with FastAPI

The example loads the bundled [HDF5 model](../diabetes_tf_model.h5) with `compile=False` and
accepts one sample containing ten numbers. `/predict` offloads a single prediction to a thread;
`/optimized_predict` combines concurrent requests through `MlBatcher`. Both return one JSON number.
The application drains the batcher on shutdown.

Use Python 3.12 on a platform with a TensorFlow wheel. The requirements select TensorFlow 2.21+
and Keras 3.15.1+. TensorFlow 2.21's [published wheels](https://pypi.org/project/tensorflow/2.21.0/#files)
cover Python 3.10–3.13 (this project requires 3.11+), not 3.14/3.15, and exclude Intel macOS.
The core package supports newer Python independently. See the [Keras guide](../../docs/machine-learning.md#keras)
for backend-neutral inference with JAX.

## Install and run

From the repository root:

```bash
uv venv --python 3.12 examples/keras/.venv
uv pip install --python examples/keras/.venv/bin/python -e '.[keras]' -r examples/keras/requirements.txt
cd examples/keras
KERAS_BACKEND=tensorflow .venv/bin/python -m uvicorn main:app --reload
```

This environment is separate from the locked repository environment. Its requirements are ranges,
not `uv.lock`. Invoke the example interpreter directly to retain its additional packages. On
Windows, use `.venv/Scripts/python.exe` and set `KERAS_BACKEND=tensorflow` before starting Python.

From another terminal:

```bash
curl --fail-with-body -X POST http://127.0.0.1:8000/optimized_predict \
  -H 'Content-Type: application/json' \
  -d '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0]'
```

Use `/predict` for the unbatched endpoint. Multiple concurrent requests are needed to exercise
batching. Stop the server with Ctrl+C to run its graceful shutdown handler.

## Load testing

Keep the server running, then run one of these commands from `examples/keras` in another terminal:

```bash
.venv/bin/python -m locust -f predict_load_test.py --host http://127.0.0.1:8000
# Or test the batched endpoint:
.venv/bin/python -m locust -f optimized_predict_load_test.py --host http://127.0.0.1:8000
```

Open `http://localhost:8089` and choose the user count and spawn rate. Test each endpoint separately
with the same settings. For measurements, omit Uvicorn's `--reload`, choose an explicit worker
count, warm up the model, and record dependencies/backend versions. Each server worker has its
own model, queue, and executor; batches do not span workers.

The provided clients use one-second connection/read timeouts and random ten-feature samples.
Their results describe that workload, not model quality or a universal throughput target.
The [benchmark page](../../BENCHMARK.md) contains historical reports, not results from this checkout.
