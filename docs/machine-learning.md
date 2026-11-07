# Prediction adapters

These adapters use the [current checkout](../README.md#install) and accept the
[core limits and executor options](api.md). Submit one sample per `process()` call. Both
invoke synchronous `model.predict` in an executor and return the corresponding prediction.
The model remains caller-owned. Keep the default `concurrency=1` unless your model and backend
can safely predict concurrently; more threads do not necessarily improve throughput.

## Keras

The `keras` extra requires Keras 3.15.1 or newer and a separately installed backend. Repository
tests use JAX; select it before importing Keras:

```bash
uv sync --locked --extra keras --group ml-test
export KERAS_BACKEND=jax
```

Save and run this example with `uv run --no-sync python`:

```python
import asyncio

import keras
from async_batcher.ml.keras.model import KerasAsyncBatcher


async def main():
    inputs = keras.Input(shape=(2,))
    model = keras.Model(inputs, keras.layers.Rescaling(2.0)(inputs))
    batcher = KerasAsyncBatcher(model=model, max_batch_size=32)
    try:
        predictions = await asyncio.gather(batcher.process([1.0, 2.0]), batcher.process([3.0, 4.0]))
        assert predictions[0].tolist() == [2.0, 4.0]
        assert predictions[1].tolist() == [6.0, 8.0]
    finally:
        await batcher.stop()


asyncio.run(main())
```

Inputs are combined with `numpy.asarray(batch)` along a new leading batch axis. Supply one dense
sample with consistent shape, not an already batched array. Multiple-input, ragged, and custom
input structures require a custom `process_batch` implementation.

Tensor outputs are split along their leading batch axis. Dictionary, list, and tuple outputs
retain their nested structure for each caller; every tensor leaf must have one row per input.
A malformed output fails the entire batch.

Keras also supports TensorFlow and PyTorch; follow its [backend installation guide](https://keras.io/getting_started/)
and validate your model on the chosen backend. Keras 2 is unsupported. Prefer the native `.keras`
model format for new models. The bundled legacy HDF5 example loads with `compile=False`, and
its inference path is tested with JAX; custom layers and other backends need separate validation.
See the [Keras 3 migration guide](https://keras.io/guides/migrating_to_keras_3/).

Runnable application examples: [FastAPI](../examples/keras/README.md) and
[gRPC](../examples/keras_with_grpc/README.md).

## scikit-learn

Run `uv sync --locked --extra sklearn`. Supply a fitted estimator with a `predict` method:

```python
import asyncio

from async_batcher.ml.sklearn.model import SklearnAsyncBatcher
from sklearn.linear_model import LinearRegression


async def main():
    model = LinearRegression().fit([[0.0], [1.0], [2.0]], [0.0, 2.0, 4.0])
    batcher = SklearnAsyncBatcher(model=model, max_batch_size=32)
    try:
        predictions = await asyncio.gather(batcher.process([3.0]), batcher.process([4.0]))
        assert abs(predictions[0] - 6.0) < 1e-8
        assert abs(predictions[1] - 8.0) < 1e-8
    finally:
        await batcher.stop()


asyncio.run(main())
```

Dense feature rows are passed as a batch. If any input is a SciPy sparse vector or single-row
matrix, all inputs are stacked as CSR without densifying sparse data; dense rows can be mixed
into that batch. Each input must represent exactly one sample. Sparse predictions remain CSR
rows with shape `(1, n_outputs)`; dense predictions follow the estimator's per-sample output shape.
Missing `predict`, invalid shapes, and prediction errors fail the batch. Training and model
mutation remain application responsibilities; finish them before submitting predictions.
