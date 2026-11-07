from __future__ import annotations

import asyncio
from unittest.mock import MagicMock

import pytest

np = pytest.importorskip("numpy")

from async_batcher.ml.keras.model import KerasAsyncBatcher  # noqa: E402


@pytest.mark.parametrize("array_samples", [False, True])
@pytest.mark.asyncio
async def test_samples_are_batched_as_one_dense_model_input(array_samples):
    def predict(inputs, batch_size):
        assert inputs.shape == (2, 1)
        assert batch_size == 2
        return inputs * 2

    model = MagicMock()
    model.predict.side_effect = predict
    batcher = KerasAsyncBatcher(model=model, max_batch_size=2)
    samples = [np.asarray([value]) if array_samples else [value] for value in [1.0, 3.0]]
    try:
        result = await asyncio.wait_for(asyncio.gather(*(batcher.process(sample) for sample in samples)), 2)
        np.testing.assert_array_equal(result, [[2.0], [6.0]])
        model.predict.assert_called_once()
    finally:
        await batcher.stop()


@pytest.mark.parametrize("array_samples", [False, True])
@pytest.mark.asyncio
async def test_real_keras_model_returns_one_prediction_per_sample(array_samples):
    keras = pytest.importorskip("keras")
    model = keras.Sequential(
        [
            keras.Input(shape=(1,)),
            keras.layers.Dense(1, use_bias=False, kernel_initializer=keras.initializers.Constant(2)),
        ]
    )
    batcher = KerasAsyncBatcher(model=model, max_batch_size=2)
    samples = [np.asarray([value]) if array_samples else [value] for value in [1.0, 3.0]]
    try:
        result = await asyncio.wait_for(asyncio.gather(*(batcher.process(sample) for sample in samples)), 10)
        np.testing.assert_allclose(result, [[2.0], [6.0]])
    finally:
        await batcher.stop()
