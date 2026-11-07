from __future__ import annotations

import asyncio
from unittest.mock import MagicMock

import pytest

np = pytest.importorskip("numpy")

from async_batcher.ml.keras.model import KerasAsyncBatcher  # noqa: E402


@pytest.mark.parametrize("structure", [list, tuple, dict])
def test_structured_outputs_are_routed_per_request(structure):
    doubled = np.asarray([[2.0], [8.0]])
    tripled = np.asarray([[3.0], [12.0]])
    output = {"double": doubled, "triple": tripled} if structure is dict else structure([doubled, tripled])
    model = MagicMock()
    model.predict.return_value = output
    results = KerasAsyncBatcher(model=model).process_batch([[1.0], [4.0]])
    assert len(results) == 2
    for result, value in zip(results, [1.0, 4.0], strict=True):
        assert isinstance(result, structure)
        if structure is dict:
            np.testing.assert_allclose(result["double"], [value * 2])
            np.testing.assert_allclose(result["triple"], [value * 3])
        else:
            np.testing.assert_allclose(result, [[value * 2], [value * 3]])


def test_mismatched_output_batch_dimensions_fail_before_routing():
    model = MagicMock()
    model.predict.return_value = {"valid": np.zeros((2, 1)), "short": np.zeros((1, 1))}
    with pytest.raises(ValueError, match="Expected 2 predictions in each Keras output, got 1"):
        KerasAsyncBatcher(model=model).process_batch([[1.0], [4.0]])


@pytest.mark.parametrize("dictionary_outputs", [False, True])
@pytest.mark.asyncio
async def test_two_output_keras_model_routes_both_heads_to_each_request(dictionary_outputs):
    keras = pytest.importorskip("keras")
    inputs = keras.Input(shape=(1,))
    doubled = keras.layers.Dense(1, use_bias=False, kernel_initializer=keras.initializers.Constant(2))(inputs)
    tripled = keras.layers.Dense(1, use_bias=False, kernel_initializer=keras.initializers.Constant(3))(inputs)
    outputs = {"double": doubled, "triple": tripled} if dictionary_outputs else [doubled, tripled]
    batcher = KerasAsyncBatcher(model=keras.Model(inputs, outputs), max_batch_size=2)
    try:
        results = await asyncio.wait_for(asyncio.gather(batcher.process([1.0]), batcher.process([4.0])), 10)
        for result, value in zip(results, [1.0, 4.0], strict=True):
            if dictionary_outputs:
                assert isinstance(result, dict)
                np.testing.assert_allclose(result["double"], [value * 2])
                np.testing.assert_allclose(result["triple"], [value * 3])
            else:
                np.testing.assert_allclose(result, [[value * 2], [value * 3]])
    finally:
        await batcher.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("output", [3.0, {"valid": np.ones((2, 1)), "scalar": np.asarray(3.0)}])
async def test_scalar_model_output_fails_all_callers_and_next_batch_recovers(output):
    model = MagicMock()
    model.predict.side_effect = [output, np.asarray([[6.0], [8.0]])]
    batcher = KerasAsyncBatcher(model=model, max_batch_size=2, max_queue_time=1)
    try:
        failures = await asyncio.wait_for(
            asyncio.gather(batcher.process([1.0]), batcher.process([2.0]), return_exceptions=True), 5
        )
        assert all(isinstance(error, ValueError) for error in failures)
        assert all("leading batch dimension" in str(error) for error in failures)
        results = await asyncio.wait_for(asyncio.gather(batcher.process([3.0]), batcher.process([4.0])), 5)
        np.testing.assert_array_equal(results, [[6.0], [8.0]])
        assert model.predict.call_count == 2
    finally:
        await batcher.stop(force=True, timeout=5)
