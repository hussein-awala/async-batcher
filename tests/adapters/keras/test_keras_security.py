from __future__ import annotations

from pathlib import Path

import pytest


def test_bundled_hdf5_model_still_loads_and_predicts():
    keras = pytest.importorskip("keras")
    np = pytest.importorskip("numpy")
    model_path = Path(__file__).resolve().parents[3] / "examples" / "diabetes_tf_model.h5"
    model = keras.models.load_model(model_path, compile=False)
    predictions = model.predict(np.zeros((2, 10), dtype="float32"), verbose=0)
    assert predictions.shape == (2, 1)
    assert np.isfinite(predictions).all()


def test_native_model_round_trip_preserves_predictions(tmp_path):
    keras = pytest.importorskip("keras")
    np = pytest.importorskip("numpy")
    model = keras.Sequential(
        [
            keras.Input(shape=(1,)),
            keras.layers.Dense(1, use_bias=False, kernel_initializer=keras.initializers.Constant(2)),
        ]
    )
    model_path = tmp_path / "predictor.keras"
    model.save(model_path)
    restored = keras.models.load_model(model_path, compile=False)
    predictions = restored.predict(np.asarray([[1.0], [4.0]], dtype="float32"), verbose=0)
    np.testing.assert_allclose(predictions, [[2.0], [8.0]])
