from __future__ import annotations

import asyncio
import runpy
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

keras = pytest.importorskip("keras")
np = pytest.importorskip("numpy")


class ExampleApp:
    def on_event(self, name):
        return lambda handler: handler

    def post(self, path):
        return lambda handler: handler


@pytest.mark.asyncio
@pytest.mark.parametrize("example", ["keras", "keras_with_grpc"])
async def test_examples_load_legacy_model_and_predict_dense_inputs(monkeypatch, tmp_path, example):
    # Exercise real Keras model loading/prediction with CI's JAX backend while
    # keeping web servers and TensorFlow installation outside this adapter suite.
    monkeypatch.setitem(sys.modules, "fastapi", SimpleNamespace(FastAPI=ExampleApp))
    monkeypatch.setitem(sys.modules, "grpc", SimpleNamespace())
    monkeypatch.setitem(sys.modules, "predictor_pb2", SimpleNamespace())
    monkeypatch.setitem(sys.modules, "predictor_pb2_grpc", SimpleNamespace(KerasPredictorServicer=object))
    script = "main.py" if example == "keras" else "server.py"
    script_path = Path(__file__).resolve().parents[3] / "examples" / example / script
    monkeypatch.chdir(tmp_path)
    namespace = runpy.run_path(str(script_path))
    batcher = namespace["batcher"] if example == "keras" else namespace["PredictorGRPC"]().batcher
    inputs = [[0.0] * 10, [1.0] * 10]
    try:
        results = await asyncio.wait_for(
            asyncio.gather(*(batcher.process(row) for row in inputs)), timeout=10
        )
        expected = batcher.model.predict(np.asarray(inputs, dtype="float32"), verbose=0)
        np.testing.assert_allclose(results, expected, rtol=1e-5)
        assert np.isfinite(results).all()
        if example == "keras":
            assert await namespace["predict"](inputs[0]) == pytest.approx(expected[0, 0])
    finally:
        await batcher.stop()
