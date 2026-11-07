from __future__ import annotations

import asyncio
import runpy
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest


@pytest.mark.asyncio
@pytest.mark.parametrize("stop_fails", [False, True])
async def test_cancelled_server_stops_rpcs_before_batcher(monkeypatch, stop_fails):
    waiting = asyncio.Event()
    order = []

    class Server:
        def add_insecure_port(self, address):
            return 50051

        async def start(self):
            pass

        async def wait_for_termination(self):
            waiting.set()
            await asyncio.Event().wait()

        async def stop(self, grace):
            assert grace == 30
            order.append("server")
            await asyncio.sleep(0)
            if stop_fails:
                raise RuntimeError("server cleanup failed")

    class Batcher:
        async def stop(self):
            order.append("batcher")

    monkeypatch.setitem(sys.modules, "grpc", SimpleNamespace(aio=SimpleNamespace(server=Server)))
    monkeypatch.setitem(sys.modules, "predictor_pb2", SimpleNamespace())
    monkeypatch.setitem(
        sys.modules,
        "predictor_pb2_grpc",
        SimpleNamespace(KerasPredictorServicer=object, add_KerasPredictorServicer_to_server=Mock()),
    )
    monkeypatch.setitem(sys.modules, "tensorflow", Mock())
    namespace = runpy.run_path(str(Path(__file__).parents[1] / "examples" / "keras_with_grpc" / "server.py"))
    serve = namespace["serve"]
    serve.__globals__["PredictorGRPC"] = lambda: SimpleNamespace(batcher=Batcher())

    serving = asyncio.create_task(serve())
    await asyncio.wait_for(waiting.wait(), 1)
    serving.cancel()
    expected_error = RuntimeError if stop_fails else asyncio.CancelledError
    with pytest.raises(expected_error):
        await asyncio.wait_for(serving, 1)
    assert order == ["server", "batcher"]
