from __future__ import annotations

import asyncio
import inspect
import runpy
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest


class ExampleApp:
    def __init__(self):
        self.handlers = {}

    def on_event(self, name):
        def register(handler):
            self.handlers[name] = handler
            return handler

        return register

    def post(self, path):
        return lambda handler: handler


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "example,batcher_names", [("keras", ["batcher"]), ("dynamodb", ["get_batcher", "write_batcher"])]
)
async def test_shutdown_awaits_batchers_on_serving_loop(monkeypatch, example, batcher_names):
    # Keep this lifecycle test independent of model backends and the web server.
    monkeypatch.setitem(sys.modules, "fastapi", SimpleNamespace(FastAPI=ExampleApp, HTTPException=Exception))
    monkeypatch.setitem(sys.modules, "pydantic", SimpleNamespace(BaseModel=object))
    monkeypatch.setitem(sys.modules, "tensorflow", Mock())
    monkeypatch.setitem(sys.modules, "aioboto3", Mock())
    monkeypatch.setitem(sys.modules, "async_batcher.aws.dynamodb.get", Mock())
    monkeypatch.setitem(sys.modules, "async_batcher.aws.dynamodb.write", Mock())
    namespace = runpy.run_path(str(Path(__file__).parents[2] / "examples" / example / "main.py"))
    handler = namespace["app"].handlers["shutdown"]
    serving_loop = asyncio.get_running_loop()
    stopped = []

    class Batcher:
        async def stop(self):
            assert asyncio.get_running_loop() is serving_loop
            await asyncio.sleep(0)
            stopped.append(self)

    batchers = [Batcher() for _ in batcher_names]
    for name, batcher in zip(batcher_names, batchers, strict=True):
        handler.__globals__[name] = batcher

    # Shutdown hooks run while the serving event loop is active.
    if inspect.iscoroutinefunction(handler):
        await handler()
    else:
        handler()
    assert stopped == batchers
