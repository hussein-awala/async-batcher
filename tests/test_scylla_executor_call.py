from __future__ import annotations

import asyncio
from unittest.mock import MagicMock

import pytest

scylla_module = pytest.importorskip("async_batcher.scylladb.update")


@pytest.mark.asyncio
async def test_scylla_process_runs_sync_batch_in_executor(monkeypatch):
    batch_query = MagicMock()
    monkeypatch.setattr(scylla_module, "BatchQuery", batch_query)
    model = MagicMock()
    batcher = scylla_module.AsyncScyllaDbWriteBatcher(max_queue_time=0)
    try:
        operation = scylla_module.WriteOperation(operation="INSERT", model=model, data={"id": 1})
        result = await asyncio.wait_for(batcher.process(operation), timeout=2)
        assert result is None
        model.batch.assert_called_once_with(batch_query.return_value.__enter__.return_value)
        model.batch.return_value.create.assert_called_once_with(id=1)
    finally:
        await batcher.stop()
