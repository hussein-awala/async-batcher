from __future__ import annotations

from unittest.mock import MagicMock

import pytest

scylla_module = pytest.importorskip("async_batcher.scylladb.update")


def test_invalid_operation_keeps_result_alignment(monkeypatch):
    batch_query = MagicMock()
    monkeypatch.setattr(scylla_module, "BatchQuery", batch_query)
    model = MagicMock()
    batcher = scylla_module.AsyncScyllaDbWriteBatcher()
    results = batcher.process_batch(
        batch=[
            scylla_module.WriteOperation(operation="UNKNOWN", model=model),
            scylla_module.WriteOperation(operation="INSERT", model=model, data={"id": 1}),
        ]
    )
    assert len(results) == 2
    assert isinstance(results[0], ValueError)
    assert str(results[0]) == "Invalid operation: UNKNOWN"
    assert results[1] is None
    model.batch.return_value.create.assert_called_once_with(id=1)
