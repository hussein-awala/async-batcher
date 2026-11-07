from __future__ import annotations

from unittest.mock import MagicMock

import pytest

scylla_module = pytest.importorskip("async_batcher.scylladb.update")


@pytest.mark.parametrize(
    "kwargs,error",
    [
        ({"operation": "INSERT"}, "data must be provided for INSERT operations"),
        ({"operation": "UPDATE"}, "key and data must be provided for UPDATE operations"),
        ({"operation": "UPDATE", "key": {"id": 1}}, "key and data must be provided for UPDATE operations"),
        (
            {"operation": "UPDATE", "data": {"value": 2}},
            "key and data must be provided for UPDATE operations",
        ),
        ({"operation": "DELETE"}, "key must be provided for DELETE operations"),
        ({"operation": "UNKNOWN"}, "Invalid operation: UNKNOWN"),
    ],
)
def test_invalid_batches_do_not_create_a_query(monkeypatch, kwargs, error):
    batch_query = MagicMock()
    monkeypatch.setattr(scylla_module, "BatchQuery", batch_query)
    model = MagicMock()
    batcher = scylla_module.AsyncScyllaDbWriteBatcher()
    results = batcher.process_batch([scylla_module.WriteOperation(model=model, **kwargs)])
    assert len(results) == 1
    assert isinstance(results[0], ValueError)
    assert str(results[0]) == error
    batch_query.assert_not_called()
    assert model.mock_calls == []


def test_empty_batch_does_not_create_a_query(monkeypatch):
    batch_query = MagicMock()
    monkeypatch.setattr(scylla_module, "BatchQuery", batch_query)
    assert scylla_module.AsyncScyllaDbWriteBatcher().process_batch([]) == []
    batch_query.assert_not_called()


def test_mixed_batch_executes_valid_items_and_preserves_error_positions(monkeypatch):
    batch_query = MagicMock()
    monkeypatch.setattr(scylla_module, "BatchQuery", batch_query)
    insert_model, update_model, delete_model, invalid_model = (MagicMock() for _ in range(4))
    operation = scylla_module.WriteOperation
    batch = [
        operation(operation="INSERT", model=invalid_model),
        operation(operation="INSERT", model=insert_model, data={"id": 1}),
        operation(operation="UNKNOWN", model=invalid_model),
        operation(operation="UPDATE", model=update_model, key={"id": 2}, data={"value": 20}),
        operation(operation="DELETE", model=delete_model, key={"id": 3}),
        operation(operation="DELETE", model=invalid_model),
    ]
    results = scylla_module.AsyncScyllaDbWriteBatcher().process_batch(batch)
    assert len(results) == len(batch)
    assert [type(result) for result in results] == [
        ValueError,
        type(None),
        ValueError,
        type(None),
        type(None),
        ValueError,
    ]
    assert [str(results[index]) for index in [0, 2, 5]] == [
        "data must be provided for INSERT operations",
        "Invalid operation: UNKNOWN",
        "key must be provided for DELETE operations",
    ]
    query = batch_query.return_value.__enter__.return_value
    insert_model.batch.assert_called_once_with(query)
    insert_model.batch.return_value.create.assert_called_once_with(id=1)
    update_model.objects.assert_called_once_with(id=2)
    update_model.objects.return_value.batch.assert_called_once_with(query)
    update_model.objects.return_value.batch.return_value.update.assert_called_once_with(value=20)
    delete_model.objects.assert_called_once_with(id=3)
    delete_model.objects.return_value.batch.assert_called_once_with(query)
    delete_model.objects.return_value.batch.return_value.delete.assert_called_once_with()
    assert invalid_model.mock_calls == []
    batch_query.return_value.__exit__.assert_called_once_with(None, None, None)
