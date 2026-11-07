from __future__ import annotations

from unittest.mock import patch

import pytest

scylla_module = pytest.importorskip("async_batcher.scylladb.update")
columns = pytest.importorskip("cassandra.cqlengine.columns")
Model = pytest.importorskip("cassandra.cqlengine.models").Model


class DefaultRecord(Model):
    __keyspace__ = "test"
    id = columns.Integer(primary_key=True)
    value = columns.Text()


class ArchiveRecord(DefaultRecord):
    __connection__ = "archive"


class OtherRecord(DefaultRecord):
    __connection__ = "other"


def make_operation(model, operation, record_id=1):
    return scylla_module.WriteOperation(
        operation=operation,
        model=model,
        key={"id": record_id},
        data={"id": record_id, "value": "saved"} if operation == "INSERT" else {"value": "saved"},
    )


@pytest.mark.parametrize("model,connection", [(DefaultRecord, None), (ArchiveRecord, "archive")])
@pytest.mark.parametrize("operation", ["INSERT", "UPDATE", "DELETE"])
def test_writes_use_the_model_connection(model, connection, operation):
    with patch("cassandra.cqlengine.query.conn.execute", return_value=[]) as execute:
        results = scylla_module.AsyncScyllaDbWriteBatcher().process_batch([make_operation(model, operation)])

    assert results == [None]
    execute.assert_called_once()
    assert execute.call_args.kwargs["connection"] == connection


@pytest.mark.parametrize(
    "models", [(DefaultRecord, ArchiveRecord), (ArchiveRecord, DefaultRecord), (ArchiveRecord, OtherRecord)]
)
@pytest.mark.parametrize("operation", ["INSERT", "UPDATE", "DELETE"])
def test_mixed_connections_fail_before_any_write(models, operation):
    batch = [make_operation(model, operation, index) for index, model in enumerate(models)]
    with patch("cassandra.cqlengine.query.conn.execute", return_value=[]) as execute:
        with pytest.raises(ValueError, match="same connection"):
            scylla_module.AsyncScyllaDbWriteBatcher().process_batch(batch)

    execute.assert_not_called()


def test_invalid_operations_do_not_select_the_connection():
    batch = [
        scylla_module.WriteOperation(operation="INSERT", model=OtherRecord),
        make_operation(ArchiveRecord, "UPDATE"),
    ]
    with patch("cassandra.cqlengine.query.conn.execute", return_value=[]) as execute:
        results = scylla_module.AsyncScyllaDbWriteBatcher().process_batch(batch)

    assert isinstance(results[0], ValueError)
    assert results[1] is None
    execute.assert_called_once()
    assert execute.call_args.kwargs["connection"] == "archive"
