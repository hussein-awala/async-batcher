from __future__ import annotations

from unittest.mock import patch

import pytest

scylla_module = pytest.importorskip("async_batcher.scylladb.update")
columns = pytest.importorskip("cassandra.cqlengine.columns")
Model = pytest.importorskip("cassandra.cqlengine.models").Model


class CounterRecord(Model):
    __keyspace__ = "test"
    __connection__ = "archive"
    id = columns.Integer(primary_key=True)
    total = columns.Counter()


class RegularRecord(Model):
    __keyspace__ = "test"
    __connection__ = "archive"
    id = columns.Integer(primary_key=True)
    total = columns.Integer()


def make_operation(model, operation):
    return scylla_module.WriteOperation(
        operation=operation,
        model=model,
        key={"id": 1},
        data={"id": 1, "total": 2} if operation == "INSERT" else {"total": 2},
    )


@pytest.mark.parametrize("operation", ["INSERT", "UPDATE", "DELETE"])
@pytest.mark.parametrize("model,counter", [(CounterRecord, True), (RegularRecord, False)])
def test_batch_type_matches_the_models_columns(operation, model, counter):
    with patch("cassandra.cqlengine.query.conn.execute", return_value=[]) as execute:
        results = scylla_module.AsyncScyllaDbWriteBatcher().process_batch([make_operation(model, operation)])

    assert results == [None]
    execute.assert_called_once()
    query = execute.call_args.args[0]
    assert query.splitlines()[0].split() == (["BEGIN", "COUNTER", "BATCH"] if counter else ["BEGIN", "BATCH"])
    assert execute.call_args.kwargs["connection"] == "archive"


@pytest.mark.parametrize("reverse", [False, True])
def test_counter_and_regular_writes_are_rejected_before_submission(reverse):
    batch = [make_operation(CounterRecord, "UPDATE"), make_operation(RegularRecord, "UPDATE")]
    if reverse:
        batch.reverse()
    with patch("cassandra.cqlengine.query.conn.execute", return_value=[]) as execute:
        with pytest.raises(ValueError, match="counter and non-counter"):
            scylla_module.AsyncScyllaDbWriteBatcher().process_batch(batch)

    execute.assert_not_called()


def test_invalid_operations_do_not_select_the_batch_type():
    batch = [
        scylla_module.WriteOperation(operation="INSERT", model=RegularRecord),
        make_operation(CounterRecord, "UPDATE"),
    ]
    with patch("cassandra.cqlengine.query.conn.execute", return_value=[]) as execute:
        results = scylla_module.AsyncScyllaDbWriteBatcher().process_batch(batch)

    assert isinstance(results[0], ValueError)
    assert results[1] is None
    execute.assert_called_once()
    assert execute.call_args.args[0].splitlines()[0].split() == ["BEGIN", "COUNTER", "BATCH"]
