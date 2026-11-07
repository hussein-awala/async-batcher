from __future__ import annotations

from unittest.mock import AsyncMock, Mock

import pytest

from tests.integration.aws import conftest as aws_fixtures
from tests.integration.scylladb import conftest as scylla_fixtures


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "module,fixture_name,class_name,args",
    [
        (aws_fixtures, "get_batcher", "AsyncDynamoDbGetBatcher", (Mock(),)),
        (aws_fixtures, "write_batcher", "AsyncDynamoDbWriteBatcher", (Mock(),)),
        (scylla_fixtures, "scylladb_write_batcher", "AsyncScyllaDbWriteBatcher", ()),
    ],
)
async def test_batcher_fixture_awaits_cleanup(monkeypatch, module, fixture_name, class_name, args):
    batcher = Mock(stop=AsyncMock())
    monkeypatch.setattr(module, class_name, Mock(return_value=batcher))
    fixture = getattr(module, fixture_name).__wrapped__(*args)

    assert await anext(fixture) is batcher
    batcher.stop.assert_not_called()
    with pytest.raises(StopAsyncIteration):
        await anext(fixture)
    batcher.stop.assert_awaited_once_with()


def test_scylla_session_fixture_shuts_down_cluster(monkeypatch):
    cluster = Mock()
    monkeypatch.setattr(scylla_fixtures, "Cluster", Mock(return_value=cluster))
    fixture = scylla_fixtures.scylladb_session.__wrapped__()

    assert next(fixture) is cluster.connect.return_value
    with pytest.raises(StopIteration):
        next(fixture)
    cluster.shutdown.assert_called_once_with()


def test_scylla_session_fixture_shuts_down_after_setup_failure(monkeypatch):
    cluster = Mock()
    cluster.connect.side_effect = RuntimeError("connection failed")
    monkeypatch.setattr(scylla_fixtures, "Cluster", Mock(return_value=cluster))
    fixture = scylla_fixtures.scylladb_session.__wrapped__()

    with pytest.raises(RuntimeError, match="connection failed"):
        next(fixture)
    cluster.shutdown.assert_called_once_with()
