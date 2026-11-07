from __future__ import annotations

from unittest.mock import AsyncMock, Mock

import pytest

pytest.importorskip("cassandra")

from tests.integration.scylladb import conftest as scylla_fixtures  # noqa: E402


@pytest.mark.asyncio
async def test_batcher_fixture_awaits_cleanup(monkeypatch):
    batcher = Mock(stop=AsyncMock())
    monkeypatch.setattr(scylla_fixtures, "AsyncScyllaDbWriteBatcher", Mock(return_value=batcher))
    fixture = scylla_fixtures.scylladb_write_batcher.__wrapped__()

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
