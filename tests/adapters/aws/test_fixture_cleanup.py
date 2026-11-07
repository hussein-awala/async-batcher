from __future__ import annotations

from unittest.mock import AsyncMock, Mock

import pytest

pytest.importorskip("aioboto3")

from tests.integration.aws import conftest as aws_fixtures  # noqa: E402


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fixture_name,class_name",
    [("get_batcher", "AsyncDynamoDbGetBatcher"), ("write_batcher", "AsyncDynamoDbWriteBatcher")],
)
async def test_batcher_fixture_awaits_cleanup(monkeypatch, fixture_name, class_name):
    batcher = Mock(stop=AsyncMock())
    monkeypatch.setattr(aws_fixtures, class_name, Mock(return_value=batcher))
    fixture = getattr(aws_fixtures, fixture_name).__wrapped__(Mock())

    assert await anext(fixture) is batcher
    batcher.stop.assert_not_called()
    with pytest.raises(StopAsyncIteration):
        await anext(fixture)
    batcher.stop.assert_awaited_once_with()
