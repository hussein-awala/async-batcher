from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

get_module = pytest.importorskip("async_batcher.aws.dynamodb.get")
retry_module = pytest.importorskip("async_batcher.aws.dynamodb._retry")
AsyncDynamoDbGetBatcher = get_module.AsyncDynamoDbGetBatcher
GetItem = get_module.GetItem


@pytest.mark.asyncio
async def test_consistent_reads_apply_to_every_table_and_survive_retries(monkeypatch):
    monkeypatch.setattr(retry_module.asyncio, "sleep", AsyncMock())
    remaining = {"second": {"Keys": [{"id": "b"}], "ConsistentRead": True}}
    resource = MagicMock()
    resource.batch_get_item = AsyncMock(
        side_effect=[
            {"Responses": {"first": [{"id": "a"}]}, "UnprocessedKeys": remaining},
            {"Responses": {"second": [{"id": "b"}]}},
        ]
    )
    session = MagicMock()
    session.resource.return_value.__aenter__ = AsyncMock(return_value=resource)
    session.resource.return_value.__aexit__ = AsyncMock(return_value=False)
    batcher = AsyncDynamoDbGetBatcher(aioboto3_session=session, consistent_read=True)
    assert await batcher.process_batch(
        [
            GetItem(table_name="first", key={"id": "a"}),
            GetItem(table_name="second", key={"id": "b"}),
            GetItem(table_name="first", key={"id": "a"}),
        ]
    ) == [{"id": "a"}, {"id": "b"}, {"id": "a"}]
    calls = resource.batch_get_item.call_args_list
    assert calls[0].kwargs["RequestItems"] == {
        "first": {"Keys": [{"id": "a"}], "ConsistentRead": True},
        "second": {"Keys": [{"id": "b"}], "ConsistentRead": True},
    }
    assert calls[1].kwargs["RequestItems"] == remaining


@pytest.mark.parametrize("consistent_read", [None, 0, 1, "true"])
def test_consistent_read_requires_a_boolean(consistent_read):
    with pytest.raises(ValueError, match="consistent_read must be a boolean"):
        AsyncDynamoDbGetBatcher(aioboto3_session=MagicMock(), consistent_read=consistent_read)
