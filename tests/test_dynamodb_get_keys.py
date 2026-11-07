from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

get_module = pytest.importorskip("async_batcher.aws.dynamodb.get")
AsyncDynamoDbGetBatcher = get_module.AsyncDynamoDbGetBatcher
GetItem = get_module.GetItem


@pytest.fixture
def dynamodb_session():
    resource = MagicMock()
    resource.batch_get_item = AsyncMock()
    resource.batch_write_item = AsyncMock()
    session = MagicMock()
    session.resource.return_value.__aenter__ = AsyncMock(return_value=resource)
    session.resource.return_value.__aexit__ = AsyncMock(return_value=False)
    return session, resource


@pytest.mark.asyncio
async def test_get_deduplicates_keys_and_maps_unordered_results(dynamodb_session):
    session, resource = dynamodb_session
    resource.batch_get_item.return_value = {
        "Responses": {"table": [{"id": "b", "value": 2}, {"id": "a", "value": 1}]}
    }
    batcher = AsyncDynamoDbGetBatcher(aioboto3_session=session)
    results = await batcher.process_batch(
        [GetItem(table_name="table", key={"id": key}) for key in ["a", "b", "a", "missing"]]
    )
    assert results == [{"id": "a", "value": 1}, {"id": "b", "value": 2}, {"id": "a", "value": 1}, None]
    assert resource.batch_get_item.call_args.kwargs["RequestItems"] == {
        "table": {"Keys": [{"id": "a"}, {"id": "b"}, {"id": "missing"}]}
    }


@pytest.mark.asyncio
async def test_get_accepts_binary_keys(dynamodb_session):
    session, resource = dynamodb_session
    item = {"id": get_module.Binary(b"key"), "value": 7}
    resource.batch_get_item.return_value = {"Responses": {"table": [item]}}
    batcher = AsyncDynamoDbGetBatcher(aioboto3_session=session)
    assert await batcher.process_batch([GetItem(table_name="table", key={"id": bytearray(b"key")})]) == [item]
