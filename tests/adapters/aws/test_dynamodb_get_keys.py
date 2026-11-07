from __future__ import annotations

from decimal import Decimal
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


@pytest.mark.parametrize("invalid_key", [{"id": "a", "unexpected": "field"}, {"other": "a"}])
@pytest.mark.asyncio
async def test_get_rejects_inconsistent_key_names_before_opening_resource(dynamodb_session, invalid_key):
    session, resource = dynamodb_session
    resource.batch_get_item.return_value = {"Responses": {"table": [{"id": "a"}]}}
    batcher = AsyncDynamoDbGetBatcher(aioboto3_session=session)
    with pytest.raises(ValueError, match="same key attributes"):
        await batcher.process_batch(
            [GetItem(table_name="table", key={"id": "a"}), GetItem(table_name="table", key=invalid_key)]
        )
    session.resource.assert_not_called()


@pytest.mark.parametrize("invalid_value", [True, 1.0, None, [1], {"id": 1}])
@pytest.mark.asyncio
async def test_get_does_not_deduplicate_invalid_keys_with_numeric_keys(dynamodb_session, invalid_value):
    session, resource = dynamodb_session
    resource.batch_get_item.return_value = {"Responses": {"table": [{"id": 1}]}}
    batcher = AsyncDynamoDbGetBatcher(aioboto3_session=session)
    with pytest.raises((TypeError, ValueError)):
        await batcher.process_batch(
            [
                GetItem(table_name="table", key={"id": 1}),
                GetItem(table_name="table", key={"id": invalid_value}),
            ]
        )
    session.resource.assert_not_called()


@pytest.mark.asyncio
async def test_get_deduplicates_equivalent_numeric_and_binary_keys_across_tables(dynamodb_session):
    session, resource = dynamodb_session
    numeric = {"id": Decimal("1"), "value": "number"}
    binary = {"id": get_module.Binary(b"key"), "value": "binary"}
    resource.batch_get_item.return_value = {"Responses": {"numbers": [numeric], "binary": [binary]}}
    batcher = AsyncDynamoDbGetBatcher(aioboto3_session=session)
    assert await batcher.process_batch(
        [
            GetItem(table_name="numbers", key={"id": 1}),
            GetItem(table_name="binary", key={"id": b"key"}),
            GetItem(table_name="numbers", key={"id": Decimal("1.00")}),
            GetItem(table_name="binary", key={"id": get_module.Binary(bytearray(b"key"))}),
        ]
    ) == [numeric, binary, numeric, binary]
    assert resource.batch_get_item.call_args.kwargs["RequestItems"] == {
        "numbers": {"Keys": [{"id": 1}]},
        "binary": {"Keys": [{"id": b"key"}]},
    }


@pytest.mark.asyncio
async def test_get_rejects_empty_keys_before_opening_resource(dynamodb_session):
    session, _ = dynamodb_session
    batcher = AsyncDynamoDbGetBatcher(aioboto3_session=session)
    with pytest.raises(ValueError, match="at least one attribute"):
        await batcher.process_batch([GetItem(table_name="table", key={})])
    session.resource.assert_not_called()
