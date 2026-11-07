from __future__ import annotations

from copy import deepcopy
from unittest.mock import AsyncMock, MagicMock

import pytest

get_module = pytest.importorskip("async_batcher.aws.dynamodb.get")
retry_module = pytest.importorskip("async_batcher.aws.dynamodb._retry")
AsyncDynamoDbGetBatcher = get_module.AsyncDynamoDbGetBatcher
GetItem = get_module.GetItem


@pytest.fixture
def dynamodb_session():
    resource = MagicMock()
    resource.batch_get_item = AsyncMock()
    session = MagicMock()
    session.resource.return_value.__aenter__ = AsyncMock(return_value=resource)
    session.resource.return_value.__aexit__ = AsyncMock(return_value=False)
    return session, resource


@pytest.mark.asyncio
async def test_projection_preserves_aliases_consistency_and_routing_across_retries(
    dynamodb_session, monkeypatch
):
    session, resource = dynamodb_session
    monkeypatch.setattr(retry_module.asyncio, "sleep", AsyncMock())
    aliases = {"#pk": "primary.key", "#payload": "payload", "#name": "name"}
    expression = "#pk, sortKey, #payload.#name, tags[1]"
    pending_key = {"primary.key": "a", "sortKey": "one"}
    remaining = {
        "first": {
            "Keys": [pending_key],
            "ProjectionExpression": expression,
            "ExpressionAttributeNames": aliases.copy(),
            "ConsistentRead": True,
        }
    }
    first = {**pending_key, "payload": {"name": "first"}, "tags": ["selected"]}
    second = {"primary.key": "b", "sortKey": "two", "payload": {"name": "second"}}
    third = {"primary.key": "c", "payload": {"name": "third"}}
    resource.batch_get_item.side_effect = [
        {"Responses": {"second": [third], "first": [second]}, "UnprocessedKeys": remaining},
        {"Responses": {"first": [first]}},
    ]
    batcher = AsyncDynamoDbGetBatcher(
        aioboto3_session=session,
        consistent_read=True,
        projection_expression=expression,
        expression_attribute_names=aliases,
    )
    # Caller-owned options cannot change the batcher's routing contract afterward.
    aliases["#pk"] = "changed"
    items = [
        GetItem(table_name="first", key=pending_key),
        GetItem(table_name="first", key={"primary.key": "b", "sortKey": "two"}),
        GetItem(table_name="second", key={"primary.key": "c"}),
        GetItem(table_name="first", key=pending_key.copy()),
        GetItem(table_name="second", key={"primary.key": "missing"}),
    ]
    original_keys = deepcopy([item.key for item in items])
    assert await batcher.process_batch(items) == [first, second, third, first, None]
    initial = resource.batch_get_item.call_args_list[0].kwargs["RequestItems"]
    for table in initial.values():
        assert table["ProjectionExpression"] == expression
        assert table["ExpressionAttributeNames"] == {
            "#pk": "primary.key",
            "#payload": "payload",
            "#name": "name",
        }
        assert table["ConsistentRead"] is True
    assert initial["first"]["Keys"] == [pending_key, {"primary.key": "b", "sortKey": "two"}]
    assert resource.batch_get_item.call_args_list[1].kwargs["RequestItems"] == remaining
    assert [item.key for item in items] == original_keys


@pytest.mark.parametrize(
    "expression,key,aliases",
    [
        ("payload", {"id": "a"}, {}),
        ("id", {"id": "a", "sortKey": "b"}, {}),
        ("id.child", {"id": "a"}, {}),
        ("id[0]", {"id": "a"}, {}),
        ("#pk.child", {"id": "a"}, {"#pk": "id"}),
        ("primary.key", {"primary.key": "a"}, {}),
    ],
)
@pytest.mark.asyncio
async def test_projection_requires_each_complete_primary_key_attribute(
    dynamodb_session, expression, key, aliases
):
    session, _ = dynamodb_session
    batcher = AsyncDynamoDbGetBatcher(
        aioboto3_session=session,
        projection_expression=expression,
        expression_attribute_names=aliases,
    )
    with pytest.raises(ValueError, match="projection_expression must include.*primary-key"):
        await batcher.process_batch([GetItem(table_name="table", key=key)])
    session.resource.assert_not_called()


@pytest.mark.parametrize(
    "options",
    [
        {"projection_expression": ""},
        {"projection_expression": "  "},
        {"projection_expression": 1},
        {"expression_attribute_names": {"#pk": "id"}},
        {"projection_expression": "id", "expression_attribute_names": []},
        {"projection_expression": "#pk", "expression_attribute_names": {"pk": "id"}},
        {"projection_expression": "#pk", "expression_attribute_names": {"#pk": ""}},
        {"projection_expression": "#pk", "expression_attribute_names": {"#pk": 1}},
    ],
)
def test_invalid_projection_configuration_is_rejected(options):
    with pytest.raises(ValueError):
        AsyncDynamoDbGetBatcher(aioboto3_session=MagicMock(), **options)
