from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

get_module = pytest.importorskip("async_batcher.aws.dynamodb.get")
write_module = pytest.importorskip("async_batcher.aws.dynamodb.write")
AsyncDynamoDbGetBatcher = get_module.AsyncDynamoDbGetBatcher
AsyncDynamoDbWriteBatcher = write_module.AsyncDynamoDbWriteBatcher
GetItem = get_module.GetItem
WriteOperation = write_module.WriteOperation


@pytest.fixture
def dynamodb_session():
    resource = MagicMock()
    resource.batch_get_item = AsyncMock()
    resource.batch_write_item = AsyncMock()
    session = MagicMock()
    session.resource.return_value.__aenter__ = AsyncMock(return_value=resource)
    session.resource.return_value.__aexit__ = AsyncMock(return_value=False)
    return session, resource


@pytest.mark.parametrize("use_ssl,expected", [(None, True), (True, True), (False, False)])
@pytest.mark.parametrize("operation", ["get", "write"])
@pytest.mark.asyncio
async def test_resources_use_tls_by_default(dynamodb_session, use_ssl, expected, operation):
    session, resource = dynamodb_session
    if operation == "get":
        resource.batch_get_item.return_value = {"Responses": {}}
        batcher = AsyncDynamoDbGetBatcher(aioboto3_session=session, use_ssl=use_ssl)
        await batcher.process_batch([GetItem(table_name="table", key={"id": "a"})])
    else:
        resource.batch_write_item.return_value = {}
        batcher = AsyncDynamoDbWriteBatcher(aioboto3_session=session, use_ssl=use_ssl)
        await batcher.process_batch([WriteOperation(table_name="table", operation="PUT", data={"id": "a"})])
    assert session.resource.call_args.kwargs["use_ssl"] is expected
