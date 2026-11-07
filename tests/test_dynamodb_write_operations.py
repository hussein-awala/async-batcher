from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

write_module = pytest.importorskip("async_batcher.aws.dynamodb.write")


@pytest.mark.parametrize("operation", ["UPDATE", "put", "", None])
@pytest.mark.asyncio
async def test_invalid_operation_fails_before_sending_a_batch(operation):
    session = MagicMock()
    batcher = write_module.AsyncDynamoDbWriteBatcher(aioboto3_session=session)
    with pytest.raises(ValueError, match="Invalid operation"):
        await batcher.process_batch(
            [
                write_module.WriteOperation(table_name="table", operation="PUT", data={"id": "valid"}),
                write_module.WriteOperation(table_name="table", operation=operation, data={"id": "invalid"}),
            ]
        )
    session.resource.assert_not_called()


@pytest.mark.parametrize(
    "operation,request_key,data_key", [("PUT", "PutRequest", "Item"), ("DELETE", "DeleteRequest", "Key")]
)
@pytest.mark.asyncio
async def test_valid_operation_request_shape_is_preserved(operation, request_key, data_key):
    session = MagicMock()
    resource = MagicMock()
    resource.batch_write_item = AsyncMock()
    session.resource.return_value.__aenter__ = AsyncMock(return_value=resource)
    session.resource.return_value.__aexit__ = AsyncMock(return_value=False)
    batcher = write_module.AsyncDynamoDbWriteBatcher(aioboto3_session=session)
    await batcher.process_batch(
        [write_module.WriteOperation(table_name="table", operation=operation, data={"id": "a"})]
    )
    assert resource.batch_write_item.call_args.kwargs["RequestItems"] == {
        "table": [{request_key: {data_key: {"id": "a"}}}]
    }
