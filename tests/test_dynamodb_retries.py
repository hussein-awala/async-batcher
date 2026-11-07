from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

get_module = pytest.importorskip("async_batcher.aws.dynamodb.get")
write_module = pytest.importorskip("async_batcher.aws.dynamodb.write")
retry_module = pytest.importorskip("async_batcher.aws.dynamodb._retry")
AsyncDynamoDbGetBatcher = get_module.AsyncDynamoDbGetBatcher
AsyncDynamoDbWriteBatcher = write_module.AsyncDynamoDbWriteBatcher
GetItem = get_module.GetItem
WriteOperation = write_module.WriteOperation
DynamoDBRetryError = retry_module.DynamoDBRetryError


@pytest.fixture
def dynamodb_session():
    resource = MagicMock()
    resource.batch_get_item = AsyncMock()
    resource.batch_write_item = AsyncMock()
    session = MagicMock()
    session.resource.return_value.__aenter__ = AsyncMock(return_value=resource)
    session.resource.return_value.__aexit__ = AsyncMock(return_value=False)
    return session, resource


@pytest.fixture
def retry_sleep(monkeypatch):
    sleep = AsyncMock()
    monkeypatch.setattr(retry_module.asyncio, "sleep", sleep)
    return sleep


@pytest.mark.asyncio
async def test_get_retries_only_unprocessed_keys_and_keeps_earlier_results(dynamodb_session, retry_sleep):
    session, resource = dynamodb_session
    remaining = {"table": {"Keys": [{"id": "b"}]}}
    resource.batch_get_item.side_effect = [
        {"Responses": {"table": [{"id": "a"}]}, "UnprocessedKeys": remaining},
        {"Responses": {"table": [{"id": "b"}]}, "UnprocessedKeys": {}},
    ]
    batcher = AsyncDynamoDbGetBatcher(aioboto3_session=session)
    assert await batcher.process_batch(
        [GetItem(table_name="table", key={"id": key}) for key in ["a", "b"]]
    ) == [{"id": "a"}, {"id": "b"}]
    assert resource.batch_get_item.call_args_list[1].kwargs["RequestItems"] == remaining
    retry_sleep.assert_awaited_once_with(0.1)


@pytest.mark.asyncio
async def test_write_retries_only_unprocessed_writes(dynamodb_session, retry_sleep):
    session, resource = dynamodb_session
    remaining = {"table": [{"DeleteRequest": {"Key": {"id": "b"}}}]}
    resource.batch_write_item.side_effect = [{"UnprocessedItems": remaining}, {"UnprocessedItems": {}}]
    batcher = AsyncDynamoDbWriteBatcher(aioboto3_session=session)
    await batcher.process_batch(
        [
            WriteOperation(table_name="table", operation="PUT", data={"id": "a"}),
            WriteOperation(table_name="table", operation="DELETE", data={"id": "b"}),
        ]
    )
    assert resource.batch_write_item.call_args_list[1].kwargs["RequestItems"] == remaining
    retry_sleep.assert_awaited_once_with(0.1)


@pytest.mark.parametrize("operation", ["get", "write"])
@pytest.mark.asyncio
async def test_retry_exhaustion_is_an_error(dynamodb_session, retry_sleep, operation):
    session, resource = dynamodb_session
    if operation == "get":
        batcher = AsyncDynamoDbGetBatcher(aioboto3_session=session, max_retries=2)
        request = GetItem(table_name="table", key={"id": "a"})
        method = resource.batch_get_item
        remaining = {"table": {"Keys": [request.key]}}
        method.return_value = {"UnprocessedKeys": remaining}
    else:
        batcher = AsyncDynamoDbWriteBatcher(aioboto3_session=session, max_retries=2)
        request = WriteOperation(table_name="table", operation="PUT", data={"id": "a"})
        method = resource.batch_write_item
        remaining = {"table": [{"PutRequest": {"Item": request.data}}]}
        method.return_value = {"UnprocessedItems": remaining}
    with pytest.raises(DynamoDBRetryError, match="after 2 retries") as exc:
        await batcher.process_batch([request])
    assert exc.value.unprocessed_items == remaining
    assert method.await_count == 3
    assert [call.args[0] for call in retry_sleep.await_args_list] == [0.1, 0.2]


@pytest.mark.asyncio
async def test_retry_can_be_cancelled(dynamodb_session, retry_sleep):
    session, resource = dynamodb_session
    resource.batch_get_item.return_value = {"UnprocessedKeys": {"table": {"Keys": [{"id": "a"}]}}}
    retry_sleep.side_effect = asyncio.CancelledError
    batcher = AsyncDynamoDbGetBatcher(aioboto3_session=session)
    with pytest.raises(asyncio.CancelledError):
        await batcher.process_batch([GetItem(table_name="table", key={"id": "a"})])
    resource.batch_get_item.assert_awaited_once()
    session.resource.return_value.__aexit__.assert_awaited_once()


@pytest.mark.parametrize("max_retries", [-1, None, True, 1.5])
def test_reject_invalid_retry_budgets(max_retries):
    with pytest.raises(ValueError, match="max_retries"):
        AsyncDynamoDbGetBatcher(max_retries=max_retries, aioboto3_session=MagicMock())
