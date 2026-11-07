from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio

pytest.importorskip("pymongo")

from async_batcher.mongodb import AsyncMongoDbWriteBatcher, MongoDbNotExecutedError  # noqa: E402
from pymongo import AsyncMongoClient, DeleteMany, DeleteOne, InsertOne, ReplaceOne, UpdateMany, UpdateOne  # noqa: E402
from pymongo.errors import ClientBulkWriteException, DuplicateKeyError, InvalidOperation, WriteError  # noqa: E402
from pymongo.results import ClientBulkWriteResult, DeleteResult, InsertOneResult, UpdateResult  # noqa: E402


@pytest_asyncio.fixture
async def client():
    client = AsyncMongoClient("mongodb://localhost:27017", serverSelectionTimeoutMS=50)
    try:
        yield client
    finally:
        await client.close()


def bulk_details(**overrides):
    return {
        "anySuccessful": True,
        "error": None,
        "writeErrors": [],
        "writeConcernErrors": [],
        "insertResults": {},
        "updateResults": {},
        "deleteResults": {},
        **overrides,
    }


@pytest.mark.asyncio
async def test_mixed_operations_route_native_results_in_request_order(client):
    namespace = "db.items"
    operations = [
        InsertOne({"_id": 1}, namespace=namespace),
        UpdateOne({"_id": 1}, {"$set": {"x": 2}}, namespace=namespace),
        DeleteMany({"x": 2}, namespace=namespace),
        ReplaceOne({"_id": 2}, {"x": 3}, upsert=True, namespace=namespace),
        DeleteOne({"_id": 9}, namespace="other.items"),
        UpdateMany({}, {"$inc": {"x": 1}}, namespace=namespace),
    ]
    expected = [
        InsertOneResult(1, True),
        UpdateResult({"n": 1, "nModified": 1}, True, in_client_bulk=True),
        DeleteResult({"n": 2}, True),
        UpdateResult({"n": 1, "nModified": 0, "upserted": {"_id": 2}}, True, in_client_bulk=True),
        DeleteResult({"n": 0}, True),
        UpdateResult({"n": 5, "nModified": 4}, True, in_client_bulk=True),
    ]
    response = ClientBulkWriteResult(
        bulk_details(
            insertResults={0: expected[0]},
            updateResults={5: expected[5], 1: expected[1], 3: expected[3]},
            deleteResults={4: expected[4], 2: expected[2]},
        ),
        acknowledged=True,
        has_verbose_results=True,
    )
    batcher = AsyncMongoDbWriteBatcher(client=client, max_batch_size=len(operations))
    with patch.object(client, "bulk_write", AsyncMock(return_value=response)) as bulk_write:
        try:
            results = await asyncio.wait_for(asyncio.gather(*(batcher.process(op) for op in operations)), 2)
            assert results == expected
            assert results[3].upserted_id == 2
            assert results[3].did_upsert
            assert results[4].deleted_count == 0
            bulk_write.assert_awaited_once_with(operations, ordered=True, verbose_results=True)
        finally:
            await batcher.stop()


@pytest.mark.parametrize("failed_index", [0, 1, 2])
@pytest.mark.parametrize("code,error_type", [(11000, DuplicateKeyError), (121, WriteError)])
@pytest.mark.asyncio
async def test_ordered_write_error_preserves_success_and_marks_unattempted_suffix(
    client, failed_index, code, error_type
):
    operations = [InsertOne({"_id": index}, namespace="db.items") for index in range(3)]
    prefix = {index: InsertOneResult(index, True) for index in range(failed_index)}
    details = {"idx": failed_index, "code": code, "errmsg": "write rejected", "op": {"document": {}}}
    error = ClientBulkWriteException(
        bulk_details(anySuccessful=bool(prefix), insertResults=prefix, writeErrors=[details]), verbose=True
    )
    with patch.object(client, "bulk_write", AsyncMock(side_effect=error)) as bulk_write:
        results = await AsyncMongoDbWriteBatcher(client=client).process_batch(operations)
    assert results[:failed_index] == list(prefix.values())
    assert isinstance(results[failed_index], error_type)
    assert results[failed_index].details == details
    for index in range(failed_index + 1, len(operations)):
        assert isinstance(results[index], MongoDbNotExecutedError)
        assert results[index].index == index
        assert results[index].failed_index == failed_index
    bulk_write.assert_awaited_once()


@pytest.mark.parametrize(
    "extra", [{"error": ConnectionError("lost reply")}, {"writeConcernErrors": [{"code": 64}]}]
)
@pytest.mark.asyncio
async def test_uncertain_bulk_outcome_is_not_reported_as_individual_success(client, extra):
    error = ClientBulkWriteException(
        bulk_details(insertResults={0: InsertOneResult(1, True)}, **extra), verbose=True
    )
    with patch.object(client, "bulk_write", AsyncMock(side_effect=error)) as bulk_write:
        with pytest.raises(ClientBulkWriteException) as raised:
            await AsyncMongoDbWriteBatcher(client=client).process_batch(
                [InsertOne({"_id": 1}, namespace="db.items")]
            )
    assert raised.value is error
    bulk_write.assert_awaited_once()


@pytest.mark.asyncio
async def test_cancellation_propagates_without_retrying_or_closing_client(client):
    with patch.object(client, "bulk_write", AsyncMock(side_effect=asyncio.CancelledError)) as bulk_write:
        with patch.object(client, "close", AsyncMock()) as close:
            with pytest.raises(asyncio.CancelledError):
                await AsyncMongoDbWriteBatcher(client=client).process_batch(
                    [InsertOne({}, namespace="db.items")]
                )
    bulk_write.assert_awaited_once()
    close.assert_not_awaited()


@pytest.mark.parametrize(
    "operation,error_type",
    [(InsertOne({}), InvalidOperation), (UpdateOne({}, {"invalid": 1}, namespace="db.items"), ValueError)],
)
@pytest.mark.asyncio
async def test_driver_validates_operations_before_any_network_write(client, operation, error_type):
    with patch.object(
        client, "_select_server", AsyncMock(side_effect=AssertionError("No network access"))
    ) as select:
        with pytest.raises(error_type):
            await AsyncMongoDbWriteBatcher(client=client).process_batch(
                [InsertOne({"_id": 1}, namespace="db.items"), operation]
            )
    select.assert_not_awaited()


@pytest.mark.asyncio
async def test_empty_batch_does_not_call_client(client):
    with patch.object(client, "bulk_write", AsyncMock()) as bulk_write:
        assert await AsyncMongoDbWriteBatcher(client=client).process_batch([]) == []
    bulk_write.assert_not_awaited()


@pytest.mark.asyncio
async def test_unacknowledged_writes_are_rejected():
    client = AsyncMongoClient("mongodb://localhost:27017", w=0)
    try:
        with pytest.raises(ValueError, match="acknowledged"):
            AsyncMongoDbWriteBatcher(client=client)
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_incomplete_verbose_results_cannot_silently_succeed(client):
    result = ClientBulkWriteResult(bulk_details(), acknowledged=True, has_verbose_results=True)
    with patch.object(client, "bulk_write", AsyncMock(return_value=result)):
        with pytest.raises(RuntimeError, match="incomplete"):
            await AsyncMongoDbWriteBatcher(client=client).process_batch([InsertOne({}, namespace="db.items")])
