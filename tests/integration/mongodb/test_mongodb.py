from __future__ import annotations

import asyncio
from uuid import uuid4

import pytest
import pytest_asyncio
from async_batcher.mongodb import AsyncMongoDbWriteBatcher, MongoDbNotExecutedError
from pymongo import AsyncMongoClient, DeleteMany, DeleteOne, InsertOne, ReplaceOne, UpdateMany, UpdateOne
from pymongo.errors import DuplicateKeyError
from pymongo.results import DeleteResult, InsertOneResult, UpdateResult

pytestmark = [pytest.mark.integration, pytest.mark.integration_mongodb]


@pytest_asyncio.fixture
async def database():
    client = AsyncMongoClient(
        "mongodb://127.0.0.1:27017",
        serverSelectionTimeoutMS=5000,
        connectTimeoutMS=5000,
        socketTimeoutMS=5000,
    )
    database = client[f"async_batcher_{uuid4().hex}"]
    try:
        await client.admin.command("ping")
        yield database
    finally:
        try:
            async with asyncio.timeout(15):
                await client.drop_database(database.name)
        finally:
            await client.close()


@pytest.mark.asyncio
async def test_all_write_operations_return_their_own_results(database):
    namespace = database.items.full_name
    operations = [
        InsertOne({"_id": 1, "group": "x", "value": 0}, namespace=namespace),
        InsertOne({"_id": 2, "group": "x", "value": 0}, namespace=namespace),
        UpdateOne({"_id": 1}, {"$inc": {"value": 1}}, namespace=namespace),
        UpdateMany({"group": "x"}, {"$inc": {"value": 2}}, namespace=namespace),
        ReplaceOne({"_id": 2}, {"group": "y", "value": 9}, namespace=namespace),
        DeleteOne({"_id": 1}, namespace=namespace),
        DeleteMany({"group": "y"}, namespace=namespace),
        UpdateOne({"_id": 3}, {"$set": {"value": 7}}, upsert=True, namespace=namespace),
        DeleteMany({"_id": 999}, namespace=namespace),
        InsertOne({"_id": "other"}, namespace=database.other.full_name),
    ]
    batcher = AsyncMongoDbWriteBatcher(client=database.client, max_batch_size=len(operations))
    try:
        results = await asyncio.wait_for(asyncio.gather(*(batcher.process(op) for op in operations)), 15)
        assert [type(result) for result in results] == [
            InsertOneResult,
            InsertOneResult,
            UpdateResult,
            UpdateResult,
            UpdateResult,
            DeleteResult,
            DeleteResult,
            UpdateResult,
            DeleteResult,
            InsertOneResult,
        ]
        assert [results[index].inserted_id for index in [0, 1, 9]] == [1, 2, "other"]
        assert [(results[index].matched_count, results[index].modified_count) for index in [2, 3, 4]] == [
            (1, 1),
            (2, 2),
            (1, 1),
        ]
        assert [results[index].deleted_count for index in [5, 6, 8]] == [1, 1, 0]
        assert results[7].upserted_id == 3
        assert results[7].did_upsert
        assert results[7].modified_count == 0
        assert await database.items.find({}).to_list() == [{"_id": 3, "value": 7}]
        assert await database.other.find_one({"_id": "other"}) == {"_id": "other"}
    finally:
        await batcher.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("failed_index", [0, 1, 2])
async def test_ordered_failure_keeps_successful_prefix_and_does_not_execute_suffix(database, failed_index):
    await database.items.insert_one({"_id": failed_index + 1})
    operations = [InsertOne({"_id": index}, namespace=database.items.full_name) for index in [1, 2, 3]]
    batcher = AsyncMongoDbWriteBatcher(client=database.client, max_batch_size=3)
    try:
        results = await asyncio.wait_for(
            asyncio.gather(*(batcher.process(op) for op in operations), return_exceptions=True), 15
        )
        for index, result in enumerate(results):
            if index < failed_index:
                assert isinstance(result, InsertOneResult)
                assert result.inserted_id == index + 1
            elif index == failed_index:
                assert isinstance(result, DuplicateKeyError)
                assert result.code == 11000
            else:
                assert isinstance(result, MongoDbNotExecutedError)
                assert result.index == index
                assert result.failed_index == failed_index
                assert await database.items.find_one({"_id": index + 1}) is None
        assert await database.items.count_documents({}) == failed_index + 1
        later = await batcher.process(InsertOne({"_id": 4}, namespace=database.items.full_name))
        assert later.inserted_id == 4
    finally:
        await batcher.stop()


@pytest.mark.asyncio
async def test_invalid_operation_is_validated_before_earlier_writes_are_sent(database):
    batcher = AsyncMongoDbWriteBatcher(client=database.client)
    with pytest.raises(ValueError):
        await batcher.process_batch(
            [
                InsertOne({"_id": 1}, namespace=database.items.full_name),
                UpdateOne({"_id": 1}, {"invalid": 2}, namespace=database.items.full_name),
            ]
        )
    assert await database.items.count_documents({}) == 0


@pytest.mark.asyncio
async def test_zero_match_and_unchanged_writes_keep_native_results_and_client_ownership(database):
    await database.items.insert_one({"_id": "unchanged", "value": 0})
    namespace = database.items.full_name
    operations = [
        UpdateOne({"_id": "missing"}, {"$set": {"value": 1}}, namespace=namespace),
        UpdateMany({"_id": "missing"}, {"$set": {"value": 2}}, namespace=namespace),
        ReplaceOne({"_id": "missing"}, {"value": 3}, namespace=namespace),
        UpdateOne({"_id": "unchanged"}, {"$set": {"value": 0}}, namespace=namespace),
        DeleteOne({"_id": "missing"}, namespace=namespace),
    ]
    batcher = AsyncMongoDbWriteBatcher(client=database.client, max_batch_size=len(operations))
    try:
        async with asyncio.timeout(15):
            results = await asyncio.gather(*(batcher.process(operation) for operation in operations))
            assert all(isinstance(result, UpdateResult) for result in results[:4])
            assert [(result.matched_count, result.modified_count) for result in results[:4]] == [
                (0, 0),
                (0, 0),
                (0, 0),
                (1, 0),
            ]
            assert all(result.upserted_id is None and not result.did_upsert for result in results[:4])
            assert isinstance(results[4], DeleteResult)
            assert results[4].deleted_count == 0
    finally:
        await batcher.stop(force=True, timeout=10)
    assert (await database.client.admin.command("ping"))["ok"] == 1.0
    assert await database.items.find({}).to_list() == [{"_id": "unchanged", "value": 0}]
