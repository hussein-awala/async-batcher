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
        assert results[7].matched_count == 0
        assert await database.items.find({}).to_list() == [{"_id": 3, "value": 7}]
        assert await database.other.find_one({"_id": "other"}) == {"_id": "other"}
    finally:
        await batcher.stop()


@pytest.mark.asyncio
async def test_ordered_failure_keeps_successful_prefix_and_does_not_execute_suffix(database):
    await database.items.insert_one({"_id": 2})
    operations = [InsertOne({"_id": index}, namespace=database.items.full_name) for index in [1, 2, 3]]
    batcher = AsyncMongoDbWriteBatcher(client=database.client, max_batch_size=3)
    try:
        results = await asyncio.wait_for(
            asyncio.gather(*(batcher.process(op) for op in operations), return_exceptions=True), 15
        )
        assert isinstance(results[0], InsertOneResult)
        assert results[0].inserted_id == 1
        assert isinstance(results[1], DuplicateKeyError)
        assert results[1].code == 11000
        assert isinstance(results[2], MongoDbNotExecutedError)
        assert results[2].failed_index == 1
        assert await database.items.count_documents({}) == 2
        assert await database.items.find_one({"_id": 3}) is None
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
