from __future__ import annotations

import asyncio
from contextlib import suppress
from typing import TYPE_CHECKING
from uuid import uuid4

import aioboto3
import pytest
import pytest_asyncio
from async_batcher.aws.dynamodb.get import AsyncDynamoDbGetBatcher
from async_batcher.aws.dynamodb.write import AsyncDynamoDbWriteBatcher

if TYPE_CHECKING:
    from types_aiobotocore_dynamodb import DynamoDBServiceResource


@pytest.fixture
def dynamodb_aioboto3_session():
    yield aioboto3.Session(
        region_name="us-west-2",
        aws_access_key_id="DUMMYIDEXAMPLE",
        aws_secret_access_key="DUMMYEXAMPLEKEY",
    )


@pytest_asyncio.fixture
async def get_batcher(dynamodb_aioboto3_session):
    batcher = AsyncDynamoDbGetBatcher(
        endpoint_url="http://localhost:8000",
        aioboto3_session=dynamodb_aioboto3_session,
        max_queue_time=2,
    )
    yield batcher
    await batcher.stop(force=True, timeout=10)


@pytest_asyncio.fixture
async def write_batcher(dynamodb_aioboto3_session):
    batcher = AsyncDynamoDbWriteBatcher(
        endpoint_url="http://localhost:8000",
        aioboto3_session=dynamodb_aioboto3_session,
        max_queue_time=2,
    )
    yield batcher
    await batcher.stop(force=True, timeout=10)


@pytest_asyncio.fixture
async def dynamodb_tables(dynamodb_aioboto3_session):
    table1_name = f"batcher-{uuid4().hex}"
    table2_name = f"batcher-{uuid4().hex}"

    async def _create_tables(_aioboto3_session):
        _dynamodb: DynamoDBServiceResource
        async with _aioboto3_session.resource(
            "dynamodb",
            endpoint_url="http://localhost:8000",
        ) as _dynamodb:
            await _dynamodb.create_table(
                TableName=table1_name,
                KeySchema=[
                    {"AttributeName": "key", "KeyType": "HASH"},
                ],
                AttributeDefinitions=[
                    {"AttributeName": "key", "AttributeType": "S"},
                ],
                ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
                TableClass="STANDARD",
            )
            await _dynamodb.create_table(
                TableName=table2_name,
                KeySchema=[
                    {"AttributeName": "key1", "KeyType": "HASH"},
                    {"AttributeName": "key2", "KeyType": "RANGE"},
                ],
                AttributeDefinitions=[
                    {"AttributeName": "key1", "AttributeType": "S"},
                    {"AttributeName": "key2", "AttributeType": "S"},
                ],
                ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
                TableClass="STANDARD",
            )

    async def _delete_tables(_aioboto3_session):
        _dynamodb: DynamoDBServiceResource
        async with _aioboto3_session.resource(
            "dynamodb",
            endpoint_url="http://localhost:8000",
        ) as _dynamodb:
            with suppress(_dynamodb.meta.client.exceptions.ResourceNotFoundException):
                await (await _dynamodb.Table(table1_name)).delete()
            with suppress(_dynamodb.meta.client.exceptions.ResourceNotFoundException):
                await (await _dynamodb.Table(table2_name)).delete()

    try:
        async with asyncio.timeout(30):
            await _create_tables(dynamodb_aioboto3_session)
        yield table1_name, table2_name
    finally:
        async with asyncio.timeout(15):
            await _delete_tables(dynamodb_aioboto3_session)


@pytest_asyncio.fixture
async def dynamodb_binary_table(dynamodb_aioboto3_session):
    name = f"batcher-binary-{uuid4().hex}"
    async with dynamodb_aioboto3_session.resource("dynamodb", endpoint_url="http://localhost:8000") as db:
        table = None
        try:
            async with asyncio.timeout(30):
                table = await db.create_table(
                    TableName=name,
                    KeySchema=[
                        {"AttributeName": "key", "KeyType": "HASH"},
                        {"AttributeName": "version", "KeyType": "RANGE"},
                    ],
                    AttributeDefinitions=[
                        {"AttributeName": "key", "AttributeType": "B"},
                        {"AttributeName": "version", "AttributeType": "N"},
                    ],
                    BillingMode="PAY_PER_REQUEST",
                )
                await table.wait_until_exists()
            yield name
        finally:
            if table is not None:
                async with asyncio.timeout(15):
                    await table.delete()
