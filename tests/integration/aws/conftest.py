from __future__ import annotations

import asyncio
from contextlib import suppress
from typing import TYPE_CHECKING

import aioboto3
import pytest
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


@pytest.fixture
def get_batcher(dynamodb_aioboto3_session):
    batcher = AsyncDynamoDbGetBatcher(
        endpoint_url="http://localhost:8000",
        aioboto3_session=dynamodb_aioboto3_session,
        buffering_time=2,
    )
    batcher.start()
    yield batcher
    batcher.stop()


@pytest.fixture
def write_batcher(dynamodb_aioboto3_session):
    batcher = AsyncDynamoDbWriteBatcher(
        endpoint_url="http://localhost:8000",
        aioboto3_session=dynamodb_aioboto3_session,
        buffering_time=2,
    )
    batcher.start()
    yield batcher
    batcher.stop()


@pytest.fixture
def dynamodb_tables(dynamodb_aioboto3_session):
    table1_name = "test_table1"
    table2_name = "test_table2"

    async def _create_tables(_aioboto3_session):
        _dynamodb: DynamoDBServiceResource
        async with _aioboto3_session.resource(
            "dynamodb",
            endpoint_url="http://localhost:8000",
        ) as _dynamodb:
            await _delete_tables(_aioboto3_session)
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

    asyncio.run(_create_tables(dynamodb_aioboto3_session))
    yield table1_name, table2_name
    asyncio.run(_delete_tables(dynamodb_aioboto3_session))
