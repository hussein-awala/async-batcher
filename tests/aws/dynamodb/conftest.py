from __future__ import annotations

import aioboto3
import pytest
from async_batcher.aws.dynamodb.get import AsyncDynamoDbGetBatcher
from async_batcher.aws.dynamodb.write import AsyncDynamoDbWriteBatcher


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
