from __future__ import annotations

import asyncio
import random

import pytest
from async_batcher.aws.dynamodb.get import AsyncDynamoDbGetBatcher, GetItem
from async_batcher.aws.dynamodb.write import AsyncDynamoDbWriteBatcher, WriteOperation

pytestmark = [pytest.mark.integration, pytest.mark.integration_dynamodb]


@pytest.mark.asyncio
async def test_dynamodb_batchers(
    dynamodb_tables: tuple[str, str],
    get_batcher: AsyncDynamoDbGetBatcher,
    write_batcher: AsyncDynamoDbWriteBatcher,
):
    tasks = []
    for i in range(0, 20, 2):
        tasks.append(
            write_batcher.process(
                item=WriteOperation(
                    table_name=dynamodb_tables[0], operation="PUT", data={"key": str(i), "value": i}
                )
            )
        )
        tasks.append(
            write_batcher.process(
                item=WriteOperation(
                    table_name=dynamodb_tables[1],
                    operation="PUT",
                    data={"key1": str(i), "key2": str(i * 2), "value": i * 3},
                )
            )
        )
    await asyncio.gather(*tasks)

    tasks = []
    for i in range(20):
        tasks.append(
            (
                (dynamodb_tables[0], i),
                get_batcher.process(item=GetItem(table_name=dynamodb_tables[0], key={"key": str(i)})),
            )
        )
        tasks.append(
            (
                (dynamodb_tables[1], i),
                get_batcher.process(
                    item=GetItem(table_name=dynamodb_tables[1], key={"key1": str(i), "key2": str(i * 2)})
                ),
            )
        )
    random.shuffle(tasks)
    results = await asyncio.gather(*[task[1] for task in tasks])

    for ind, result in enumerate(results):
        table, i = tasks[ind][0]
        if table == dynamodb_tables[0]:
            if i % 2 == 0:
                assert result == {"key": str(i), "value": i}
            else:
                assert result is None
        else:
            if i % 2 == 0:
                assert result == {"key1": str(i), "key2": str(i * 2), "value": i * 3}
            else:
                assert result is None


@pytest.mark.asyncio
async def test_dynamodb_strongly_consistent_read(dynamodb_tables, dynamodb_aioboto3_session, write_batcher):
    table = dynamodb_tables[0]
    item = {"key": "consistent-read", "value": "written"}
    await write_batcher.process(WriteOperation(table_name=table, operation="PUT", data=item))
    reader = AsyncDynamoDbGetBatcher(
        endpoint_url="http://localhost:8000",
        aioboto3_session=dynamodb_aioboto3_session,
        consistent_read=True,
    )
    try:
        assert await reader.process(GetItem(table_name=table, key={"key": item["key"]})) == item
    finally:
        await reader.stop()
