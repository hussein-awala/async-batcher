from __future__ import annotations

import asyncio
import random

import pytest
from async_batcher.aws.dynamodb.get import AsyncDynamoDbGetBatcher, GetItem
from async_batcher.aws.dynamodb.write import AsyncDynamoDbWriteBatcher, WriteOperation


@pytest.mark.asyncio
async def test_get_items(get_batcher: AsyncDynamoDbGetBatcher, write_batcher: AsyncDynamoDbWriteBatcher):
    tasks = []
    for i in range(0, 20, 2):
        tasks.append(
            write_batcher.process(
                item=WriteOperation(
                    table_name="test-table", operation="PUT", data={"Key": str(i), "Value": i}
                )
            )
        )
        tasks.append(
            write_batcher.process(
                item=WriteOperation(
                    table_name="multi-keys-table",
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
                ("test-table", i),
                get_batcher.process(item=GetItem(table_name="test-table", key={"Key": str(i)})),
            )
        )
        tasks.append(
            (
                ("multi-keys-table", i),
                get_batcher.process(
                    item=GetItem(table_name="multi-keys-table", key={"key1": str(i), "key2": str(i * 2)})
                ),
            )
        )
    random.shuffle(tasks)
    results = await asyncio.gather(*[task[1] for task in tasks])

    for ind, result in enumerate(results):
        table, i = tasks[ind][0]
        if table == "test-table":
            if i % 2 == 0:
                assert result == {"Key": str(i), "Value": i}
            else:
                assert result is None
        else:
            if i % 2 == 0:
                assert result == {"key1": str(i), "key2": str(i * 2), "value": i * 3}
            else:
                assert result is None
