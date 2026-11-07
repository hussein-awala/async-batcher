from __future__ import annotations

import asyncio
from decimal import Decimal

import pytest
from async_batcher.aws.dynamodb.get import AsyncDynamoDbGetBatcher, GetItem
from async_batcher.aws.dynamodb.write import WriteOperation
from boto3.dynamodb.types import Binary
from botocore.exceptions import ClientError

pytestmark = [pytest.mark.asyncio, pytest.mark.integration, pytest.mark.integration_dynamodb]


async def test_binary_numeric_keys_route_duplicates_missing_rows_and_deletes(
    dynamodb_binary_table, dynamodb_aioboto3_session, write_batcher
):
    table = dynamodb_binary_table
    rows = [
        {"key": b"\x00\xff", "version": Decimal("0"), "value": "first"},
        {"key": b"other", "version": Decimal("2"), "value": "second"},
    ]
    reader = AsyncDynamoDbGetBatcher(
        endpoint_url="http://localhost:8000",
        aioboto3_session=dynamodb_aioboto3_session,
        consistent_read=True,
    )
    keys = [
        {"key": bytearray(b"other"), "version": 2},
        {"version": Decimal("0.0"), "key": Binary(b"\x00\xff")},
        {"key": b"\x00\xff", "version": 0},
        {"key": b"missing", "version": 0},
    ]
    try:
        async with asyncio.timeout(30):
            assert await asyncio.gather(
                *(
                    write_batcher.process(WriteOperation(table_name=table, operation="PUT", data=r))
                    for r in rows
                )
            ) == [None, None]
            assert await asyncio.gather(
                *(reader.process(GetItem(table_name=table, key=key)) for key in keys)
            ) == [rows[1], rows[0], rows[0], None]
            assert await asyncio.gather(
                *(
                    write_batcher.process(WriteOperation(table_name=table, operation="DELETE", data=key))
                    for key in (keys[1], keys[3])
                )
            ) == [None, None]
            assert await asyncio.gather(
                *(reader.process(GetItem(table_name=table, key=key)) for key in keys)
            ) == [rows[1], None, None, None]
    finally:
        await reader.stop(force=True, timeout=10)


async def test_duplicate_write_rejection_is_reported_and_the_batcher_recovers(
    dynamodb_tables, dynamodb_aioboto3_session, write_batcher
):
    table = dynamodb_tables[0]
    reader = AsyncDynamoDbGetBatcher(
        endpoint_url="http://localhost:8000",
        aioboto3_session=dynamodb_aioboto3_session,
        consistent_read=True,
    )
    try:
        async with asyncio.timeout(30):
            results = await asyncio.gather(
                *(
                    write_batcher.process(
                        WriteOperation(
                            table_name=table, operation="PUT", data={"key": "duplicate", "value": n}
                        )
                    )
                    for n in (1, 2)
                ),
                return_exceptions=True,
            )
            assert all(isinstance(result, ClientError) for result in results)
            assert [result.response["Error"]["Code"] for result in results] == ["ValidationException"] * 2
            key = GetItem(table_name=table, key={"key": "duplicate"})
            assert await reader.process(key) is None
            row = {"key": "duplicate", "value": "recovered"}
            assert (
                await write_batcher.process(WriteOperation(table_name=table, operation="PUT", data=row))
                is None
            )
            assert await reader.process(key) == row
    finally:
        await reader.stop(force=True, timeout=10)
