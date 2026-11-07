from __future__ import annotations

import pytest
import pytest_asyncio

pytest.importorskip("sqlalchemy")
pytest.importorskip("aiosqlite")

from async_batcher.sqlalchemy.write import AsyncSqlalchemyWriteBatcher  # noqa: E402

from sqlalchemy import Column, Integer, MetaData, String, Table  # noqa: E402
from sqlalchemy.ext.asyncio import create_async_engine  # noqa: E402


@pytest_asyncio.fixture
async def database():
    engine = create_async_engine("sqlite+aiosqlite://")
    table = Table("records", MetaData(), Column("id", Integer, primary_key=True), Column("name", String))
    async with engine.begin() as connection:
        await connection.run_sync(table.create)
    try:
        yield engine, table
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_returning_accepts_a_sql_expression(database):
    engine, table = database
    batcher = AsyncSqlalchemyWriteBatcher(model=table, async_engine=engine, returning=table.c.id + 1)
    assert await batcher.process_batch([{"id": 30, "name": "One"}]) == [(31,)]


@pytest.mark.parametrize("container", [list, tuple])
@pytest.mark.asyncio
async def test_returning_preserves_input_order(database, container):
    engine, table = database
    batcher = AsyncSqlalchemyWriteBatcher(
        model=table, async_engine=engine, returning=container([table.c.id, table.c.name])
    )
    batch = [{"id": i, "name": f"Name {i}"} for i in [9, 1, 5, 2]]
    assert await batcher.process_batch(batch) == [(i, f"Name {i}") for i in [9, 1, 5, 2]]


def test_bulk_update_rejects_returning_before_execution():
    table = Table("records", MetaData(), Column("id", Integer, primary_key=True))
    with pytest.raises(ValueError, match="RETURNING is not supported for bulk UPDATE"):
        AsyncSqlalchemyWriteBatcher(model=table, async_engine=None, operation="update", returning=table.c.id)
