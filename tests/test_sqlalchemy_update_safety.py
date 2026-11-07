from __future__ import annotations

import pytest
import pytest_asyncio

pytest.importorskip("sqlalchemy")
pytest.importorskip("aiosqlite")

from async_batcher.sqlalchemy.write import AsyncSqlalchemyWriteBatcher  # noqa: E402

from sqlalchemy import Column, Integer, MetaData, String, Table, select  # noqa: E402
from sqlalchemy.exc import InvalidRequestError  # noqa: E402
from sqlalchemy.ext.asyncio import create_async_engine  # noqa: E402
from sqlalchemy.orm import registry  # noqa: E402
from sqlalchemy.orm.exc import StaleDataError  # noqa: E402


@pytest_asyncio.fixture
async def database():
    engine = create_async_engine("sqlite+aiosqlite://")
    table = Table("records", MetaData(), Column("id", Integer, primary_key=True), Column("name", String))
    async with engine.begin() as connection:
        await connection.run_sync(table.create)
        await connection.execute(table.insert(), [{"id": 1, "name": "One"}, {"id": 2, "name": "Two"}])
    try:
        yield engine, table
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_core_table_update_is_rejected_before_it_can_change_every_row(database):
    engine, table = database
    with pytest.raises(ValueError, match="Bulk UPDATE requires a mapped ORM class"):
        batcher = AsyncSqlalchemyWriteBatcher(model=table, async_engine=engine, operation="update")
        await batcher.process_batch([{"name": "changed"}])
    async with engine.connect() as connection:
        assert (await connection.execute(select(table).order_by(table.c.id))).all() == [
            (1, "One"),
            (2, "Two"),
        ]


@pytest.mark.parametrize("include_primary_key", [False, True])
@pytest.mark.asyncio
async def test_orm_update_requires_and_uses_each_items_primary_key(database, include_primary_key):
    engine, table = database

    class Record:
        pass

    mapper_registry = registry()
    mapper_registry.map_imperatively(Record, table)
    try:
        batcher = AsyncSqlalchemyWriteBatcher(model=Record, async_engine=engine, operation="update")
        if include_primary_key:
            await batcher.process_batch([{"id": 1, "name": "changed"}])
        else:
            with pytest.raises((InvalidRequestError, StaleDataError)):
                await batcher.process_batch([{"name": "changed"}])
        async with engine.connect() as connection:
            rows = (await connection.execute(select(table).order_by(table.c.id))).all()
        assert rows == [(1, "changed" if include_primary_key else "One"), (2, "Two")]
    finally:
        mapper_registry.dispose()
