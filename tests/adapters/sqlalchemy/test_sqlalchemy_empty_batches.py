from __future__ import annotations

import pytest

pytest.importorskip("sqlalchemy")
pytest.importorskip("aiosqlite")

from async_batcher.sqlalchemy.write import AsyncSqlalchemyWriteBatcher  # noqa: E402
from sqlalchemy import Column, Integer, MetaData, Table, func, select  # noqa: E402
from sqlalchemy.ext.asyncio import create_async_engine  # noqa: E402


@pytest.mark.parametrize(
    "returning_kind", ["none", "empty_list", "empty_tuple", "column", "list", "expression"]
)
@pytest.mark.asyncio
async def test_empty_batch_does_not_insert_a_default_row(returning_kind):
    engine = create_async_engine("sqlite+aiosqlite://")
    table = Table("default_rows", MetaData(), Column("id", Integer, primary_key=True))
    returning = {
        "none": None,
        "empty_list": [],
        "empty_tuple": (),
        "column": table.c.id,
        "list": [table.c.id],
        "expression": table.c.id + 1,
    }[returning_kind]
    try:
        async with engine.begin() as connection:
            await connection.run_sync(table.create)
        batcher = AsyncSqlalchemyWriteBatcher(model=table, async_engine=engine, returning=returning)
        expected = None if returning_kind in {"none", "empty_list", "empty_tuple"} else []
        assert await batcher.process_batch([]) == expected
        async with engine.connect() as connection:
            assert await connection.scalar(select(func.count()).select_from(table)) == 0
    finally:
        await engine.dispose()
