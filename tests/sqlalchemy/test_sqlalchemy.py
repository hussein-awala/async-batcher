from __future__ import annotations

import asyncio

import pytest
from async_batcher.sqlalchemy.write import AsyncSqlalchemyWriteBatcher

from sqlalchemy import select
from tests.sqlalchemy.conftest import _TestModel


@pytest.mark.asyncio(scope="session")
async def test_async_sqlalchemy_write_batcher(async_sqlite_engine, create_models):
    insert_batcher = AsyncSqlalchemyWriteBatcher(
        model=_TestModel,
        async_engine=async_sqlite_engine,
        operation="insert",
        returning=[_TestModel.name, _TestModel.age],
    )
    update_batcher = AsyncSqlalchemyWriteBatcher(
        model=_TestModel,
        async_engine=async_sqlite_engine,
        operation="update",
    )
    # 10 inserts
    returned_result = await asyncio.gather(
        *[insert_batcher.process({"id": i, "name": f"Name {i}", "age": i}) for i in range(10)]
    )
    # check returned result
    assert returned_result == [(f"Name {i}", i) for i in range(10)]
    # read all rows from the table and validate them
    async with insert_batcher.async_session_maker() as session:
        rows_in_table = (await session.scalars(select(_TestModel))).all()
    assert len(rows_in_table) == 10
    for i, row in enumerate(rows_in_table):
        assert row.id == i
        assert row.name == f"Name {i}"
        assert row.age == i
    # 10 updates
    await asyncio.gather(*[update_batcher.process({"id": i, "age": i * 2}) for i in range(10)])
    # read all rows from the table and validate them
    async with update_batcher.async_session_maker() as session:
        rows_in_table = (await session.scalars(select(_TestModel))).all()
    assert len(rows_in_table) == 10
    for i, row in enumerate(rows_in_table):
        assert row.id == i
        assert row.name == f"Name {i}"
        assert row.age == i * 2
    # stop the batcher
    await insert_batcher.stop()
    await update_batcher.stop()
