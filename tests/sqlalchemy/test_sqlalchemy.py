from __future__ import annotations

import asyncio

import pytest
from async_batcher.sqlalchemy.write import AsyncSqlalchemyWriteBatcher

from sqlalchemy import select
from tests.sqlalchemy.conftest import TestModel


@pytest.mark.asyncio(scope="session")
async def test_async_sqlalchemy_write_batcher(async_session_maker, create_models):
    insert_batcher = AsyncSqlalchemyWriteBatcher(
        model=TestModel,
        async_session_maker=async_session_maker,
        operation="insert",
        returning=[TestModel.name, TestModel.age],
    )
    update_batcher = AsyncSqlalchemyWriteBatcher(
        model=TestModel,
        async_session_maker=async_session_maker,
        operation="update",
    )
    # 10 inserts
    returned_result = await asyncio.gather(
        *[insert_batcher.process({"id": i, "name": f"Name {i}", "age": i}) for i in range(10)]
    )
    # check returned result
    assert returned_result == [(f"Name {i}", i) for i in range(10)]
    # read all rows from the table and validate them
    async with insert_batcher.create_async_session() as session:
        rows_in_table = (await session.scalars(select(TestModel))).all()
        await session.commit()
    assert len(rows_in_table) == 10
    assert rows_in_table == [TestModel(id=i, name=f"Name {i}", age=i) for i in range(10)]
    # 10 updates
    await asyncio.gather(*[update_batcher.process({"id": i, "age": i * 2}) for i in range(10)])
    # read all rows from the table and validate them
    async with update_batcher.create_async_session() as session:
        rows_in_table = (await session.scalars(select(TestModel))).all()
        await session.commit()
    assert len(rows_in_table) == 10
    assert rows_in_table == [TestModel(id=i, name=f"Name {i}", age=i) for i in range(10)]
    # stop the batcher
    await insert_batcher.stop()
    await update_batcher.stop()
