from __future__ import annotations

import asyncio

import pytest
import pytest_asyncio

pytest.importorskip("sqlalchemy")
pytest.importorskip("aiosqlite")

from async_batcher.sqlalchemy.write import AsyncSqlalchemyWriteBatcher  # noqa: E402
from sqlalchemy import Column, Integer, MetaData, String, Table, select  # noqa: E402
from sqlalchemy.exc import IntegrityError  # noqa: E402
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine  # noqa: E402


@pytest_asyncio.fixture
async def database():
    engine = create_async_engine("sqlite+aiosqlite://")
    table = Table(
        "records",
        MetaData(),
        Column("id", Integer, primary_key=True),
        Column("name", String, server_default="fallback"),
    )
    async with engine.begin() as connection:
        await connection.run_sync(table.create)
    try:
        yield engine, table
    finally:
        await engine.dispose()


@pytest.mark.parametrize("returning", [False, True])
@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.asyncio
async def test_core_inserts_preserve_each_items_columns_and_result_order(database, returning, reverse):
    engine, table = database
    batch = [{"id": 7}, {"id": 2, "name": "Two"}, {"id": 8, "name": None}, {"id": 1, "name": "One"}]
    if reverse:
        batch.reverse()
    expected = [(item["id"], item.get("name", "fallback")) for item in batch]
    batcher = AsyncSqlalchemyWriteBatcher(
        model=table, async_engine=engine, returning=[table.c.id, table.c.name] if returning else None
    )
    result = await batcher.process_batch(batch)
    assert result == (expected if returning else None)
    async with engine.connect() as connection:
        assert (await connection.execute(select(table).order_by(table.c.id))).all() == sorted(expected)


@pytest.mark.asyncio
async def test_default_only_rows_keep_their_position_between_other_insert_shapes(database):
    engine, table = database
    batcher = AsyncSqlalchemyWriteBatcher(
        model=table, async_engine=engine, returning=[table.c.id, table.c.name]
    )
    assert await batcher.process_batch([{}, {"name": "named"}, {}]) == [
        (1, "fallback"),
        (2, "named"),
        (3, "fallback"),
    ]


@pytest.mark.parametrize("returning", [False, True])
@pytest.mark.asyncio
async def test_later_insert_failure_rolls_back_every_parameter_group(database, returning):
    engine, table = database
    batcher = AsyncSqlalchemyWriteBatcher(
        model=table, async_engine=engine, returning=[table.c.id] if returning else None
    )
    with pytest.raises(IntegrityError):
        await batcher.process_batch([{"id": 1}, {"id": 2, "name": "Two"}, {"id": 1, "name": "duplicate"}])
    async with engine.connect() as connection:
        assert (await connection.execute(select(table))).all() == []


@pytest.mark.asyncio
async def test_cancellation_between_insert_groups_rolls_back_earlier_rows(database):
    engine, table = database
    second_group_started = asyncio.Event()
    release_second_group = asyncio.Event()

    class GatedSession(AsyncSession):
        async def execute(self, *args, **kwargs):
            if kwargs["params"][0]["id"] == 2:
                second_group_started.set()
                await release_second_group.wait()
            return await super().execute(*args, **kwargs)

    batcher = AsyncSqlalchemyWriteBatcher(model=table, async_engine=engine)
    batcher.async_session_maker = async_sessionmaker(engine, class_=GatedSession)
    task = asyncio.create_task(batcher.process_batch([{"id": 1}, {"id": 2, "name": "Two"}]))
    try:
        await asyncio.wait_for(second_group_started.wait(), 2)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        async with engine.connect() as connection:
            assert (await connection.execute(select(table))).all() == []
    finally:
        release_second_group.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
