from __future__ import annotations

import asyncio
import gc
import weakref

import pytest

pytest.importorskip("sqlalchemy")
pytest.importorskip("aiosqlite")

from async_batcher.sqlalchemy.write import AsyncSqlalchemyWriteBatcher  # noqa: E402
from sqlalchemy import Column, Integer, MetaData, Table, select  # noqa: E402
from sqlalchemy.ext.asyncio import create_async_engine  # noqa: E402


@pytest.mark.asyncio
async def test_finished_batches_do_not_retain_their_tasks():
    engine = create_async_engine("sqlite+aiosqlite://")
    table = Table("records", MetaData(), Column("id", Integer, primary_key=True))
    try:
        async with engine.begin() as connection:
            await connection.run_sync(table.create)
        batcher = AsyncSqlalchemyWriteBatcher(model=table, async_engine=engine)
        task_refs = []
        for i in range(3):
            task = asyncio.create_task(batcher.process_batch([{"id": i}]))
            task_refs.append(weakref.ref(task))
            await task
            del task
        await asyncio.sleep(0)
        gc.collect()
        assert all(task_ref() is None for task_ref in task_refs)
        async with batcher.async_session_maker() as session:
            assert len((await session.execute(select(table))).all()) == 3
    finally:
        await engine.dispose()
