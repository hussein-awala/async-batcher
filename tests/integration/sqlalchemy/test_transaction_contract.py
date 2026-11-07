from __future__ import annotations

import asyncio
from uuid import uuid4

import pytest
import pytest_asyncio
from async_batcher.sqlalchemy.write import AsyncSqlalchemyWriteBatcher
from sqlalchemy import Column, Integer, MetaData, String, Table, insert, select, text
from sqlalchemy.exc import IntegrityError

pytestmark = [
    pytest.mark.asyncio(loop_scope="session"),
    pytest.mark.integration,
    pytest.mark.integration_postgres,
]


@pytest_asyncio.fixture(loop_scope="session")
async def contract_table(async_postgres_engine):
    metadata = MetaData()
    table = Table(
        f"batcher_{uuid4().hex}",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("token", String, nullable=False, unique=True),
        Column("label", String, server_default=text("'default label'"), nullable=True),
    )
    try:
        async with asyncio.timeout(15), async_postgres_engine.begin() as connection:
            await connection.run_sync(metadata.create_all)
        yield table
    finally:
        async with asyncio.timeout(15), async_postgres_engine.begin() as connection:
            await connection.run_sync(metadata.drop_all)


async def test_returning_preserves_heterogeneous_input_order_defaults_and_explicit_null(
    async_postgres_engine, contract_table
):
    table = contract_table
    parameters = [
        {"token": "first"},
        {"token": "second", "label": None},
        {"token": "third", "label": "explicit"},
        {"token": "fourth"},
    ]
    batcher = AsyncSqlalchemyWriteBatcher(
        model=table,
        async_engine=async_postgres_engine,
        returning=[table.c.id, table.c.token, table.c.label],
        max_batch_size=len(parameters),
    )
    try:
        async with asyncio.timeout(15):
            results = await asyncio.gather(*(batcher.process(item) for item in parameters))
            assert [(row.token, row.label) for row in results] == [
                ("first", "default label"),
                ("second", None),
                ("third", "explicit"),
                ("fourth", "default label"),
            ]
            ids = [row.id for row in results]
            assert len(set(ids)) == len(parameters)
            assert ids == sorted(ids)
            async with async_postgres_engine.connect() as connection:
                stored = (await connection.execute(select(table).order_by(table.c.id))).all()
            assert stored == results
    finally:
        await batcher.stop(force=True, timeout=10)


async def test_later_parameter_group_failure_rolls_back_earlier_group_and_allows_reuse(
    async_postgres_engine, contract_table
):
    table = contract_table
    batcher = AsyncSqlalchemyWriteBatcher(
        model=table,
        async_engine=async_postgres_engine,
        returning=table.c.token,
        max_batch_size=3,
    )
    parameters = [
        {"token": "first"},
        {"token": "second", "label": "explicit"},
        {"token": "first"},
    ]
    try:
        async with asyncio.timeout(15):
            results = await asyncio.gather(
                *(batcher.process(item) for item in parameters), return_exceptions=True
            )
            assert all(isinstance(result, IntegrityError) for result in results)
            async with async_postgres_engine.connect() as connection:
                assert (await connection.execute(select(table))).all() == []
            assert tuple(await batcher.process({"token": "recovered"})) == ("recovered",)
    finally:
        await batcher.stop(force=True, timeout=10)

    # The batcher closes its sessions, while the caller's engine remains usable.
    async with asyncio.timeout(15), async_postgres_engine.connect() as connection:
        assert (await connection.execute(select(table.c.token))).scalars().all() == ["recovered"]


async def wait_for_blocked_insert(engine, blocker_pid, table_name):
    # Observe PostgreSQL's lock state rather than assuming a slow request is blocked.
    async with asyncio.timeout(15), engine.connect() as connection:
        while not await connection.scalar(
            text(
                "SELECT EXISTS (SELECT 1 FROM pg_stat_activity "
                "WHERE :blocker = ANY(pg_blocking_pids(pid)) "
                "AND wait_event_type = 'Lock' AND query LIKE :table)"
            ),
            {"blocker": blocker_pid, "table": f"%{table_name}%"},
        ):
            # pg_stat_activity snapshots are cached for a transaction.
            await connection.rollback()
            await asyncio.sleep(0.01)


@pytest.mark.parametrize("cancel_caller", [False, True], ids=["force-stop", "caller-cancellation"])
async def test_cancellation_of_a_live_blocked_transaction_preserves_ownership(
    async_postgres_engine, contract_table, cancel_caller
):
    table = contract_table
    batcher = AsyncSqlalchemyWriteBatcher(
        model=table,
        async_engine=async_postgres_engine,
        returning=table.c.token,
        max_batch_size=2,
    )
    tasks = []
    async with async_postgres_engine.connect() as blocker:
        transaction = await blocker.begin()
        try:
            blocker_pid = await blocker.scalar(text("SELECT pg_backend_pid()"))
            # An uncommitted unique key blocks the second parameter group, after
            # the batcher's first INSERT has already executed in its transaction.
            await blocker.execute(insert(table), {"token": "blocked"})
            tasks = [
                asyncio.create_task(batcher.process({"token": "before"})),
                asyncio.create_task(batcher.process({"token": "blocked", "label": "second group"})),
            ]
            await wait_for_blocked_insert(async_postgres_engine, blocker_pid, table.name)
            if cancel_caller:
                tasks[0].cancel()
                with pytest.raises(asyncio.CancelledError):
                    await tasks[0]
                assert not tasks[1].done()
                await transaction.rollback()
                assert tuple(await asyncio.wait_for(tasks[1], 15)) == ("blocked",)
                await asyncio.wait_for(batcher.flush(), 15)
                expected = ["before", "blocked"]
            else:
                await batcher.stop(force=True, timeout=10)
                outcomes = await asyncio.gather(*tasks, return_exceptions=True)
                assert all(isinstance(outcome, asyncio.CancelledError) for outcome in outcomes)
                await transaction.rollback()
                expected = []
            async with async_postgres_engine.connect() as connection:
                stored = (
                    (await connection.execute(select(table.c.token).order_by(table.c.token))).scalars().all()
                )
                assert stored == expected
            assert tuple(await asyncio.wait_for(batcher.process({"token": "recovered"}), 15)) == (
                "recovered",
            )
        finally:
            if transaction.is_active:
                await transaction.rollback()
            await batcher.stop(force=True, timeout=10)
            await asyncio.gather(*tasks, return_exceptions=True)
    async with async_postgres_engine.connect() as connection:
        assert await connection.scalar(text("SELECT 1")) == 1
