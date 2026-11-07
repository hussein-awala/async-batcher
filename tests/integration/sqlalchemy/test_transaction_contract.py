from __future__ import annotations

import asyncio
from uuid import uuid4

import pytest
import pytest_asyncio
from async_batcher.sqlalchemy.write import AsyncSqlalchemyWriteBatcher
from sqlalchemy import Column, Integer, MetaData, String, Table, select, text
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
