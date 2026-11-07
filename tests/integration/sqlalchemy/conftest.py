from __future__ import annotations

from typing import TYPE_CHECKING
from uuid import uuid4

import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.schema import CreateSchema, DropSchema

from tests.adapters.sqlalchemy.models import _TestModel

if TYPE_CHECKING:
    from sqlalchemy.orm import DeclarativeBase


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def async_postgres_engine() -> AsyncEngine:
    schema = f"batcher_{uuid4().hex}"
    async_engine = create_async_engine(
        "postgresql+asyncpg://postgres:password@localhost/postgres",
        execution_options={"schema_translate_map": {None: schema}},
    )
    created = False
    try:
        async with async_engine.begin() as connection:
            await connection.execute(CreateSchema(schema))
        created = True
        yield async_engine
    finally:
        try:
            if created:
                async with async_engine.begin() as connection:
                    await connection.execute(DropSchema(schema, cascade=True))
        finally:
            await async_engine.dispose()


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def create_models(async_postgres_engine) -> DeclarativeBase:
    try:
        async with async_postgres_engine.begin() as conn:
            await conn.run_sync(_TestModel.metadata.create_all)
        yield _TestModel
    finally:
        async with async_postgres_engine.begin() as conn:
            await conn.run_sync(_TestModel.metadata.drop_all)
