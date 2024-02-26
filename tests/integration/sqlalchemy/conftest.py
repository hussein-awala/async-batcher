from __future__ import annotations

from typing import TYPE_CHECKING

import pytest_asyncio

from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from tests.conftest import _TestModel

if TYPE_CHECKING:
    from sqlalchemy.orm import DeclarativeBase


@pytest_asyncio.fixture(scope="session")
async def async_postgres_engine() -> AsyncEngine:
    async_engine = create_async_engine("postgresql+asyncpg://postgres:password@localhost/postgres")
    yield async_engine
    await async_engine.dispose()


@pytest_asyncio.fixture(scope="session")
async def create_models(async_postgres_engine) -> DeclarativeBase:
    async with async_postgres_engine.begin() as conn:
        await conn.run_sync(_TestModel.metadata.create_all)
    yield _TestModel
    async with async_postgres_engine.begin() as conn:
        await conn.run_sync(_TestModel.metadata.drop_all)
