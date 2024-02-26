from __future__ import annotations

import pytest_asyncio

from sqlalchemy import String
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class BaseModel(DeclarativeBase):
    pass


class TestModel(BaseModel):
    __tablename__ = "test_table"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(30))
    age: Mapped[int] = mapped_column()


@pytest_asyncio.fixture(scope="session")
async def async_sqlite_engine() -> AsyncEngine:
    async_engine = create_async_engine("sqlite+aiosqlite://")
    yield async_engine
    await async_engine.dispose()


@pytest_asyncio.fixture(scope="session")
async def async_session_maker(async_sqlite_engine) -> async_sessionmaker[AsyncSession]:
    yield async_sessionmaker(bind=async_sqlite_engine)


@pytest_asyncio.fixture(scope="session")
async def create_models(async_sqlite_engine) -> DeclarativeBase:
    async with async_sqlite_engine.begin() as conn:
        await conn.run_sync(TestModel.metadata.create_all)
    yield TestModel
    async with async_sqlite_engine.begin() as conn:
        await conn.run_sync(TestModel.metadata.drop_all)
