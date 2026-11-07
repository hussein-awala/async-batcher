from __future__ import annotations

import pytest
from sqlalchemy import insert, select

from tests.adapters.sqlalchemy.models import _TestModel
from tests.integration.sqlalchemy.conftest import async_postgres_engine as engine_fixture

pytestmark = [
    pytest.mark.asyncio(loop_scope="session"),
    pytest.mark.integration,
    pytest.mark.integration_postgres,
]


async def test_independent_fixture_runs_preserve_each_others_tables():
    first_run = engine_fixture.__wrapped__()
    second_run = engine_fixture.__wrapped__()
    try:
        first = await anext(first_run)
        second = await anext(second_run)
        for engine, name in [(first, "first run"), (second, "second run")]:
            async with engine.begin() as connection:
                await connection.run_sync(_TestModel.metadata.create_all)
                await connection.execute(insert(_TestModel), {"id": 1, "name": name, "age": 0})
        async with second.connect() as connection:
            assert (await connection.execute(select(_TestModel.name))).scalars().all() == ["second run"]
        await second_run.aclose()
        async with first.connect() as connection:
            assert (await connection.execute(select(_TestModel.name))).scalars().all() == ["first run"]
    finally:
        await second_run.aclose()
        await first_run.aclose()
