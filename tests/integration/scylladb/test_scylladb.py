from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING
from uuid import uuid4

import pytest
from async_batcher.scylladb.update import AsyncScyllaDbWriteBatcher, WriteOperation

if TYPE_CHECKING:
    from cassandra.cqlengine.models import Model


@pytest.mark.asyncio
async def test_scylladb_batchers(
    scylladb_tables: tuple[Model, Model], scylladb_write_batcher: AsyncScyllaDbWriteBatcher
):
    tasks = []
    ids = [uuid4() for _ in range(20)]
    for i in range(10):
        tasks.append(
            scylladb_write_batcher.process(
                item=WriteOperation(
                    operation="INSERT",
                    model=scylladb_tables[0],
                    data={"id": ids[i], "attr1": str(i), "attr2": i * 2},
                )
            )
        )
    for i in range(10):
        tasks.append(
            scylladb_write_batcher.process(
                item=WriteOperation(
                    operation="INSERT",
                    model=scylladb_tables[1],
                    data={"id": ids[i + 10], "attr1": str(i * 2), "attr2": i * 3},
                )
            )
        )
    await asyncio.gather(*tasks)
    assert scylladb_tables[0].objects.count() == 10
    assert scylladb_tables[1].objects.count() == 10
    for i in range(10):
        row = scylladb_tables[0].objects(id=ids[i]).first()
        assert row.id == ids[i]
        assert row.attr1 == str(i)
        assert row.attr2 == i * 2
    for i in range(10):
        row = scylladb_tables[1].objects(id=ids[i + 10]).first()
        assert row.id == ids[i + 10]
        assert row.attr1 == str(i * 2)
        assert row.attr2 == i * 3

    tasks = []
    for i in range(5):
        tasks.append(
            scylladb_write_batcher.process(
                item=WriteOperation(
                    operation="DELETE",
                    model=scylladb_tables[0],
                    key={"id": ids[i]},
                )
            )
        )
    for i in range(5, 8):
        tasks.append(
            scylladb_write_batcher.process(
                item=WriteOperation(
                    operation="UPDATE",
                    model=scylladb_tables[0],
                    key={"id": ids[i]},
                    data={"attr2": i * 4},
                )
            )
        )
    for i in range(7):
        tasks.append(
            scylladb_write_batcher.process(
                item=WriteOperation(
                    operation="INSERT",
                    model=scylladb_tables[0],
                    data={"id": ids[i + 10], "attr1": str(i * 2), "attr2": i * 3},
                )
            )
        )
    await asyncio.gather(*tasks)
    assert scylladb_tables[0].objects.count() == 12
    for i in range(5):
        assert scylladb_tables[0].objects(id=ids[i]).first() is None
    for i in range(5, 8):
        row = scylladb_tables[0].objects(id=ids[i]).first()
        assert row.id == ids[i]
        assert row.attr1 == str(i)
        assert row.attr2 == i * 4
    for i in range(7):
        row = scylladb_tables[0].objects(id=ids[i + 10]).first()
        assert row.id == ids[i + 10]
        assert row.attr1 == str(i * 2)
        assert row.attr2 == i * 3


@pytest.mark.asyncio
async def test_scylladb_batchers_exceptions(
    scylladb_tables: tuple[Model, Model], scylladb_write_batcher: AsyncScyllaDbWriteBatcher
):
    scylladb_write_batcher.buffering_time = 0.1
    with pytest.raises(ValueError, match="key must be provided for DELETE operations"):
        await asyncio.gather(
            scylladb_write_batcher.process(
                item=WriteOperation(
                    operation="DELETE",
                    model=scylladb_tables[0],
                )
            )
        )
    with pytest.raises(ValueError, match="data must be provided for INSERT operations"):
        await asyncio.gather(
            scylladb_write_batcher.process(
                item=WriteOperation(
                    operation="INSERT",
                    model=scylladb_tables[0],
                )
            )
        )
    with pytest.raises(ValueError, match="key and data must be provided for UPDATE operations"):
        await asyncio.gather(
            scylladb_write_batcher.process(
                item=WriteOperation(
                    operation="UPDATE",
                    model=scylladb_tables[0],
                )
            )
        )
    with pytest.raises(ValueError, match="key and data must be provided for UPDATE operations"):
        await asyncio.gather(
            scylladb_write_batcher.process(
                item=WriteOperation(
                    operation="UPDATE",
                    model=scylladb_tables[0],
                    data={"attr2": 8},
                )
            )
        )
    with pytest.raises(ValueError, match="key and data must be provided for UPDATE operations"):
        await asyncio.gather(
            scylladb_write_batcher.process(
                item=WriteOperation(
                    operation="UPDATE",
                    model=scylladb_tables[0],
                    key={"id": uuid4()},
                )
            )
        )
