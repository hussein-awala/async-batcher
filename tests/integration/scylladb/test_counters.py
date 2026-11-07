from __future__ import annotations

import asyncio
from uuid import uuid4

import pytest
from async_batcher.scylladb.update import AsyncScyllaDbWriteBatcher, WriteOperation

pytestmark = [pytest.mark.integration, pytest.mark.integration_scylladb]


@pytest.mark.asyncio
async def test_counter_batches_increment_and_delete(scylladb_counter_table):
    model = scylladb_counter_table
    ids = [uuid4(), uuid4()]
    batcher = AsyncScyllaDbWriteBatcher(max_batch_size=2, max_queue_time=0.01)
    try:
        for increments in [(2, 3), (4, 5)]:
            assert await asyncio.gather(
                *(
                    batcher.process(
                        WriteOperation(
                            operation="UPDATE", model=model, key={"id": key}, data={"total": value}
                        )
                    )
                    for key, value in zip(ids, increments, strict=True)
                )
            ) == [None, None]

        assert [model.objects(id=key).get().total for key in ids] == [6, 8]
        deletion = WriteOperation(operation="DELETE", model=model, key={"id": ids[0]})
        assert await batcher.process(deletion) is None
        assert model.objects(id=ids[0]).first() is None
        assert model.objects(id=ids[1]).get().total == 8
    finally:
        await batcher.stop()
