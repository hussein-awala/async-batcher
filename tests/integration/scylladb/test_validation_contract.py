from __future__ import annotations

import asyncio
from uuid import uuid4

import pytest
from async_batcher.scylladb.update import AsyncScyllaDbWriteBatcher, WriteOperation
from cassandra.cqlengine import ValidationError

pytestmark = [pytest.mark.asyncio, pytest.mark.integration, pytest.mark.integration_scylladb]


async def test_request_validation_error_does_not_discard_valid_neighbors(scylladb_tables):
    first, second = scylladb_tables
    ids = [uuid4(), uuid4()]
    batcher = AsyncScyllaDbWriteBatcher(max_batch_size=3)
    try:
        async with asyncio.timeout(20):
            results = await asyncio.gather(
                batcher.process(
                    WriteOperation(operation="INSERT", model=first, data={"id": ids[0], "attr2": 0})
                ),
                batcher.process(WriteOperation(operation="DELETE", model=first)),
                batcher.process(
                    WriteOperation(operation="INSERT", model=second, data={"id": ids[1], "attr2": 2})
                ),
                return_exceptions=True,
            )
            assert results[0] is None
            assert isinstance(results[1], ValueError)
            assert results[2] is None
            assert await asyncio.to_thread(lambda: first.objects(id=ids[0]).get().attr2) == 0
            assert await asyncio.to_thread(lambda: second.objects(id=ids[1]).get().attr2) == 2
    finally:
        await batcher.stop(force=True, timeout=10)


async def test_model_validation_failure_aborts_queued_statements_and_allows_reuse(scylladb_tables):
    model = scylladb_tables[0]
    ids = [uuid4(), uuid4()]
    batcher = AsyncScyllaDbWriteBatcher(max_batch_size=2)
    try:
        async with asyncio.timeout(20):
            results = await asyncio.gather(
                batcher.process(
                    WriteOperation(operation="INSERT", model=model, data={"id": ids[0], "attr1": "valid"})
                ),
                batcher.process(
                    WriteOperation(operation="INSERT", model=model, data={"id": ids[1], "attr1": 123})
                ),
                return_exceptions=True,
            )
            assert all(isinstance(result, ValidationError) for result in results)
            assert await asyncio.to_thread(model.objects.count) == 0
            assert (
                await batcher.process(
                    WriteOperation(operation="INSERT", model=model, data={"id": ids[0], "attr1": "recovered"})
                )
                is None
            )
            assert await asyncio.to_thread(lambda: model.objects(id=ids[0]).get().attr1) == "recovered"
    finally:
        await batcher.stop(force=True, timeout=10)
