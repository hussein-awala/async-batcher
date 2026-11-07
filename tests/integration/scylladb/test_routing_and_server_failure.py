from __future__ import annotations

import asyncio
from uuid import uuid4

import pytest
from async_batcher.scylladb.update import AsyncScyllaDbWriteBatcher, WriteOperation
from cassandra import InvalidRequest
from cassandra.cluster import Cluster
from cassandra.cqlengine import columns, connection
from cassandra.cqlengine.management import sync_table
from cassandra.cqlengine.models import Model

pytestmark = [pytest.mark.asyncio, pytest.mark.integration, pytest.mark.integration_scylladb]


async def test_named_connection_uses_its_real_session_without_touching_default(
    scylladb_session, scylladb_tables
):
    default_model = scylladb_tables[0]
    keyspace = f"batcher_named_{uuid4().hex}"
    key = uuid4()
    named_session = None
    named_cluster = None
    listeners_added = False
    batcher = AsyncScyllaDbWriteBatcher()
    default_requests = []
    named_requests = []

    def observe_default(future):
        default_requests.append(future.query)

    def observe_named(future):
        named_requests.append(future.query)

    try:
        await asyncio.to_thread(
            scylladb_session.execute,
            f"CREATE KEYSPACE {keyspace} "
            "WITH REPLICATION = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}",
        )
        # cqlengine unregisters and shuts down the registered cluster, so this
        # connection owns a separate cluster instead of borrowing the default's.
        named_cluster = Cluster(["127.0.0.1"], port=9042, connect_timeout=10, control_connection_timeout=10)
        named_session = await asyncio.to_thread(named_cluster.connect, keyspace)
        named_session.default_timeout = 10
        connection.register_connection(keyspace, session=named_session, default=False)

        class NamedModel(Model):
            __keyspace__ = keyspace
            __connection__ = keyspace
            __table_name__ = default_model.__table_name__
            id = columns.UUID(primary_key=True)
            attr1 = columns.Text()
            attr2 = columns.Double()

        await asyncio.to_thread(sync_table, NamedModel)
        await asyncio.to_thread(default_model.create, id=key, attr1="default sentinel")
        # These are SDK observers, not replacement execution methods. Both
        # sessions issue real CQL to distinct keyspaces on the local service.
        scylladb_session.add_request_init_listener(observe_default)
        named_session.add_request_init_listener(observe_named)
        listeners_added = True
        async with asyncio.timeout(20):
            assert (
                await batcher.process(
                    WriteOperation(operation="INSERT", model=NamedModel, data={"id": key, "attr1": "named"})
                )
                is None
            )
            assert (
                await batcher.process(
                    WriteOperation(
                        operation="UPDATE", model=NamedModel, key={"id": key}, data={"attr1": "updated"}
                    )
                )
                is None
            )
        await batcher.stop(timeout=10)
        scylladb_session.remove_request_init_listener(observe_default)
        named_session.remove_request_init_listener(observe_named)
        listeners_added = False
        assert default_requests == []
        assert len(named_requests) == 2
        assert await asyncio.to_thread(lambda: NamedModel.objects(id=key).get().attr1) == "updated"
        assert (
            await asyncio.to_thread(lambda: default_model.objects(id=key).get().attr1) == "default sentinel"
        )
    finally:
        await batcher.stop(force=True, timeout=10)
        if listeners_added:
            scylladb_session.remove_request_init_listener(observe_default)
            named_session.remove_request_init_listener(observe_named)
        try:
            if named_cluster is not None:
                try:
                    await asyncio.to_thread(connection.unregister_connection, keyspace)
                finally:
                    await asyncio.to_thread(named_cluster.shutdown)
        finally:
            await asyncio.to_thread(scylladb_session.execute, f"DROP KEYSPACE IF EXISTS {keyspace}")


async def test_server_schema_rejection_fails_the_batch_without_writes_and_allows_reuse(scylladb_tables):
    model = scylladb_tables[0]

    class StaleModel(Model):
        __keyspace__ = model.__keyspace__
        __table_name__ = model.__table_name__
        id = columns.UUID(primary_key=True)
        contract_only = columns.Text()

    # Deliberately do not sync StaleModel: its valid local value targets a
    # column absent from the server schema, forcing a genuine CQL rejection.
    ids = [uuid4(), uuid4()]
    batcher = AsyncScyllaDbWriteBatcher(max_batch_size=2)
    try:
        async with asyncio.timeout(20):
            results = await asyncio.gather(
                batcher.process(WriteOperation(operation="INSERT", model=model, data={"id": ids[0]})),
                batcher.process(
                    WriteOperation(
                        operation="INSERT", model=StaleModel, data={"id": ids[1], "contract_only": "valid"}
                    )
                ),
                return_exceptions=True,
            )
            assert all(isinstance(result, InvalidRequest) for result in results)
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
