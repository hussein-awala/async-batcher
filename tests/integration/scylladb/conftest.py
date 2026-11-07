from __future__ import annotations

from uuid import uuid4

import pytest
import pytest_asyncio
from async_batcher.scylladb.update import AsyncScyllaDbWriteBatcher
from cassandra.cluster import Cluster
from cassandra.cqlengine import columns, connection
from cassandra.cqlengine.management import drop_table, sync_table
from cassandra.cqlengine.models import Model


@pytest_asyncio.fixture
async def scylladb_write_batcher():
    write_batcher = AsyncScyllaDbWriteBatcher(max_queue_time=2)
    yield write_batcher
    await write_batcher.stop(force=True, timeout=10)


@pytest.fixture
def scylladb_session():
    cluster = Cluster(["127.0.0.1"], port=9042, connect_timeout=10, control_connection_timeout=10)
    keyspace = f"batcher_{uuid4().hex}"
    session = None

    try:
        session = cluster.connect()
        session.default_timeout = 10
        session.execute(
            f"CREATE KEYSPACE {keyspace} "
            "WITH REPLICATION = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}"
        )
        session.set_keyspace(keyspace)
        connection.register_connection(keyspace, session=session, default=True)
        yield session
    finally:
        try:
            if session is not None:
                session.execute(f"DROP KEYSPACE IF EXISTS {keyspace}")
        finally:
            connection.unregister_connection(keyspace)
            cluster.shutdown()


@pytest.fixture
def scylladb_tables(scylladb_session):
    class TestModel1(Model):
        __table_name__ = "test_table1"
        __keyspace__ = scylladb_session.keyspace
        id = columns.UUID(primary_key=True)
        attr1 = columns.Text()
        attr2 = columns.Double()

    class TestModel2(Model):
        __table_name__ = "test_table2"
        __keyspace__ = scylladb_session.keyspace
        id = columns.UUID(primary_key=True)
        attr1 = columns.Text()
        attr2 = columns.Double()

    try:
        sync_table(TestModel1)
        sync_table(TestModel2)
        yield TestModel1, TestModel2
    finally:
        drop_table(TestModel1)
        drop_table(TestModel2)


@pytest.fixture
def scylladb_counter_table(scylladb_session):
    class CounterModel(Model):
        __table_name__ = "test_counters"
        __keyspace__ = scylladb_session.keyspace
        __connection__ = scylladb_session.keyspace
        id = columns.UUID(primary_key=True)
        total = columns.Counter()

    try:
        sync_table(CounterModel)
        yield CounterModel
    finally:
        drop_table(CounterModel)
