from __future__ import annotations

import pytest
import pytest_asyncio
from async_batcher.scylladb.update import AsyncScyllaDbWriteBatcher
from cassandra.cluster import Cluster
from cassandra.connection import DefaultEndPoint
from cassandra.cqlengine import columns, connection
from cassandra.cqlengine.management import drop_table, sync_table
from cassandra.cqlengine.models import Model

TEST_KEYSPACE = "test_keyspace"


@pytest_asyncio.fixture
async def scylladb_write_batcher():
    write_batcher = AsyncScyllaDbWriteBatcher(max_queue_time=2)
    yield write_batcher
    await write_batcher.stop()


@pytest.fixture
def scylladb_session():
    cluster = Cluster(
        [
            DefaultEndPoint("localhost", 9042),
            DefaultEndPoint("localhost", 9043),
        ]
    )

    try:
        session = cluster.connect()
        session.execute(
            f"CREATE KEYSPACE IF NOT EXISTS {TEST_KEYSPACE} "
            "WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 3}"
        )
        session.execute(f"USE {TEST_KEYSPACE}")
        yield session
    finally:
        cluster.shutdown()


@pytest.fixture
def scylladb_tables(scylladb_session):
    class TestModel1(Model):
        __table_name__ = "test_table1"
        __keyspace__ = TEST_KEYSPACE
        id = columns.UUID(primary_key=True)
        attr1 = columns.Text()
        attr2 = columns.Double()

    class TestModel2(Model):
        __table_name__ = "test_table2"
        __keyspace__ = TEST_KEYSPACE
        id = columns.UUID(primary_key=True)
        attr1 = columns.Text()
        attr2 = columns.Double()

    connection.register_connection("test_cluster", session=scylladb_session, default=True)
    sync_table(TestModel1)
    sync_table(TestModel2)
    yield TestModel1, TestModel2
    drop_table(TestModel1)
    drop_table(TestModel2)
