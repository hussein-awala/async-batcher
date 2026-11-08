# Database write adapters

These examples require Async Batcher 0.3.0; see [installation](installation.md). All adapters accept the
[core limits and lifecycle](api.md); stop them before closing database resources.

## SQLAlchemy

Install the adapter and the SQLite async driver used by this example:

```bash
python -m pip install 'async-batcher[sqlalchemy]==0.3.0' 'aiosqlite>=0.22.1,<0.23'
```

SQLAlchemy 2.0.10 or newer is required. The extra installs SQLAlchemy; applications must also
install the async driver for their chosen dialect, such as `asyncpg` for PostgreSQL.
The repository's `adapter-test` development group is only needed when running its adapter tests.

`AsyncSqlalchemyWriteBatcher` takes a model, a caller-owned `AsyncEngine`, and `operation="insert"`
or `"update"`. Each batch uses its own session and commits once all of its statements succeed.
The adapter does not dispose the engine and does not join an application-owned transaction.

This runnable example uses an in-memory SQLite database:

```python
import asyncio

from async_batcher.sqlalchemy.write import AsyncSqlalchemyWriteBatcher
from sqlalchemy import String
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Item(Base):
    __tablename__ = "items"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String)


async def main():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    batcher = AsyncSqlalchemyWriteBatcher(
        model=Item, async_engine=engine, returning=[Item.id, Item.name], max_batch_size=100
    )
    try:
        async with engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        rows = await asyncio.gather(batcher.process({"name": "one"}), batcher.process({"name": "two"}))
        assert [row.name for row in rows] == ["one", "two"]
    finally:
        try:
            await batcher.stop()
        finally:
            await engine.dispose()


asyncio.run(main())
```

Inserts accept a mapped ORM class or a Core `Table`. Adjacent inputs with different field sets
are executed separately, preserving omitted-column defaults and input order. With `returning`,
each caller receives a SQLAlchemy `Row`; without it, each receives `None`. Ordered INSERT
RETURNING requires dialect support and can cause SQLAlchemy to issue individual statements.
SQLAlchemy's mapped ORM insert behavior can omit explicit `None` values on columns with defaults;
it does not always mean SQL NULL. Core `Table` inserts preserve explicit `None` as NULL.

Updates require a mapped ORM class and every primary-key value in each input dictionary.
Core `Table` updates and UPDATE RETURNING are rejected. Each successful update returns `None`;
the adapter does not expose per-item matched-row counts. Driver or commit errors fail the batch.
See SQLAlchemy's [bulk DML contracts](https://docs.sqlalchemy.org/en/20/orm/queryguide/dml.html).

## ScyllaDB

Install `python -m pip install 'async-batcher[scylla]==0.3.0'`. Import from
`async_batcher.scylladb.update`; the underlying Scylla driver uses the `cassandra` module name.
Configure your cqlengine model connection and keyspace before submitting requests.

```python
import asyncio

from async_batcher.scylladb.update import AsyncScyllaDbWriteBatcher, WriteOperation


async def insert_rows(model):
    # model is an application cqlengine Model class with an active connection.
    batcher = AsyncScyllaDbWriteBatcher(max_batch_size=100)
    try:
        return await asyncio.gather(
            batcher.process(WriteOperation(operation="INSERT", model=model, data={"id": 1})),
            batcher.process(WriteOperation(operation="INSERT", model=model, data={"id": 2})),
        )
    finally:
        await batcher.stop()
```

`INSERT` requires `data`; `UPDATE` requires both `key` and `data`; `DELETE` requires `key`.
Successful requests return `None`. Invalid operation names or missing required dictionaries fail
their individual requests while valid operations can still execute. Other model/driver failures
can fail the entire batch. Valid operations must share one connection and cannot mix counter and
non-counter models; counter models use a COUNTER batch.

The synchronous cqlengine `BatchQuery` runs in the core executor. You own its connection and
cluster; keep them open through shutdown, including any synchronous work that continues after a
forced stop. The adapter adds no retries. Scylla batches do not provide SQL transaction isolation
or ordering for dependent writes; consult the driver's [batch semantics](https://python-driver.docs.scylladb.com/stable/cqlengine/batches.html).

## MongoDB

Install `python -m pip install 'async-batcher[mongodb]==0.3.0'` to use
`AsyncMongoDbWriteBatcher` with **PyMongo 4.19+ and MongoDB Server 8.0+**. It uses PyMongo's
native async client; Motor is not required.
Pass native `InsertOne`, `UpdateOne`, `UpdateMany`, `DeleteOne`, `DeleteMany`, or
`ReplaceOne` operations with a `namespace` such as `"app.items"`. A batch can target
multiple collections. Each caller receives its own `InsertOneResult`, `UpdateResult`,
or `DeleteResult`, including inserted IDs, upsert IDs, and matched/modified/deleted counts.
For upserts, inspect `did_upsert` and `upserted_id`. Native PyMongo 4.19 client-bulk
results include an upsert in `matched_count`, unlike collection-level update results;
the adapter preserves the driver's result objects.

```python
import asyncio

from async_batcher.mongodb import AsyncMongoDbWriteBatcher
from pymongo import AsyncMongoClient, InsertOne, UpdateOne


async def main():
    client = AsyncMongoClient("mongodb://localhost:27017")
    batcher = AsyncMongoDbWriteBatcher(client=client, max_batch_size=100)
    try:
        inserted, updated = await asyncio.gather(
            batcher.process(InsertOne({"_id": 1, "value": 0}, namespace="app.items")),
            batcher.process(UpdateOne({"_id": 1}, {"$set": {"value": 1}}, namespace="app.items")),
        )
        print(inserted.inserted_id, updated.modified_count)
    finally:
        try:
            await batcher.stop()
        finally:
            await client.close()
```

Writes execute in order within each batch. Keep the default `concurrency=1` to
serialize batches. On a write error, earlier successful requests retain their results,
the failing request raises a PyMongo write error, and later requests in that batch
raise `MongoDbNotExecutedError`. Subsequent batches can continue. Ordered batches
are not transactions: earlier writes are not rolled back.

Network and write-concern errors fail the whole batch because individual outcomes
may be uncertain. The adapter adds no retries; PyMongo retains its own retry policy.
Unacknowledged writes (`w=0`) are rejected. The client belongs to the caller and must
be used on the batcher's event loop, then closed after stopping the batcher.

The [client bulk-write API](https://www.mongodb.com/docs/languages/python/pymongo-driver/current/crud/bulk-write/)
provides the per-operation results used here; older collection bulk writes only expose
aggregate counts.

See [integration test setup](../CONTRIBUTING.md#integration-tests) for the pinned local service.
