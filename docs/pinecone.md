# Pinecone vector batches


Install the `pinecone` extra for Async Batcher 0.3.0; see [installation](installation.md)
for Python requirements and release availability:

```bash
python -m pip install 'async-batcher[pinecone]==0.3.0'
```
The extra requires Pinecone SDK 10.3 through 10.x.
The vector adapters use a caller-owned `pinecone.AsyncIndex` and one namespace per batcher:

- `AsyncPineconeUpsertBatcher`: accepts SDK `Vector` objects (dense, sparse or hybrid),
  returning `None` when the server acknowledges the write.
- `AsyncPineconeFetchBatcher`: accepts vector IDs and returns SDK `Vector` objects or `None`
  for missing IDs, preserving input order and duplicate requests.
- `AsyncPineconeDeleteBatcher`: accepts vector IDs and returns `None` after a successful call.
  Deleting a missing ID also succeeds.

```python
import asyncio

from pinecone import AsyncIndex, Vector
from async_batcher.pinecone import AsyncPineconeUpsertBatcher


async def write_vectors(host: str):
    # The SDK reads PINECONE_API_KEY from the environment.
    async with AsyncIndex(host=host) as index:
        batcher = AsyncPineconeUpsertBatcher(index=index, namespace="articles")
        try:
            await asyncio.gather(
                batcher.process(Vector(id="one", values=[0.1, 0.2, 0.3])),
                batcher.process(Vector(id="two", values=[0.4, 0.5, 0.6])),
            )
        finally:
            # Drain accepted work before the caller closes the shared index.
            await batcher.stop()
```

Use your index's actual vector dimension. These adapters target the SDK's
[vector interface](https://sdk.pinecone.io/python/how-to/vectors/upsert-and-query.html),
not schema-based document indexes. They batch upserts, fetches and deletes; similarity
queries and partial updates remain direct SDK operations.

Each dispatch makes one native SDK call. `max_batch_size` defaults to 100 and must be
between 1 and 1,000. All three batchers require IDs of 1–512 ASCII characters without NUL.
Upserts also have a [2 MB request limit](https://docs.pinecone.io/reference/api/database-limits/operation-limits);
large vectors or metadata need a smaller batch size. SDK validation, size and service errors
fail that dispatch, with no extra adapter retries or rollback. Duplicate upsert IDs within
one dispatch are rejected before writing; use `concurrency=1` (the default) for sequential
dispatches, or separate awaited writes when repeatedly replacing the same ID. Fetches and
deletes deduplicate IDs within a dispatch.

Writes are eventually consistent: an acknowledgement does not guarantee immediate visibility
in a subsequent fetch or query, and deletion success does not establish that an ID existed.
The caller owns the index and must keep it open until every batcher using it has stopped.
The adapters never create, enter, or close the index.


See [core lifecycle](api.md) and [test setup](../CONTRIBUTING.md#adapter-tests).
