from __future__ import annotations

from typing import TYPE_CHECKING

from pymongo import DeleteMany, DeleteOne, InsertOne, ReplaceOne, UpdateMany, UpdateOne
from pymongo.errors import ClientBulkWriteException, DuplicateKeyError, WriteError
from pymongo.results import DeleteResult, InsertOneResult, UpdateResult

from async_batcher.batcher import AsyncBatcher
from async_batcher.exceptions import AsyncBatchException

if TYPE_CHECKING:
    from pymongo import AsyncMongoClient
    from pymongo.results import ClientBulkWriteResult

WriteOperation = InsertOne | UpdateOne | UpdateMany | DeleteOne | DeleteMany | ReplaceOne
WriteResult = InsertOneResult | UpdateResult | DeleteResult


class MongoDbNotExecutedError(AsyncBatchException):
    """An earlier write failed, so MongoDB did not attempt this operation."""

    def __init__(self, index: int, failed_index: int):
        self.index = index
        self.failed_index = failed_index
        super().__init__(f"Operation {index} was not executed because operation {failed_index} failed")


def _ordered_results(result: ClientBulkWriteResult | None, count: int) -> list[WriteResult]:
    by_index = {}
    if result is not None:
        by_index.update(result.insert_results)
        by_index.update(result.update_results)
        by_index.update(result.delete_results)
    if set(by_index) != set(range(count)):
        raise RuntimeError("MongoDB returned incomplete per-operation results")
    return [by_index[index] for index in range(count)]


class AsyncMongoDbWriteBatcher(AsyncBatcher[WriteOperation, WriteResult]):
    """Batch ordered writes with PyMongo Async and MongoDB 8.0 or later.

    Pass native PyMongo operations with their ``namespace`` set. Each request
    receives its native write result. An ordered write error preserves successful
    prefix results and fails the unattempted suffix with MongoDbNotExecutedError.
    Network and write-concern errors fail the batch because its outcome may be
    uncertain. No adapter-level retries are performed.

    Ordering applies within each batch. Keep the default concurrency of one to
    serialize batches. The caller owns the client and must close it after stopping
    the batcher; both must run on the same event loop.
    """

    def __init__(self, *, client: AsyncMongoClient, **kwargs):
        if not client.write_concern.acknowledged:
            raise ValueError("MongoDB batching requires acknowledged writes")
        super().__init__(**kwargs)
        self.client = client

    async def process_batch(self, batch: list[WriteOperation]) -> list[WriteResult | Exception]:
        if not batch:
            return []
        try:
            result = await self.client.bulk_write(batch, ordered=True, verbose_results=True)
        except ClientBulkWriteException as error:
            write_errors = error.write_errors or []
            if error.error is not None or error.write_concern_errors or len(write_errors) != 1:
                raise
            details = write_errors[0]
            failed_index = details.get("idx")
            if not isinstance(failed_index, int) or not 0 <= failed_index < len(batch):
                raise
            results: list[WriteResult | Exception] = _ordered_results(error.partial_result, failed_index)
            code = details.get("code")
            exception_type = DuplicateKeyError if code in {11000, 11001, 12582} else WriteError
            results.append(exception_type(details.get("errmsg", "MongoDB write failed"), code, details))
            results.extend(
                MongoDbNotExecutedError(index, failed_index) for index in range(failed_index + 1, len(batch))
            )
            return results
        return _ordered_results(result, len(batch))
