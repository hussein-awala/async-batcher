from __future__ import annotations

from typing import TypeVar

from pinecone import AsyncIndex, Vector

from async_batcher.batcher import AsyncBatcher

_Item = TypeVar("_Item")
_Result = TypeVar("_Result")


class _PineconeBatcher(AsyncBatcher[_Item, _Result]):
    """Share a caller-owned index; stopping a batcher never closes that index."""

    def __init__(
        self,
        *,
        index: AsyncIndex,
        namespace: str = "",
        max_batch_size: int = 100,
        max_queue_time: float = 0.01,
        concurrency: int = 1,
        **kwargs,
    ):
        # Reject the synchronous client before a write could run on the event loop.
        if not isinstance(index, AsyncIndex):
            raise TypeError("index must be a pinecone.AsyncIndex")
        if not isinstance(namespace, str):
            raise TypeError("namespace must be a string")
        super().__init__(
            max_batch_size=max_batch_size,
            max_queue_time=max_queue_time,
            concurrency=concurrency,
            **kwargs,
        )
        if not 1 <= self.max_batch_size <= 1000:
            raise ValueError("Pinecone max_batch_size must be between 1 and 1000")
        self.index = index
        self.namespace = namespace

    @staticmethod
    def _check_count(batch: list) -> None:
        # process_batch is public and can also be called without the dispatcher.
        if len(batch) > 1000:
            raise ValueError("Pinecone requests cannot contain more than 1000 items")

    @staticmethod
    def _unique_ids(batch: list[str]) -> list[str]:
        if any(not isinstance(identifier, str) for identifier in batch):
            raise TypeError("Pinecone IDs must be strings")
        if any(
            not identifier or len(identifier) > 512 or not identifier.isascii() or "\x00" in identifier
            for identifier in batch
        ):
            raise ValueError("Pinecone IDs must contain 1 to 512 ASCII characters without NUL")
        return list(dict.fromkeys(batch))


class AsyncPineconeUpsertBatcher(_PineconeBatcher[Vector, None]):
    """Upsert dense, sparse or hybrid SDK Vectors into one namespace.

    Each successful request returns None: the server accepted that vector, but
    reads may not see it immediately. Duplicate IDs in one dispatch are rejected
    before writing; concurrent dispatches have no ordering guarantee. The SDK's
    2 MB request-size limit still applies, so reduce max_batch_size for large
    vectors or metadata. SDK errors fail the dispatch without rolling back writes.
    """

    async def process_batch(self, batch: list[Vector]) -> list[None]:
        self._check_count(batch)
        if not batch:
            return []
        if any(not isinstance(vector, Vector) for vector in batch):
            raise TypeError("Pinecone upserts require pinecone.Vector objects")
        ids = self._unique_ids([vector.id for vector in batch])
        if len(ids) != len(batch):
            raise ValueError("Duplicate Pinecone vector IDs in one upsert batch")
        # Omit SDK batch_size: one native request either raises or acknowledges
        # the dispatch, instead of hiding partial failures in a bulk result.
        response = await self.index.upsert(vectors=batch, namespace=self.namespace)
        if response.has_errors or response.upserted_count != len(batch):
            raise RuntimeError(
                f"Pinecone did not acknowledge the complete upsert batch "
                f"({response.upserted_count} of {len(batch)} vectors)"
            )
        return [None] * len(batch)


class AsyncPineconeFetchBatcher(_PineconeBatcher[str, Vector | None]):
    """Fetch IDs from one namespace, returning SDK Vectors or None for missing IDs.

    Duplicate IDs share one native lookup while each caller receives its result
    in input order. A missing ID can also reflect Pinecone's eventual consistency.
    """

    async def process_batch(self, batch: list[str]) -> list[Vector | None]:
        self._check_count(batch)
        if not batch:
            return []
        response = await self.index.fetch(ids=self._unique_ids(batch), namespace=self.namespace)
        return [response.vectors.get(identifier) for identifier in batch]


class AsyncPineconeDeleteBatcher(_PineconeBatcher[str, None]):
    """Delete IDs from one namespace, returning None after the SDK call succeeds.

    Duplicate IDs share one native delete. Missing IDs are ignored by Pinecone;
    success does not establish whether an ID existed or is already invisible.
    """

    async def process_batch(self, batch: list[str]) -> list[None]:
        self._check_count(batch)
        if not batch:
            return []
        await self.index.delete(ids=self._unique_ids(batch), namespace=self.namespace)
        return [None] * len(batch)
