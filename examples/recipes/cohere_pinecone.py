"""Embed documents with Cohere v2 and upsert into an existing Pinecone vector index."""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import os
import time
from contextlib import aclosing
from contextvars import Context
from pathlib import Path
from typing import TYPE_CHECKING

import httpx2
from async_batcher import AsyncBatcher
from async_batcher.pinecone import AsyncPineconeUpsertBatcher
from examples.recipes.support import BatchMeasurements, Document, read_jsonl
from pinecone import AsyncIndex, Vector

if TYPE_CHECKING:
    from collections.abc import AsyncIterable, Iterable

MAX_TEXT_BYTES = 65536
VECTOR_REQUEST_BUDGET = 1_800_000


def document_cost(document: Document) -> int:
    if not isinstance(document, Document):
        raise TypeError("Source items must be Document instances")
    identifier = document.id
    if (
        not isinstance(identifier, str)
        or not 1 <= len(identifier) <= 512
        or not identifier.isascii()
        or "\x00" in identifier
    ):
        raise ValueError("Document IDs must contain 1 to 512 ASCII characters without NUL")
    if not isinstance(document.text, str) or not document.text.strip():
        raise ValueError("Document text must be a nonempty string")
    size = len(document.text.encode("utf-8"))
    if size > MAX_TEXT_BYTES:
        raise ValueError("Document text exceeds this recipe's 64 KiB limit")
    return size


def vector_cost(vector: Vector) -> int:
    # No metadata/sparse fields are emitted by this recipe. Reserve 32 bytes per
    # finite Python float, plus escaped ID and record/separator overhead.
    return 32 * len(vector.values) + len(json.dumps(vector.id).encode()) + 128


class CohereEmbedder:
    def __init__(self, client: httpx2.AsyncClient, *, model: str, dimension: int):
        if not isinstance(model, str) or not model.strip():
            raise ValueError("An explicit Cohere model is required")
        if isinstance(dimension, bool) or not isinstance(dimension, int) or not 1 <= dimension <= 20000:
            raise ValueError("dimension must be an integer between 1 and 20000")
        self.client, self.model, self.dimension = client, model, dimension

    async def embed(self, documents: list[Document]) -> list[Vector]:
        if not documents:
            return []
        if len(documents) > 96:
            raise ValueError("Cohere permits at most 96 texts per request")
        for document in documents:
            document_cost(document)
        response = await self.client.post(
            "/v2/embed",
            json={
                "model": self.model,
                "texts": [document.text for document in documents],
                "input_type": "search_document",
                "embedding_types": ["float"],
                "output_dimension": self.dimension,
                "truncate": "NONE",
            },
        )
        response.raise_for_status()
        payload = response.json()
        embeddings = payload.get("embeddings") if isinstance(payload, dict) else None
        rows = embeddings.get("float") if isinstance(embeddings, dict) else None
        if not isinstance(rows, list) or len(rows) != len(documents):
            raise ValueError("Cohere response must contain one float embedding per document")
        vectors = []
        for document, row in zip(documents, rows, strict=True):
            if not isinstance(row, list) or len(row) != self.dimension:
                raise ValueError("Cohere response embedding dimension does not match the configured index")
            if any(isinstance(value, bool) or not isinstance(value, int | float) for value in row):
                raise ValueError("Cohere embeddings must contain finite numbers")
            try:
                values = [float(value) for value in row]
            except (ValueError, OverflowError) as error:
                raise ValueError("Cohere embeddings must contain finite numbers") from error
            if not all(math.isfinite(value) for value in values):
                raise ValueError("Cohere embeddings must contain finite numbers")
            vectors.append(Vector(id=document.id, values=values))
        # Returning only after validating every row prevents a partial valid prefix
        # from reaching storage when the provider response is malformed.
        return vectors


async def index_documents(
    source: Iterable[Document] | AsyncIterable[Document],
    *,
    cohere_client: httpx2.AsyncClient,
    index: AsyncIndex,
    model: str,
    dimension: int,
    namespace: str,
    batch_size: int = 32,
    max_pending: int = 64,
) -> dict:
    """Own bounded batchers/maps; the caller owns both provider clients."""
    if (
        not isinstance(namespace, str)
        or not namespace.strip()
        or len(namespace) > 512
        or not namespace.isascii()
        or "\x00" in namespace
    ):
        raise ValueError("Namespace must contain 1 to 512 ASCII characters without NUL")
    namespace_bytes = len(json.dumps(namespace).encode())
    if isinstance(batch_size, bool) or not isinstance(batch_size, int) or not 1 <= batch_size <= 96:
        raise ValueError("batch_size must be an integer between 1 and 96")
    if isinstance(max_pending, bool) or not isinstance(max_pending, int) or max_pending < 1:
        raise ValueError("max_pending must be a positive integer")
    embedder = CohereEmbedder(cohere_client, model=model, dimension=dimension)
    embedding_measurements, write_measurements = BatchMeasurements(), BatchMeasurements()
    embeddings = AsyncBatcher.from_callable(
        embedder.embed,
        max_batch_size=batch_size,
        max_queue_size=max_pending,
        max_batch_cost=262144,
        item_cost=document_cost,
        concurrency=1,
        processor_context=Context(),
        log_exceptions=False,
        on_batch=embedding_measurements.observe,
    )
    writes = AsyncPineconeUpsertBatcher(
        index=index,
        namespace=namespace,
        max_batch_size=batch_size,
        max_queue_size=max_pending,
        max_batch_cost=VECTOR_REQUEST_BUDGET - namespace_bytes - 128,
        item_cost=vector_cost,
        concurrency=1,
        processor_context=Context(),
        log_exceptions=False,
        on_batch=write_measurements.observe,
    )
    # Check dimension before consuming source items or making embedding requests.
    # This cannot prove model/revision/metric compatibility; the caller must do so.
    stats = await index.describe_index_stats()
    if type(stats.dimension) is not int or stats.dimension != dimension:
        raise ValueError("Existing Pinecone index dimension does not match the requested embeddings")
    completed = 0
    started = time.perf_counter()
    async with embeddings, writes:
        async with aclosing(embeddings.map(source, max_pending=max_pending)) as vectors:
            async with aclosing(writes.map(vectors, max_pending=max_pending)) as results:
                async for _ in results:
                    completed += 1
    return {
        "completed_items": completed,
        "elapsed_seconds": time.perf_counter() - started,
        "embedding": embedding_measurements.report(embeddings),
        "storage": write_measurements.report(writes),
    }


async def run(args):
    # Explicitly caller-owned clients outlive both batchers and their accepted work.
    async with (
        httpx2.AsyncClient(
            base_url="https://api.cohere.com",
            headers={"Authorization": f"Bearer {os.environ['COHERE_API_KEY']}"},
            timeout=30,
            follow_redirects=False,
        ) as client,
        AsyncIndex(host=args.host, api_key=os.environ["PINECONE_API_KEY"], timeout=30) as index,
    ):
        return await index_documents(
            read_jsonl(args.input),
            cohere_client=client,
            index=index,
            model=args.model,
            dimension=args.dimension,
            namespace=args.namespace,
        )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--host", required=True, help="Existing Pinecone vector-index HTTPS host")
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--model", required=True, help="Cohere Embed v4+ model supporting this dimension")
    parser.add_argument("--dimension", required=True, type=int)
    args = parser.parse_args()
    if not args.host.startswith("https://"):
        parser.error("--host must be an explicit https:// Pinecone index host")
    if not os.environ.get("COHERE_API_KEY") or not os.environ.get("PINECONE_API_KEY"):
        parser.error("Set COHERE_API_KEY and PINECONE_API_KEY in the environment")
    print(json.dumps(asyncio.run(run(args)), indent=2))


if __name__ == "__main__":
    main()
