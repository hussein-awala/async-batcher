"""Bounded embedding-to-storage pipeline with an explicit non-semantic demo model."""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import sqlite3
import time
from contextlib import aclosing, closing
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Protocol

from async_batcher import AsyncBatcher
from examples.recipes.support import BatchMeasurements, Document, demo_documents, read_jsonl

if TYPE_CHECKING:
    from collections.abc import AsyncIterable, Iterable


@dataclass(frozen=True)
class VectorRecord:
    id: str
    values: list[float]


class EmbeddingModel(Protocol):
    async def embed(self, texts: list[str]) -> list[list[float]]:
        """Return one finite vector per text, in the same order."""
        ...


class VectorSink(Protocol):
    async def upsert(self, records: list[VectorRecord]) -> list[str]:
        """Persist records and return their IDs in input order."""
        ...


class DemoEmbeddingModel:
    """Three deterministic text features; NOT a semantic embedding model."""

    async def embed(self, texts: list[str]) -> list[list[float]]:
        await asyncio.sleep(0)
        return [
            [float(len(text)), float(len(text.split())), float(sum(char.isdigit() for char in text))]
            for text in texts
        ]


class SQLiteVectorSink:
    """Durable vector storage for the demo; it does not implement similarity search."""

    def __init__(self, database: Path):
        self.database = database

    def initialize(self) -> None:
        with closing(sqlite3.connect(self.database)) as connection, connection:
            connection.execute(
                "CREATE TABLE IF NOT EXISTS vectors "
                "(id TEXT PRIMARY KEY CHECK(length(id) > 0), embedding TEXT NOT NULL)"
            )

    def _write(self, records: list[VectorRecord]) -> list[str]:
        with closing(sqlite3.connect(self.database)) as connection, connection:
            connection.executemany(
                "INSERT INTO vectors (id, embedding) VALUES (?, ?) "
                "ON CONFLICT(id) DO UPDATE SET embedding = excluded.embedding",
                [(record.id, json.dumps(record.values, allow_nan=False)) for record in records],
            )
        return [record.id for record in records]

    async def upsert(self, records: list[VectorRecord]) -> list[str]:
        return await asyncio.to_thread(self._write, records)


async def embed_and_store(
    source: Iterable[Document] | AsyncIterable[Document],
    model: EmbeddingModel,
    sink: VectorSink,
    *,
    max_pending: int = 64,
    batch_size: int = 32,
) -> dict:
    """Own the two batchers; the caller owns and closes model/sink clients."""

    async def embed(documents):
        vectors = await model.embed([document.text for document in documents])
        records = []
        for document, vector in zip(documents, vectors, strict=True):
            if not vector or not all(math.isfinite(value) for value in vector):
                raise ValueError("Embedding model must return nonempty finite vectors")
            records.append(VectorRecord(document.id, vector))
        return records

    embedding_measurements = BatchMeasurements()
    write_measurements = BatchMeasurements()
    embeddings = AsyncBatcher.from_callable(
        embed,
        max_batch_size=batch_size,
        max_queue_size=max_pending,
        max_batch_cost=65536,
        item_cost=lambda document: len(document.text.encode()),
        max_queue_time=0.01,
        on_batch=embedding_measurements.observe,
    )
    writes = AsyncBatcher.from_callable(
        sink.upsert,
        max_batch_size=batch_size,
        max_queue_size=max_pending,
        max_queue_time=0.01,
        concurrency=1,
        on_batch=write_measurements.observe,
    )
    completed = 0
    started = time.perf_counter()
    async with embeddings, writes:
        async with aclosing(embeddings.map(source, max_pending=max_pending)) as vectors:
            async with aclosing(writes.map(vectors, max_pending=max_pending)) as results:
                async for _ in results:
                    completed += 1
    elapsed = time.perf_counter() - started
    return {
        "completed_items": completed,
        "elapsed_seconds": elapsed,
        "items_per_second": completed / elapsed,
        "embedding": embedding_measurements.report(embeddings),
        "storage": write_measurements.report(writes),
    }


async def demo(source, database: Path, *, max_pending: int = 64, batch_size: int = 32) -> dict:
    sink = SQLiteVectorSink(database)
    await asyncio.to_thread(sink.initialize)
    report = await embed_and_store(
        source, DemoEmbeddingModel(), sink, max_pending=max_pending, batch_size=batch_size
    )
    return {"workload": "Demo text features + SQLite vector storage; NOT semantic embeddings", **report}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--input", type=Path)
    parser.add_argument("--rows", type=int, default=1000)
    parser.add_argument("--batch-size", type=int, default=32)
    parser.add_argument("--max-pending", type=int, default=64)
    args = parser.parse_args()
    if args.rows < 0:
        parser.error("--rows must be nonnegative")
    source = read_jsonl(args.input) if args.input else demo_documents(args.rows)
    report = asyncio.run(
        demo(source, args.database, max_pending=args.max_pending, batch_size=args.batch_size)
    )
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
