"""Import a JSONL file or generated demo stream with bounded SQLite writes."""

from __future__ import annotations

import argparse
import asyncio
import json
import sqlite3
import time
from contextlib import aclosing, closing
from pathlib import Path
from typing import TYPE_CHECKING

from async_batcher import AsyncBatcher
from examples.recipes.support import BatchMeasurements, Document, demo_documents, read_jsonl

if TYPE_CHECKING:
    from collections.abc import AsyncIterable, Iterable


class SQLiteWriter:
    def __init__(self, database: Path):
        self.database = database

    def initialize(self) -> None:
        with closing(sqlite3.connect(self.database)) as connection, connection:
            connection.execute(
                "CREATE TABLE IF NOT EXISTS documents "
                "(id TEXT PRIMARY KEY CHECK(length(id) > 0), text TEXT NOT NULL)"
            )

    def write(self, documents: list[Document]) -> list[str]:
        # A connection is never shared across threads. Each bulk call commits or
        # rolls back as a unit; earlier completed batches remain committed.
        with closing(sqlite3.connect(self.database)) as connection, connection:
            connection.executemany(
                "INSERT INTO documents (id, text) VALUES (?, ?) "
                "ON CONFLICT(id) DO UPDATE SET text = excluded.text",
                [(document.id, document.text) for document in documents],
            )
        return [document.id for document in documents]


async def write_stream(
    source: Iterable[Document] | AsyncIterable[Document],
    database: Path,
    *,
    max_pending: int = 64,
    batch_size: int = 32,
    max_batch_cost: int = 65536,
) -> dict:
    writer = SQLiteWriter(database)
    measurements = BatchMeasurements()
    batcher = AsyncBatcher.from_callable(
        writer.write,
        max_batch_size=batch_size,
        max_batch_cost=max_batch_cost,
        item_cost=lambda document: len(document.id.encode()) + len(document.text.encode()),
        max_queue_size=max_pending,
        max_queue_time=0.01,
        concurrency=1,
        on_batch=measurements.observe,
    )
    await asyncio.to_thread(writer.initialize)
    completed = 0
    started = time.perf_counter()
    async with batcher:
        async with aclosing(batcher.map(source, max_pending=max_pending)) as results:
            async for _ in results:
                completed += 1
    elapsed = time.perf_counter() - started
    return {
        "workload": "SQLite upserts; one transaction per batch",
        "completed_items": completed,
        "elapsed_seconds": elapsed,
        "items_per_second": completed / elapsed,
        **measurements.report(batcher),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument(
        "--input", type=Path, help="JSONL with string id/text fields; otherwise generate demo rows"
    )
    parser.add_argument("--rows", type=int, default=1000)
    parser.add_argument("--batch-size", type=int, default=32)
    parser.add_argument("--max-pending", type=int, default=64)
    args = parser.parse_args()
    if args.rows < 0:
        parser.error("--rows must be nonnegative")
    source = read_jsonl(args.input) if args.input else demo_documents(args.rows)
    report = asyncio.run(
        write_stream(source, args.database, batch_size=args.batch_size, max_pending=args.max_pending)
    )
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
