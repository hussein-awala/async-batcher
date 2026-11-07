"""Import an immutable JSONL file with explicit, bounded rejection records."""

from __future__ import annotations

import argparse
import asyncio
import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from contextlib import aclosing, closing
from contextvars import Context
from dataclasses import dataclass
from functools import partial
from pathlib import Path

from async_batcher import AsyncBatcher
from examples.recipes.resumable_sqlite import Source
from examples.recipes.support import Document


@dataclass(frozen=True)
class ImportRow:
    start: int
    end: int
    document: Document | None
    rejection: str | None


def read_rows(source: Source, offset: int, *, chunk_size: int = 32, max_line_bytes: int = 65536):
    """Turn known input errors into metadata; I/O/source-change errors still raise."""
    if chunk_size < 1 or max_line_bytes < 1 or not 0 <= offset <= source.size:
        raise ValueError("Invalid reader limits or source offset")
    source.check()
    rows = []
    with source.path.open("rb") as stream:
        stream.seek(offset)
        for _ in range(chunk_size):
            start = stream.tell()
            line = stream.readline(max_line_bytes + 1)
            if not line:
                break
            reason, document = None, None
            if len(line) > max_line_bytes:
                reason = "line_too_large"
                # Discard the rest in bounded pieces, so the following line retains
                # its real boundary even when this record is larger than memory.
                while not line.endswith(b"\n"):
                    line = stream.readline(max_line_bytes + 1)
                    if not line:
                        break
            else:
                try:
                    value = json.loads(line.decode("utf-8"))
                except (ValueError, UnicodeError):
                    reason = "invalid_utf8_or_json"
                else:
                    if (
                        not isinstance(value, dict)
                        or set(value) != {"id", "text"}
                        or not isinstance(value["id"], str)
                        or not value["id"]
                        or not isinstance(value["text"], str)
                    ):
                        reason = "invalid_document_fields"
                    else:
                        document = Document(**value)
            rows.append(ImportRow(start, stream.tell(), document, reason))
    source.check()
    return rows


class ImportStore:
    def __init__(self, database: Path, run_id: str, source: Source):
        if not isinstance(run_id, str) or not run_id:
            raise ValueError("run_id must be a nonempty string")
        self.database, self.run_id, self.source = database, run_id, source

    def connect(self):
        connection = sqlite3.connect(self.database, timeout=5)
        try:
            if connection.execute("PRAGMA journal_mode=WAL").fetchone()[0] != "wal":
                raise RuntimeError("This recipe requires file-backed SQLite with WAL")
            connection.execute("PRAGMA synchronous=FULL")
            connection.execute("PRAGMA foreign_keys=ON")
            return connection
        except BaseException:
            connection.close()
            raise

    def initialize(self):
        self.source.check()
        with closing(self.connect()) as connection, connection:
            connection.executescript(
                "CREATE TABLE IF NOT EXISTS rejected_import_runs "
                "(run_id TEXT PRIMARY KEY, source_path TEXT NOT NULL, sha256 TEXT NOT NULL);"
                "CREATE TABLE IF NOT EXISTS rejected_import_rows "
                "(run_id TEXT NOT NULL REFERENCES rejected_import_runs, start INTEGER NOT NULL, "
                "end INTEGER NOT NULL, rejection TEXT, PRIMARY KEY(run_id, start));"
                "CREATE TABLE IF NOT EXISTS rejected_import_documents "
                "(run_id TEXT NOT NULL REFERENCES rejected_import_runs, id TEXT NOT NULL, "
                "text TEXT NOT NULL, PRIMARY KEY(run_id, id));"
            )
            # No implicit replay policy: partial and finished runs both need a new ID.
            connection.execute(
                "INSERT INTO rejected_import_runs VALUES (?, ?, ?)",
                (self.run_id, str(self.source.path), self.source.sha256),
            )

    def write(self, rows: list[ImportRow]) -> list[bool]:
        self.source.check()
        with closing(self.connect()) as connection, connection:
            connection.execute("BEGIN IMMEDIATE")
            for row in rows:
                connection.execute(
                    "INSERT INTO rejected_import_rows VALUES (?, ?, ?, ?)",
                    (self.run_id, row.start, row.end, row.rejection),
                )
                if row.document is not None:
                    connection.execute(
                        "INSERT INTO rejected_import_documents VALUES (?, ?, ?) "
                        "ON CONFLICT(run_id, id) DO UPDATE SET text = excluded.text",
                        (self.run_id, row.document.id, row.document.text),
                    )
            self.source.check()
        return [row.rejection is None for row in rows]


async def import_file(
    path: Path,
    database: Path,
    run_id: str,
    *,
    executor: ThreadPoolExecutor,
    batch_size: int = 32,
    max_pending: int = 64,
) -> dict:
    """Borrow the executor; its owner must join threads after cancellation too."""
    for name, value in (("batch_size", batch_size), ("max_pending", max_pending)):
        if isinstance(value, bool) or not isinstance(value, int) or value < 1:
            raise ValueError(f"{name} must be a positive integer")
    loop = asyncio.get_running_loop()
    source = await loop.run_in_executor(executor, Source.inspect, path)
    store = ImportStore(database, run_id, source)

    async def records():
        offset = 0
        while offset < source.size:
            chunk = await loop.run_in_executor(executor, partial(read_rows, source, offset))
            if not chunk:
                raise RuntimeError("Source ended before its recorded size")
            for row in chunk:
                yield row
            offset = chunk[-1].end

    batcher = AsyncBatcher.from_callable(
        store.write,
        max_batch_size=batch_size,
        max_queue_size=max_pending,
        concurrency=1,
        executor=executor,
        processor_context=Context(),
        log_exceptions=False,
    )
    await loop.run_in_executor(executor, store.initialize)
    accepted = rejected = 0
    async with batcher, aclosing(batcher.map(records(), max_pending=max_pending)) as outcomes:
        async for succeeded in outcomes:
            accepted += succeeded
            rejected += not succeeded
    return {"run_id": run_id, "accepted_records": accepted, "rejected_records": rejected}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--database", required=True, type=Path)
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()
    with ThreadPoolExecutor(max_workers=2) as executor:
        report = asyncio.run(import_file(args.input, args.database, args.run_id, executor=executor))
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
