"""Resume an immutable JSONL import using an atomic SQLite checkpoint."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import aclosing, closing
from contextvars import Context
from dataclasses import dataclass
from functools import partial
from pathlib import Path

from async_batcher import AsyncBatcher
from examples.recipes.support import BatchMeasurements, Document


class SourceChanged(ValueError):
    """The job's immutable source no longer matches its original contents."""


class CheckpointConflict(RuntimeError):
    """Another writer advanced this job, or the supplied records contain a gap."""


def _stamp(stat: os.stat_result) -> tuple[int, int, int, int, int]:
    return stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns


@dataclass(frozen=True)
class Source:
    path: Path
    sha256: str
    size: int
    stamp: tuple[int, int, int, int, int]

    @classmethod
    def inspect(cls, path: Path) -> Source:
        digest = hashlib.sha256()
        with path.open("rb") as stream:
            before = _stamp(os.fstat(stream.fileno()))
            while chunk := stream.read(1024 * 1024):
                digest.update(chunk)
            if before != _stamp(os.fstat(stream.fileno())):
                raise SourceChanged("Source changed while computing its identity")
        source = cls(path, digest.hexdigest(), before[2], before)
        source.check()
        return source

    def check(self) -> None:
        # Query the current path through a fresh handle, as inspect/read_chunk do.
        # Windows path-stat and handle-stat can disagree on timestamp semantics.
        with self.path.open("rb") as stream:
            stamp = _stamp(os.fstat(stream.fileno()))
        if stamp != self.stamp:
            raise SourceChanged("Source changed during import; restore the original immutable file")


@dataclass(frozen=True)
class Record:
    start: int
    end: int
    document: Document


def read_chunk(source: Source, offset: int, *, chunk_size: int = 32, max_line_bytes: int = 65536):
    if not 0 <= offset <= source.size:
        raise ValueError("Offset must be within the source")
    if chunk_size < 1 or max_line_bytes < 1:
        raise ValueError("Chunk size and line limit must be positive")
    source.check()
    records = []
    with source.path.open("rb") as stream:
        if _stamp(os.fstat(stream.fileno())) != source.stamp:
            raise SourceChanged("Source was replaced before reading")
        stream.seek(offset)
        for _ in range(chunk_size):
            start = stream.tell()
            line = stream.readline(max_line_bytes + 1)
            if not line:
                break
            if len(line) > max_line_bytes:
                raise ValueError(f"JSONL record at byte {start} exceeds the line limit")
            try:
                value = json.loads(line.decode("utf-8"))
            except (ValueError, UnicodeError) as error:
                raise ValueError(f"Invalid UTF-8 JSON record at byte {start}") from error
            if not isinstance(value, dict) or set(value) != {"id", "text"}:
                raise ValueError(f"Record at byte {start} must contain exactly id and text")
            if not isinstance(value["id"], str) or not value["id"] or not isinstance(value["text"], str):
                raise ValueError(f"Record at byte {start} needs a nonempty string id and string text")
            records.append(Record(start, stream.tell(), Document(**value)))
        if _stamp(os.fstat(stream.fileno())) != source.stamp:
            raise SourceChanged("Source changed while reading")
    source.check()
    return records


class CheckpointWriter:
    def __init__(self, database: Path, job_id: str, source: Source):
        if not isinstance(job_id, str) or not job_id:
            raise ValueError("job_id must be a nonempty string")
        self.database, self.job_id, self.source = database, job_id, source

    def _connect(self):
        connection = sqlite3.connect(self.database, timeout=5)
        try:
            mode = connection.execute("PRAGMA journal_mode=WAL").fetchone()[0]
            if mode != "wal":
                raise RuntimeError("This recipe requires a file-backed SQLite database with WAL")
            connection.execute("PRAGMA synchronous=FULL")
            connection.execute("PRAGMA foreign_keys=ON")
            return connection
        except BaseException:
            connection.close()
            raise

    def initialize(self) -> tuple[int, int]:
        self.source.check()
        with closing(self._connect()) as connection, connection:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                "CREATE TABLE IF NOT EXISTS import_jobs (job_id TEXT PRIMARY KEY, "
                "sha256 TEXT NOT NULL, source_size INTEGER NOT NULL, next_offset INTEGER NOT NULL "
                "CHECK(next_offset >= 0 AND next_offset <= source_size), "
                "processed_rows INTEGER NOT NULL "
                "CHECK(typeof(processed_rows) = 'integer' AND processed_rows >= 0))"
            )
            connection.execute(
                "CREATE TABLE IF NOT EXISTS import_documents (job_id TEXT NOT NULL REFERENCES import_jobs, "
                "id TEXT NOT NULL CHECK(length(id) > 0), text TEXT NOT NULL, PRIMARY KEY(job_id, id))"
            )
            connection.execute(
                "INSERT INTO import_jobs VALUES (?, ?, ?, 0, 0) ON CONFLICT(job_id) DO NOTHING",
                (self.job_id, self.source.sha256, self.source.size),
            )
            digest, size, offset, rows = connection.execute(
                "SELECT sha256, source_size, next_offset, processed_rows FROM import_jobs WHERE job_id = ?",
                (self.job_id,),
            ).fetchone()
            if (digest, size) != (self.source.sha256, self.source.size):
                raise SourceChanged(
                    "This job_id belongs to different source contents; restore the original file"
                )
            return offset, rows

    def write(self, records: list[Record]) -> list[str]:
        if not records:
            return []
        self.source.check()
        with closing(self._connect()) as connection, connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT sha256, source_size, next_offset FROM import_jobs WHERE job_id = ?", (self.job_id,)
            ).fetchone()
            if row is None or row[:2] != (self.source.sha256, self.source.size):
                raise SourceChanged("Initialize this job with the matching source before writing")
            offset = row[2]
            for record in records:
                if record.start != offset or not offset < record.end <= self.source.size:
                    raise CheckpointConflict(
                        "Batch is not contiguous with the saved checkpoint; restart the import"
                    )
                offset = record.end
            connection.executemany(
                "INSERT INTO import_documents VALUES (?, ?, ?) "
                "ON CONFLICT(job_id, id) DO UPDATE SET text = excluded.text",
                [(self.job_id, record.document.id, record.document.text) for record in records],
            )
            connection.execute(
                "UPDATE import_jobs SET next_offset = ?, processed_rows = processed_rows + ? "
                "WHERE job_id = ?",
                (offset, len(records), self.job_id),
            )
            self.source.check()
        return [record.document.id for record in records]


async def import_jsonl(
    source_path: Path,
    database: Path,
    job_id: str,
    *,
    executor: ThreadPoolExecutor,
    batch_size: int = 32,
    max_pending: int = 64,
) -> dict:
    """Drain accepted writes on exit; the caller must join its executor afterward."""
    loop = asyncio.get_running_loop()
    source = await loop.run_in_executor(executor, Source.inspect, source_path)
    writer = CheckpointWriter(database, job_id, source)
    offset, previous_rows = await loop.run_in_executor(executor, writer.initialize)
    started = time.perf_counter()
    measurements = BatchMeasurements()

    async def records():
        position = offset
        while position < source.size:
            chunk = await loop.run_in_executor(executor, partial(read_chunk, source, position))
            if not chunk:
                raise SourceChanged("Source ended before its recorded size")
            for record in chunk:
                yield record
            position = chunk[-1].end

    batcher = AsyncBatcher.from_callable(
        writer.write,
        max_batch_size=batch_size,
        max_queue_size=max_pending,
        max_queue_time=0.01,
        concurrency=1,
        executor=executor,
        processor_context=Context(),
        log_exceptions=False,
        on_batch=measurements.observe,
    )
    completed = 0
    async with batcher, aclosing(batcher.map(records(), max_pending=max_pending)) as results:
        async for _ in results:
            completed += 1
    final_offset, processed_rows = await loop.run_in_executor(executor, writer.initialize)
    return {
        "completed_this_run": completed,
        "previously_processed": previous_rows,
        "processed_rows": processed_rows,
        "next_offset": final_offset,
        "source_bytes": source.size,
        "elapsed_seconds": time.perf_counter() - started,
        **measurements.report(batcher),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--job-id", required=True)
    args = parser.parse_args()
    # Joining after asyncio.run also waits for any cancelled file/hash thread call.
    with ThreadPoolExecutor(max_workers=2) as executor:
        report = asyncio.run(import_jsonl(args.input, args.database, args.job_id, executor=executor))
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
