"""Persist validated embeddings before replayable vector-store delivery."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import math
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from contextlib import aclosing, closing
from contextvars import Context
from dataclasses import asdict, dataclass
from functools import partial
from pathlib import Path

from async_batcher import AsyncBatcher
from examples.recipes.embedding_vectors import DemoEmbeddingModel, SQLiteVectorSink, VectorRecord
from examples.recipes.resumable_sqlite import Source, read_chunk


@dataclass(frozen=True)
class Identity:
    provider: str
    model: str
    revision: str
    preprocessing: str
    input_type: str
    dimension: int
    destination: str

    def encode(self):
        for name, value in asdict(self).items():
            if name != "dimension" and (not isinstance(value, str) or not value.strip()):
                raise ValueError(f"{name} must be an explicit nonempty string")
        if type(self.dimension) is not int or not 1 <= self.dimension <= 20000:
            raise ValueError("dimension must be an integer from 1 through 20000")
        return json.dumps(asdict(self), sort_keys=True, separators=(",", ":"))


def validated_vectors(rows, count, dimension):
    if not isinstance(rows, list | tuple) or len(rows) != count:
        raise ValueError("Model must return exactly one vector per input")
    result = []
    for row in rows:
        if not isinstance(row, list | tuple) or len(row) != dimension:
            raise ValueError("Embedding dimension does not match the job identity")
        if any(isinstance(value, bool) or not isinstance(value, int | float) for value in row):
            raise ValueError("Embeddings must contain finite numeric values")
        try:
            values = [float(value) for value in row]
        except (ValueError, OverflowError):
            raise ValueError("Embeddings must contain finite numeric values") from None
        if not all(math.isfinite(value) for value in values):
            raise ValueError("Embeddings must contain finite numeric values")
        result.append(values)
    return result


class Spool:
    def __init__(self, database: Path, job_id: str, identity: Identity):
        if not isinstance(job_id, str) or not job_id:
            raise ValueError("job_id must be a nonempty string")
        self.database, self.job_id, self.identity = database, job_id, identity
        self.configuration = identity.encode()

    def connect(self):
        connection = sqlite3.connect(self.database, timeout=5)
        try:
            if connection.execute("PRAGMA journal_mode=WAL").fetchone()[0] != "wal":
                raise RuntimeError("The spool requires file-backed SQLite with WAL")
            connection.execute("PRAGMA synchronous=FULL")
            connection.execute("PRAGMA foreign_keys=ON")
            return connection
        except BaseException:
            connection.close()
            raise

    def initialize(self, source: Source):
        with closing(self.connect()) as connection, connection:
            connection.executescript(
                "CREATE TABLE IF NOT EXISTS embedding_jobs (job_id TEXT PRIMARY KEY, "
                "configuration TEXT NOT NULL, source_sha TEXT NOT NULL, source_size INTEGER NOT NULL);"
                "CREATE TABLE IF NOT EXISTS embedding_spool (job_id TEXT NOT NULL REFERENCES embedding_jobs, "
                "offset INTEGER NOT NULL, document_id TEXT NOT NULL, text_sha TEXT NOT NULL, "
                "vector TEXT NOT NULL, delivered INTEGER NOT NULL DEFAULT 0 CHECK(delivered IN (0, 1)), "
                "PRIMARY KEY(job_id, offset), UNIQUE(job_id, document_id));"
            )
            connection.execute(
                "INSERT INTO embedding_jobs VALUES (?, ?, ?, ?) ON CONFLICT(job_id) DO NOTHING",
                (self.job_id, self.configuration, source.sha256, source.size),
            )
            row = connection.execute(
                "SELECT configuration, source_sha, source_size FROM embedding_jobs WHERE job_id = ?",
                (self.job_id,),
            ).fetchone()
            if row != (self.configuration, source.sha256, source.size):
                raise ValueError("Job identity or source contents changed; use the original job inputs")

    def _check(self, connection):
        row = connection.execute(
            "SELECT configuration FROM embedding_jobs WHERE job_id = ?", (self.job_id,)
        ).fetchone()
        if row != (self.configuration,):
            raise ValueError("Initialize the spool with the matching job identity")

    @staticmethod
    def content(record):
        return hashlib.sha256(record.document.text.encode("utf-8")).hexdigest()

    def missing(self, records):
        with closing(self.connect()) as connection:
            self._check(connection)
            missing = []
            identifiers = set()
            for record in records:
                if record.document.id in identifiers:
                    raise ValueError("Source document IDs must be unique")
                identifiers.add(record.document.id)
                row = connection.execute(
                    "SELECT offset, document_id, text_sha FROM embedding_spool "
                    "WHERE job_id = ? AND (offset = ? OR document_id = ?)",
                    (self.job_id, record.start, record.document.id),
                ).fetchall()
                expected = (record.start, record.document.id, self.content(record))
                if row and row != [expected]:
                    raise ValueError("Source document identity conflicts with the durable spool")
                if not row:
                    missing.append(record)
            return missing

    def save(self, records, vectors):
        # All vectors have been validated before this transaction starts.
        with closing(self.connect()) as connection, connection:
            self._check(connection)
            connection.executemany(
                "INSERT INTO embedding_spool(job_id, offset, document_id, text_sha, vector) "
                "VALUES (?, ?, ?, ?, ?)",
                [
                    (
                        self.job_id,
                        record.start,
                        record.document.id,
                        self.content(record),
                        json.dumps(vector, allow_nan=False),
                    )
                    for record, vector in zip(records, vectors, strict=True)
                ],
            )

    def pending(self, after: int, limit: int):
        with closing(self.connect()) as connection:
            self._check(connection)
            rows = connection.execute(
                "SELECT offset, document_id, vector FROM embedding_spool "
                "WHERE job_id = ? AND delivered = 0 AND offset > ? ORDER BY offset LIMIT ?",
                (self.job_id, after, limit),
            ).fetchall()
        return [
            (
                offset,
                VectorRecord(
                    identifier, validated_vectors([json.loads(vector)], 1, self.identity.dimension)[0]
                ),
            )
            for offset, identifier, vector in rows
        ]

    def acknowledge(self, offsets):
        with closing(self.connect()) as connection, connection:
            self._check(connection)
            connection.executemany(
                "UPDATE embedding_spool SET delivered = 1 WHERE job_id = ? AND offset = ?",
                [(self.job_id, offset) for offset in offsets],
            )

    def counts(self):
        with closing(self.connect()) as connection:
            self._check(connection)
            total, delivered = connection.execute(
                "SELECT COUNT(*), COALESCE(SUM(delivered), 0) FROM embedding_spool WHERE job_id = ?",
                (self.job_id,),
            ).fetchone()
        return {
            "persisted_embeddings": total,
            "acknowledged_vectors": delivered,
            "pending_vectors": total - delivered,
        }


def limits(batch_size, max_pending):
    for name, value in (("batch_size", batch_size), ("max_pending", max_pending)):
        if type(value) is not int or value < 1:
            raise ValueError(f"{name} must be a positive integer")


async def stage_file(path, spool, model, *, executor, batch_size=32, max_pending=64):
    """Borrow an async text-list -> vector-list model and the caller's executor."""
    limits(batch_size, max_pending)
    loop = asyncio.get_running_loop()

    async def local(function, *args):
        return await loop.run_in_executor(executor, function, *args)

    source = await local(Source.inspect, path)
    await local(spool.initialize, source)

    async def records():
        offset = 0
        while offset < source.size:
            chunk = await local(partial(read_chunk, source, offset))
            if not chunk:
                raise RuntimeError("Source ended before its recorded size")
            for record in chunk:
                yield record
            offset = chunk[-1].end

    async def embed(records):
        missing = await local(spool.missing, records)
        if missing:
            vectors = validated_vectors(
                await model.embed([r.document.text for r in missing]), len(missing), spool.identity.dimension
            )
            await local(source.check)
            await local(spool.save, missing, vectors)
        return [record.document.id for record in records]

    async with (
        AsyncBatcher.from_callable(
            embed,
            max_batch_size=batch_size,
            max_queue_size=max_pending,
            concurrency=1,
            processor_context=Context(),
            log_exceptions=False,
        ) as batcher,
        aclosing(batcher.map(records(), max_pending=max_pending)) as results,
    ):
        async for _ in results:
            pass
    await local(source.check)
    return await local(spool.counts)


async def deliver_pending(spool, sink, *, executor, batch_size=32, max_pending=64):
    """Replay durable vectors; sink upserts must converge for the same ID/vector."""
    limits(batch_size, max_pending)
    loop = asyncio.get_running_loop()

    async def entries():
        after = -1
        while True:
            page = await loop.run_in_executor(executor, spool.pending, after, max_pending)
            if not page:
                return
            for entry in page:
                yield entry
            after = page[-1][0]

    async def write(entries):
        records = [record for _, record in entries]
        identifiers = await sink.upsert(records)
        if identifiers != [record.id for record in records]:
            raise ValueError("Sink must acknowledge every input ID in order")
        await loop.run_in_executor(executor, spool.acknowledge, [offset for offset, _ in entries])
        return identifiers

    async with (
        AsyncBatcher.from_callable(
            write,
            max_batch_size=batch_size,
            max_queue_size=max_pending,
            concurrency=1,
            processor_context=Context(),
            log_exceptions=False,
        ) as batcher,
        aclosing(batcher.map(entries(), max_pending=max_pending)) as results,
    ):
        async for _ in results:
            pass
    return await loop.run_in_executor(executor, spool.counts)


async def demo(args, executor):
    identity = Identity(
        "local-demo", "text-features", "v1", "identity", "document", 3, str(args.sink.resolve())
    )
    spool = Spool(args.spool, args.job_id, identity)
    sink = SQLiteVectorSink(args.sink)
    await asyncio.get_running_loop().run_in_executor(executor, sink.initialize)
    await stage_file(args.input, spool, DemoEmbeddingModel(), executor=executor)
    return await deliver_pending(spool, sink, executor=executor)


def main():
    parser = argparse.ArgumentParser(description=__doc__ + " Demo model is NOT a semantic embedding model.")
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--spool", required=True, type=Path)
    parser.add_argument("--sink", required=True, type=Path)
    parser.add_argument("--job-id", required=True)
    args = parser.parse_args()
    with ThreadPoolExecutor(max_workers=2) as executor:
        report = asyncio.run(demo(args, executor))
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
