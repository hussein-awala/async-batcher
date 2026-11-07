"""Bounded open-loop SQLite measurements; see benchmarks/SQLITE.md."""

from __future__ import annotations

import argparse
import asyncio
import gzip
import json
import math
import random
import sqlite3
import statistics
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

from async_batcher import AsyncBatcher, GroupedBatcher

from benchmarks.core import metadata, percentiles, sample_lag


@dataclass(frozen=True)
class Config:
    duration: float = 3.0
    repetitions: int = 3
    outstanding: int = 128
    warmup: int = 32
    batch_size: int = 32
    queue_time: float = 0.002
    sparse_rate: float = 25.0
    burst_rate: float = 500.0
    saturated_rate: float = 5000.0
    burst_size: int = 32
    payload_bytes: int = 256
    keys: int = 4
    hot_fraction: float = 0.8
    grouped: bool = False
    drain_timeout: float = 20.0
    seed: int = 42

    def validate(self):
        for name in (
            "repetitions",
            "outstanding",
            "warmup",
            "batch_size",
            "burst_size",
            "payload_bytes",
            "keys",
        ):
            if getattr(self, name) < 1:
                raise ValueError(f"{name} must be positive")
        if self.warmup > self.outstanding:
            raise ValueError("warmup cannot exceed outstanding")
        for name in ("duration", "sparse_rate", "burst_rate", "saturated_rate", "drain_timeout"):
            if not math.isfinite(getattr(self, name)) or getattr(self, name) <= 0:
                raise ValueError(f"{name} must be finite and positive")
        if not math.isfinite(self.queue_time) or self.queue_time < 0:
            raise ValueError("queue_time must be finite and nonnegative")
        if not 0 <= self.hot_fraction <= 1:
            raise ValueError("hot_fraction must be between zero and one")
        if self.duration * max(self.sparse_rate, self.burst_rate, self.saturated_rate) > 250_000:
            raise ValueError("at most 250000 arrivals per trial are supported")


@dataclass(frozen=True)
class Row:
    identifier: int
    key: int
    payload: bytes


def arrivals(config, profile):
    rate = getattr(config, f"{profile}_rate")
    burst = config.burst_size if profile == "burst" else 1
    for index in range(math.ceil(config.duration * rate)):
        offset = (index // burst) * burst / rate
        if offset < config.duration:
            yield index, offset


async def wait_until_due(deadline):
    """Recheck after timer wakeups, which asyncio may deliver slightly early."""
    while (remaining := deadline - time.perf_counter()) > 0:
        await asyncio.sleep(remaining)


class SQLiteWriter:
    """One connection and all its operations belong to one executor thread."""

    def __init__(self, path):
        self.path = path
        self.executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="sqlite-benchmark")
        self.connection = None
        self.calls = []
        self.settings = {}
        self.lock = asyncio.Semaphore(1)

    async def call(self, function, *args):
        return await asyncio.get_running_loop().run_in_executor(self.executor, function, *args)

    def initialize(self):
        self.connection = sqlite3.connect(self.path, timeout=5)
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA synchronous=FULL")
        self.connection.execute(
            "CREATE TABLE records (id INTEGER PRIMARY KEY, group_key INTEGER, payload BLOB)"
        )
        self.connection.commit()
        self.settings = {
            name: self.connection.execute(f"PRAGMA {name}").fetchone()[0]
            for name in ("journal_mode", "synchronous", "fullfsync", "page_size", "wal_autocheckpoint")
        }

    def write(self, rows):
        started = time.perf_counter()
        with self.connection:
            self.connection.executemany(
                "INSERT INTO records VALUES (?, ?, ?)",
                [(row.identifier, row.key, row.payload) for row in rows],
            )
        finished = time.perf_counter()
        self.calls.append({"size": len(rows), "started": started, "finished": finished})
        return [(row.identifier, started, finished) for row in rows]

    async def process(self, rows):
        # Grouped children share the same backend capacity as every baseline.
        async with self.lock:
            return await self.call(self.write, rows)

    def contents(self):
        return self.connection.execute(
            "SELECT id, group_key, payload FROM records WHERE id >= 0 ORDER BY id"
        ).fetchall()

    async def close(self):
        try:
            if self.connection is not None:
                await self.call(self.connection.close)
        finally:
            self.executor.shutdown(wait=True, cancel_futures=True)


class Direct:
    def __init__(self, writer):
        self.writer = writer

    async def process(self, row):
        return (await self.writer.process([row]))[0]

    async def stop(self, force=False):
        pass


class ApplicationBatcher:
    """Deliberately small queue/count/deadline batcher, not a production library."""

    def __init__(self, writer, config):
        self.writer = writer
        self.config = config
        self.queue = asyncio.Queue()
        self.worker = asyncio.create_task(self.run())

    async def process(self, row):
        future = asyncio.get_running_loop().create_future()
        self.queue.put_nowait((row, future, time.perf_counter()))
        return await future

    async def run(self):
        while True:
            first = await self.queue.get()
            batch = [first]
            deadline = first[2] + self.config.queue_time
            while len(batch) < self.config.batch_size:
                try:
                    entry = self.queue.get_nowait()
                except asyncio.QueueEmpty:
                    remaining = deadline - time.perf_counter()
                    if remaining <= 0:
                        break
                    try:
                        entry = await asyncio.wait_for(self.queue.get(), timeout=remaining)
                    except TimeoutError:
                        break
                batch.append(entry)
            try:
                results = await self.writer.process([row for row, _, _ in batch])
            except Exception as error:
                for _, future, _ in batch:
                    if not future.done():
                        future.set_exception(error)
            else:
                for (_, future, _), result in zip(batch, results, strict=True):
                    if not future.done():
                        future.set_result(result)

    async def stop(self, force=False):
        self.worker.cancel()
        await asyncio.gather(self.worker, return_exceptions=True)


def strategy(mode, writer, config):
    def child(_key=None):
        return AsyncBatcher.from_callable(
            writer.process,
            max_batch_size=config.batch_size,
            max_queue_time=config.queue_time,
            max_queue_size=config.outstanding,
            concurrency=1,
            log_exceptions=False,
        )

    if mode == "direct":
        return Direct(writer)
    if mode == "application":
        return ApplicationBatcher(writer, config)
    if mode == "library":
        return child()
    if mode == "grouped":
        return GroupedBatcher(
            child,
            group_key=lambda row: row.key,
            max_groups=config.keys,
            idle_timeout=config.duration + config.drain_timeout + 5,
        )
    raise ValueError(f"unknown mode: {mode}")


def summarize(records, duration, finished, calls, peak, lag):
    accepted = [record for record in records if record["status"] == "completed"]
    rejected = [record for record in records if record["status"] == "rejected"]
    if len(accepted) + len(rejected) != len(records):
        raise RuntimeError("a benchmark request did not settle successfully")

    def metrics(rows):
        completed = [row for row in rows if row["status"] == "completed"]
        return {
            "offered": len(rows),
            "completed": len(completed),
            "rejected": len(rows) - len(completed),
            "latency_ms": percentiles([(row["completed"] - row["scheduled"]) * 1000 for row in completed]),
            "queue_ms": percentiles([(row["started"] - row["launched"]) * 1000 for row in completed]),
        }

    return {
        **metrics(records),
        "offered_per_second": len(records) / duration,
        "completed_per_second_including_drain": len(accepted) / finished,
        "completed_within_arrival_window": sum(row["completed"] <= duration for row in accepted),
        "drain_seconds": max(0, finished - duration),
        "elapsed_seconds": finished,
        "peak_outstanding": peak,
        "launch_lateness_ms": percentiles([(row["launched"] - row["scheduled"]) * 1000 for row in records]),
        "event_loop_lag_ms": percentiles(lag),
        "batch_sizes": percentiles([call["size"] for call in calls]),
        "transaction_count": len(calls),
        "per_key": {
            str(key): metrics([row for row in records if row["key"] == key])
            for key in sorted({row["key"] for row in records})
        },
    }


async def trial(config, mode, profile, database):
    config.validate()
    writer = SQLiteWriter(database)
    batcher = None
    pending = set()
    records = []
    failures = []
    sampler = None
    lag = []
    peak = 0
    rng = random.Random(config.seed)
    payload = b"x" * config.payload_bytes
    expected_rows = {}
    try:
        async with asyncio.timeout(config.duration + config.drain_timeout + 5):
            await writer.call(writer.initialize)
            batcher = strategy(mode, writer, config)
            pending = {
                asyncio.create_task(batcher.process(Row(-index - 1, index % config.keys, payload)))
                for index in range(config.warmup)
            }
            await asyncio.gather(*pending)
            pending.clear()
            writer.calls.clear()
            ready = asyncio.Event()
            sampler = asyncio.create_task(sample_lag(0.005, lag, ready))
            await ready.wait()
            origin = time.perf_counter()

            async def execute(row, record):
                try:
                    identifier, started, _ = await batcher.process(row)
                    if identifier != row.identifier:
                        raise RuntimeError("processor returned the wrong request ID")
                    record.update(
                        status="completed", started=started - origin, completed=time.perf_counter() - origin
                    )
                except Exception as error:
                    record["status"] = "failed"
                    failures.append(error)

            for identifier, scheduled in arrivals(config, profile):
                remaining = origin + scheduled - time.perf_counter()
                if remaining > 0:
                    await wait_until_due(origin + scheduled)
                elif identifier % 64 == 0:
                    await asyncio.sleep(0)  # Bound catch-up work without moving scheduled arrival times.
                if failures:
                    raise failures[0]
                key = (
                    0
                    if config.keys == 1 or rng.random() < config.hot_fraction
                    else rng.randrange(1, config.keys)
                )
                record = {
                    "id": identifier,
                    "key": key,
                    "scheduled": scheduled,
                    "launched": time.perf_counter() - origin,
                    "status": "rejected",
                }
                records.append(record)
                # One identical admission boundary for all implementations. Completed
                # tasks release capacity even if their callbacks have not run yet.
                pending = {task for task in pending if not task.done()}
                if len(pending) >= config.outstanding:
                    continue
                row = Row(identifier, key, payload)
                expected_rows[identifier] = (key, payload)
                record["status"] = "pending"
                pending.add(asyncio.create_task(execute(row, record)))
                peak = max(peak, len(pending))
            await wait_until_due(origin + config.duration)
            if pending:
                await asyncio.gather(*pending)
            finished = time.perf_counter() - origin
            if failures:
                raise failures[0]
            await batcher.stop()
            sampler.cancel()
            await asyncio.gather(sampler, return_exceptions=True)
            actual = {
                identifier: (key, value) for identifier, key, value in await writer.call(writer.contents)
            }
            if actual != expected_rows:
                raise RuntimeError("persisted SQLite rows differ from accepted requests")
            calls = [
                {**call, "started": call["started"] - origin, "finished": call["finished"] - origin}
                for call in writer.calls
            ]
            return {
                "summary": summarize(records, config.duration, finished, calls, peak, lag),
                "requests": records,
                "transactions": calls,
                "lag_samples_ms": lag,
                "sqlite_settings": writer.settings,
                "persisted_rows_verified": len(actual),
            }
    finally:
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        if sampler is not None:
            sampler.cancel()
            await asyncio.gather(sampler, return_exceptions=True)
        try:
            if batcher is not None:
                await batcher.stop(force=True)
        finally:
            await writer.close()


async def experiment(config, storage=None):
    config.validate()
    modes = ["direct", "application", "library"] + (["grouped"] if config.grouped else [])
    cases = [(mode, profile) for mode in modes for profile in ("sparse", "burst", "saturated")]
    report = {
        "schema_version": 1,
        "started_at": datetime.now(UTC).isoformat(),
        "environment": {**metadata(), "sqlite_version": sqlite3.sqlite_version},
        "config": asdict(config),
        "storage_parent": str(storage or tempfile.gettempdir()),
        "trial_order": [],
        "cases": {f"{mode}/{profile}": [] for mode, profile in cases},
    }
    for repetition in range(config.repetitions):
        ordered = cases.copy()
        random.Random(config.seed + repetition).shuffle(ordered)
        for mode, profile in ordered:
            name = f"{mode}/{profile}"
            report["trial_order"].append({"repetition": repetition + 1, "case": name})
            with tempfile.TemporaryDirectory(prefix="async-batcher-sqlite-", dir=storage) as directory:
                result = await trial(config, mode, profile, Path(directory) / "records.sqlite")
            report["cases"][name].append(result)
    report["finished_at"] = datetime.now(UTC).isoformat()
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    defaults = Config()
    for name in (
        "duration",
        "queue_time",
        "sparse_rate",
        "burst_rate",
        "saturated_rate",
        "hot_fraction",
        "drain_timeout",
    ):
        parser.add_argument(f"--{name.replace('_', '-')}", type=float, default=getattr(defaults, name))
    for name in (
        "repetitions",
        "outstanding",
        "warmup",
        "batch_size",
        "burst_size",
        "payload_bytes",
        "keys",
        "seed",
    ):
        parser.add_argument(f"--{name.replace('_', '-')}", type=int, default=getattr(defaults, name))
    parser.add_argument("--grouped", action="store_true")
    parser.add_argument("--storage", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = vars(parser.parse_args(argv))
    output = args.pop("output")
    storage = args.pop("storage")
    config = Config(**args)
    try:
        config.validate()
    except ValueError as error:
        parser.error(str(error))
    report = asyncio.run(experiment(config, storage))
    output.parent.mkdir(parents=True, exist_ok=True)
    opener = gzip.open if output.suffix == ".gz" else open
    with opener(output, "wt", encoding="utf-8") as stream:
        json.dump(report, stream, indent=2, allow_nan=False)
        stream.write("\n")
    print("case | median completed/s | median rejected | median p95 latency ms")
    for name, trials in report["cases"].items():
        summaries = [trial["summary"] for trial in trials]
        throughput = statistics.median(s["completed_per_second_including_drain"] for s in summaries)
        rejected = statistics.median(s["rejected"] for s in summaries)
        latency = statistics.median(s["latency_ms"]["p95"] for s in summaries)
        print(f"{name} | {throughput:.1f} | {rejected:g} | {latency:.3f}")
    print(f"Raw arrival, transaction, per-key, and environment data: {output}")


if __name__ == "__main__":
    main()
