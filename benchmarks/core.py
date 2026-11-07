"""Bounded synthetic core benchmarks; see benchmarks/README.md for interpretation."""

from __future__ import annotations

import argparse
import asyncio
import importlib.metadata
import json
import math
import os
import platform
import random
import statistics
import subprocess
import sys
import threading
import time
import tracemalloc
from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

from async_batcher import AsyncBatcher


@dataclass(frozen=True)
class Config:
    requests: int = 1000
    warmup: int = 128
    repetitions: int = 3
    outstanding: int = 128
    batch_sizes: tuple[int, ...] = (1, 8, 32)
    concurrencies: tuple[int, ...] = (1, 4)
    workloads: tuple[str, ...] = ("async", "thread")
    fixed_delay: float = 0.001
    per_item_delay: float = 0.00002
    queue_time: float = 0.001
    lag_interval: float = 0.005
    timeout: float = 30.0
    seed: int = 42

    def validate(self):
        for name in ("requests", "warmup", "repetitions", "outstanding"):
            if getattr(self, name) < 1:
                raise ValueError(f"{name} must be positive")
        for name in ("batch_sizes", "concurrencies"):
            values = getattr(self, name)
            if not values or any(value < 1 for value in values) or len(set(values)) != len(values):
                raise ValueError(f"{name} must contain distinct positive integers")
        if not self.workloads or len(set(self.workloads)) != len(self.workloads):
            raise ValueError("workloads must be nonempty and distinct")
        if set(self.workloads) - {"async", "thread"}:
            raise ValueError("workloads must be async or thread")
        for name in ("fixed_delay", "per_item_delay", "queue_time", "lag_interval", "timeout"):
            value = getattr(self, name)
            if not math.isfinite(value) or value < 0:
                raise ValueError(f"{name} must be finite and nonnegative")
        if not self.lag_interval or not self.timeout:
            raise ValueError("lag_interval and timeout must be positive")
        if self.fixed_delay + self.per_item_delay * max(self.batch_sizes) > self.timeout:
            raise ValueError("one operation's simulated delay exceeds the trial timeout")


@dataclass(frozen=True)
class Case:
    workload: str
    concurrency: int
    batch_size: int | None  # None is the direct single-item baseline.

    @property
    def name(self):
        mode = "direct" if self.batch_size is None else f"batch-{self.batch_size}"
        return f"{self.workload}/{mode}/concurrency-{self.concurrency}"


def percentiles(values):
    """Linear-interpolated percentiles; empty samples remain explicitly absent."""
    ordered = sorted(values)
    result = {"samples": len(ordered)}
    for label, fraction in (("p50", 0.5), ("p95", 0.95), ("p99", 0.99)):
        if not ordered:
            result[label] = None
            continue
        position = (len(ordered) - 1) * fraction
        lower = math.floor(position)
        upper = math.ceil(position)
        result[label] = ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)
    result["max"] = max(ordered) if ordered else None
    return result


class Operation:
    def __init__(self, config):
        self.config = config
        self.calls = 0
        self.items = 0
        self.smallest = math.inf
        self.largest = 0
        self.lock = threading.Lock()

    def delay(self, items):
        with self.lock:
            self.calls += 1
            self.items += len(items)
            self.smallest = min(self.smallest, len(items))
            self.largest = max(self.largest, len(items))
        return self.config.fixed_delay + self.config.per_item_delay * len(items)

    async def asynchronous(self, items):
        await asyncio.sleep(self.delay(items))
        return [item * 2 for item in items]

    def synchronous(self, items):
        time.sleep(self.delay(items))
        return [item * 2 for item in items]


class AsyncOperationBatcher(AsyncBatcher):
    async def process_batch(self, batch):
        return await self.operation.asynchronous(batch)


class ThreadOperationBatcher(AsyncBatcher):
    def process_batch(self, batch):
        return self.operation.synchronous(batch)


async def sample_lag(interval, samples, ready):
    loop = asyncio.get_running_loop()
    while True:
        expected = loop.time() + interval
        ready.set()
        await asyncio.sleep(interval)
        samples.append(max(0.0, loop.time() - expected) * 1000)


async def trial(config, case, requests, executor=None, *, memory=False):
    operation = Operation(config)
    semaphore = asyncio.Semaphore(case.concurrency)
    batcher = None
    if case.batch_size is not None:
        cls = AsyncOperationBatcher if case.workload == "async" else ThreadOperationBatcher
        batcher = cls(
            max_batch_size=case.batch_size,
            max_queue_time=config.queue_time,
            max_queue_size=config.outstanding,
            concurrency=case.concurrency,
            executor=executor,
        )
        batcher.operation = operation

    async def submit(item):
        if batcher is not None:
            return await batcher.process(item)
        async with semaphore:
            if case.workload == "async":
                return (await operation.asynchronous([item]))[0]
            loop = asyncio.get_running_loop()
            return (await loop.run_in_executor(executor, operation.synchronous, [item]))[0]

    inputs = iter(range(requests))
    latencies = []
    lag = []
    ready = asyncio.Event()

    async def worker():
        for item in inputs:
            started = time.perf_counter()
            result = await submit(item)
            elapsed = time.perf_counter() - started
            if result != item * 2:
                raise RuntimeError(f"Incorrect result for request {item}")
            latencies.append(elapsed * 1000)

    if memory and tracemalloc.is_tracing():
        raise RuntimeError("Memory trials require tracemalloc to be inactive")
    if memory:
        tracemalloc.start()
    sampler = asyncio.create_task(sample_lag(config.lag_interval, lag, ready))
    tasks = []
    succeeded = False
    try:
        await ready.wait()
        started = time.perf_counter()
        tasks = [asyncio.create_task(worker()) for _ in range(min(requests, config.outstanding))]
        async with asyncio.timeout(config.timeout):
            await asyncio.gather(*tasks)
        duration = time.perf_counter() - started
        peak = tracemalloc.get_traced_memory()[1] if memory else None
        succeeded = True
    finally:
        sampler.cancel()
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(sampler, *tasks, return_exceptions=True)
        try:
            if batcher is not None:
                await batcher.stop(force=not succeeded)
        finally:
            if memory:
                tracemalloc.stop()
    if len(latencies) != requests or operation.items != requests:
        raise RuntimeError("Incomplete workload; refusing to report performance")
    return {
        "requests": requests,
        "request_worker_tasks": len(tasks),
        "sampler_tasks": 1,
        "dispatcher_tasks": int(batcher is not None),
        "batch_worker_tasks_created": operation.calls if batcher is not None else 0,
        "executor_thread_limit": case.concurrency if case.workload == "thread" else 0,
        "elapsed_seconds": duration,
        "requests_per_second": requests / duration,
        "latency_ms": percentiles(latencies),
        "event_loop_lag_ms": percentiles(lag),
        "operation_calls": operation.calls,
        "batch_items": {
            "min": operation.smallest,
            "max": operation.largest,
            "mean": operation.items / operation.calls,
        },
        "peak_traced_bytes": peak,
    }


def metadata():
    repo = Path(__file__).resolve().parents[1]

    def git(*args):
        try:
            return subprocess.check_output(
                ["git", *args], cwd=repo, text=True, stderr=subprocess.DEVNULL
            ).strip()
        except (OSError, subprocess.CalledProcessError):
            return None

    status = git("status", "--porcelain")
    return {
        "commit": git("rev-parse", "HEAD"),
        "working_tree_dirty": bool(status) if status is not None else None,
        "python": sys.version,
        "implementation": platform.python_implementation(),
        "platform": platform.platform(),
        "machine": platform.machine(),
        "logical_cpu_count": os.cpu_count(),
        "packages": dict(sorted((d.metadata["Name"], d.version) for d in importlib.metadata.distributions())),
    }


async def experiment(config):
    config.validate()
    cases = [
        Case(workload, concurrency, batch_size)
        for workload in config.workloads
        for concurrency in config.concurrencies
        for batch_size in (None, *config.batch_sizes)
    ]
    report = {
        "schema_version": 1,
        "started_at": datetime.now(UTC).isoformat(),
        "environment": metadata(),
        "config": asdict(config),
        "trial_order": [],
        "cases": {case.name: {"case": asdict(case), "timing_trials": []} for case in cases},
    }
    # Separate traced memory passes avoid charging tracemalloc overhead to timing results.
    for repetition in range(config.repetitions + 1):
        memory = repetition == config.repetitions
        ordered = cases.copy()
        random.Random(config.seed + repetition).shuffle(ordered)
        for case in ordered:
            report["trial_order"].append({"case": case.name, "phase": "memory" if memory else repetition + 1})
            context = (
                ThreadPoolExecutor(max_workers=case.concurrency)
                if case.workload == "thread"
                else nullcontext()
            )
            with context as executor:
                await trial(config, case, config.warmup, executor)
                measured = await trial(config, case, config.requests, executor, memory=memory)
            result = report["cases"][case.name]
            if memory:
                result["memory_trial"] = measured
            else:
                result["timing_trials"].append(measured)
    report["finished_at"] = datetime.now(UTC).isoformat()
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("requests", "warmup", "repetitions", "outstanding", "seed"):
        parser.add_argument(f"--{name}", type=int, default=getattr(Config(), name))
    for name in ("fixed_delay", "per_item_delay", "queue_time", "lag_interval", "timeout"):
        parser.add_argument(f"--{name.replace('_', '-')}", type=float, default=getattr(Config(), name))
    parser.add_argument("--batch-sizes", nargs="+", type=int, default=Config().batch_sizes)
    parser.add_argument("--concurrencies", nargs="+", type=int, default=Config().concurrencies)
    parser.add_argument("--workloads", nargs="+", choices=["async", "thread"], default=Config().workloads)
    parser.add_argument("--output", type=Path, required=True)
    args = vars(parser.parse_args(argv))
    output = args.pop("output")
    config = Config(**args)
    try:
        config.validate()
    except ValueError as error:
        parser.error(str(error))
    report = asyncio.run(experiment(config))
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, allow_nan=False) + "\n")
    print("case | median req/s | median p50/p95/p99 ms | memory-pass peak KiB")
    for name, result in report["cases"].items():
        timings = result["timing_trials"]
        rate = statistics.median(t["requests_per_second"] for t in timings)
        latency = "/".join(
            f"{statistics.median(t['latency_ms'][p] for t in timings):.3f}" for p in ("p50", "p95", "p99")
        )
        peak = result["memory_trial"]["peak_traced_bytes"] / 1024
        print(f"{name} | {rate:.1f} | {latency} | {peak:.1f}")
    print(f"Raw trials, lag percentiles and environment: {output}")


if __name__ == "__main__":
    main()
