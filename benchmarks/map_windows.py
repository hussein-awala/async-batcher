"""Measure cheap async map overhead with bounded pending windows; no speed assertions."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import platform
import random
import statistics
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path


async def trial(batcher_type, requests, window):
    async def process(batch):
        return [item * 2 for item in batch]

    batcher = batcher_type.from_callable(
        process, max_batch_size=64, max_queue_size=window, max_queue_time=0, concurrency=4
    )
    started = time.perf_counter()
    count = 0
    try:
        async with asyncio.timeout(60):
            async for value in batcher.map(range(requests), max_pending=window):
                if value != count * 2:
                    raise AssertionError(f"Misrouted result at {count}: {value}")
                count += 1
    finally:
        await batcher.stop(force=count != requests)
    elapsed = time.perf_counter() - started
    if count != requests:
        raise AssertionError(f"Incomplete stream {count}/{requests}")
    return {
        "window": window,
        "requests": requests,
        "elapsed_seconds": elapsed,
        "items_per_second": requests / elapsed,
        "batches": batcher.stats().batches_completed,
    }


async def main(args):
    sys.path.insert(0, str(args.repo.resolve()))
    import async_batcher
    from async_batcher import AsyncBatcher

    origin = Path(async_batcher.__file__).resolve()
    if not origin.is_relative_to(args.repo.resolve()):
        raise RuntimeError(f"Wrong checkout imported: {origin}")
    rng = random.Random(args.seed)
    raw = []
    orders = []
    started = datetime.now(UTC).isoformat()
    for repetition in range(args.repetitions):
        windows = args.windows.copy()
        rng.shuffle(windows)
        orders.append(windows)
        for window in windows:
            await trial(AsyncBatcher, min(1024, args.requests), window)
            result = await trial(AsyncBatcher, args.requests, window)
            result["repetition"] = repetition
            raw.append(result)
            print(json.dumps(result), flush=True)

    def git(*values):
        return subprocess.check_output(["git", *values], cwd=args.repo, text=True).strip()

    result = {
        "label": args.label,
        "started_at": started,
        "finished_at": datetime.now(UTC).isoformat(),
        "python": sys.version,
        "platform": platform.platform(),
        "machine": platform.machine(),
        "source": str(origin),
        "commit": git("rev-parse", "HEAD"),
        "dirty": bool(git("status", "--porcelain")),
        "runtime_sha256": {
            name: hashlib.sha256((args.repo / name).read_bytes()).hexdigest()
            for name in ("async_batcher/map.py", "async_batcher/batcher.py")
        },
        "settings": {
            "requests": args.requests,
            "windows": args.windows,
            "repetitions": args.repetitions,
            "seed": args.seed,
            "batch_size": 64,
            "concurrency": 4,
            "ordered": True,
            "queue_time": 0,
        },
        "case_order": orders,
        "trials": raw,
        "median_seconds": {
            str(w): statistics.median(r["elapsed_seconds"] for r in raw if r["window"] == w)
            for w in args.windows
        },
    }
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result["median_seconds"]), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--label", required=True)
    parser.add_argument("--requests", type=int, default=16384)
    parser.add_argument("--windows", type=int, nargs="+", default=[128, 1024, 8192])
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--seed", type=int, default=17)
    args = parser.parse_args()
    if args.requests < 1 or args.repetitions < 1 or any(w < 1 for w in args.windows):
        parser.error("requests, repetitions and windows must be positive")
    asyncio.run(main(args))
