# Reproducible core benchmark

This service-free harness compares `AsyncBatcher` with direct operations on the same checkout.
It measures a **synthetic fixed-cost bulk operation**, not a database, model, HTTP service, or
universal library speedup. No cloud credentials, server, or optional dependency is used.

For real database arrivals and overload measurements, see the [open-loop SQLite experiment](SQLITE.md).

## Run

From the repository root, install only the core checkout:

```bash
uv sync --locked --no-default-groups --python 3.12
uv run --no-sync python -m unittest discover -s benchmarks -p 'test_*.py'
uv run --no-sync python -m benchmarks.core --output /tmp/async-batcher-core.json
```

The default experiment uses 1,000 measured requests per trial, 128 warmup requests, three timing
repetitions, and a separate memory pass. It compares async and thread workloads, operation
concurrency 1 and 4, and batch sizes 1, 8, and 32 against a direct baseline for each concurrency.
At most 128 request-worker tasks submit work at once. Run trials on an otherwise idle machine;
record whether it is on battery, its power mode, and other relevant load when publishing a report.
Keep output outside the checkout so a previous result does not mark the next run's tree dirty.

For a smaller smoke run:

```bash
uv run --no-sync python -m benchmarks.core \
  --requests 32 --warmup 16 --repetitions 1 --outstanding 8 \
  --batch-sizes 1 4 --concurrencies 1 2 \
  --output /tmp/async-batcher-smoke.json
```

For a controlled representative run, spell out and retain the settings:

```bash
uv run --no-sync python -m benchmarks.core \
  --requests 1000 --warmup 128 --repetitions 5 --outstanding 128 \
  --batch-sizes 1 8 32 --concurrencies 1 4 \
  --fixed-delay 0.001 --per-item-delay 0.00002 --queue-time 0.001 \
  --lag-interval 0.005 --seed 42 \
  --output /tmp/async-batcher-core.json
```

## Workload and baseline

Every logical request carries an integer; the operation returns that integer doubled. A service
call containing `n` items waits for `fixed_delay + n * per_item_delay` seconds and then computes
the results. The async workload uses `asyncio.sleep`; the thread workload uses `time.sleep` in
an explicitly sized `ThreadPoolExecutor`. The latter models synchronous blocking I/O, **not
CPU-bound Python work**. Timer resolution and OS scheduling can make waits longer than requested.

The direct baseline uses the same operation with one item per call, a semaphore limiting calls
to the selected concurrency, and the same outstanding-request limit. The batcher limits active
batches to that concurrency and uses the same thread-pool size for the thread workload. Its queue
capacity equals the outstanding limit. Batch size 1 is an overhead control; larger batches can
amortize the modeled fixed delay. Real APIs may have a different cost curve, size limits, and
error behavior. This benchmark assumes a bulk primitive exists and succeeds.

This is a bounded **closed-loop** workload: each request worker submits another item only after
its previous result arrives. It does not simulate a fixed external arrival rate. Slow cases
therefore generate fewer arrivals per second, and measured latency includes waiting behind other
requests. Do not interpret these percentiles as production overload or capacity guarantees.

Each timing or memory trial gets its own warmup on the same executor used for that trial.
Warmup requests are excluded from measured counts. Case order is shuffled deterministically for
each repetition using the recorded seed, reducing consistent ordering bias. No measured case
runs concurrently with another. A per-trial timeout prevents stalled async work from yielding
an apparently successful result; a running thread must still finish its sleep during cleanup.

## Metrics and report

The JSON output records the exact Git commit, dirty-tree flag, Python/platform/CPU information,
installed package versions, configuration, UTC start/end times, case order, and every raw trial.
Use a clean committed checkout for published measurements. Do not compare reports solely by the
package version: development checkouts can retain the same package version across commits.

- **Throughput:** completed logical requests divided by elapsed time from starting request workers
  until all their results arrive. Warmup and batcher/executor shutdown are excluded.
- **Latency:** per-request elapsed time from submission until its result, including semaphore or
  queue wait. p50/p95/p99 use linear interpolation between sorted samples.
- **Event-loop lag:** how late the sampler resumes after a scheduled sleep. The default interval is
  5 ms; each sample schedules its next sleep from the current time rather than catching up missed
  ticks. Empty samples are `null`, not zero. Sampling itself adds some overhead to both modes.
- **Memory:** peak Python allocations tracked by `tracemalloc` in one separate measured pass after
  warmup. It starts immediately before the sampler/request tasks, excludes initial operation and
  batcher construction, and includes latency/lag lists and harness tasks. It is not process RSS,
  native-library memory, or a statistically repeated memory estimate. Timing-trial tracing is off.
- **Work and tasks:** logical request count, operation-call count, actual batch sizes, request-worker
  count, sampler/dispatcher counts, created batch-worker tasks, and executor thread limit are retained.
  Internal asyncio futures/callbacks are not counted as tasks.

The console prints the median throughput and the median of each trial's latency percentile.
Those are not pooled percentiles or confidence intervals. Lag percentiles, all repetitions, and
the separate memory pass remain in JSON. Report the spread between repetitions alongside medians;
rerun on an idle machine if environmental noise dominates a comparison.

Smoke tests validate completion, per-item results, batching, finite metrics, report structure,
and cleanup. There is no throughput or latency CI threshold. Correctness and reproducible
measurement belong in CI; noisy performance comparisons need an explicitly controlled run.

The older application measurements remain in [BENCHMARK.md](../BENCHMARK.md#historical-application-benchmarks).
