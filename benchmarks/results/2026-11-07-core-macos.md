# Core benchmark: 7 November 2026

This is a controlled **synthetic bulk-operation comparison**, not an application capacity claim.
Batching amortizes a deliberately modeled fixed call cost. Larger batches performed more work
per call in this run; batch size 1 exposed overhead without that advantage. Real services have
different cost curves and limits.

The [complete JSON report](2026-11-07-core-macos.json) retains every trial's metrics, configuration,
case order and environment. The [harness methodology](../README.md) defines the measurements.
These results replace no historical application measurements.

## Environment and reproduction

- Clean signed checkout: [`9d80bc646c22834c00d1aea6d46eb0178f3a69f6`](https://github.com/hussein-awala/async-batcher/commit/9d80bc646c22834c00d1aea6d46eb0178f3a69f6).
- CPython 3.12.16, macOS 27.2 arm64, Apple M2 Max, 12 logical/physical CPUs, 64 GiB RAM.
- AC power; other agents' local test workloads finished or paused. OS background work was not
  disabled, and CPU affinity and power mode were not fixed. This is one machine, one session.
- The isolated environment contained only the editable `async-batcher` 0.0.1 distribution.
  The Git commit identifies the code; that development package version alone does not.
- Run: 17:28:03–17:29:13 UTC on 7 November 2026, about 70.5 seconds including warmup and memory passes.

From that checkout, using uv 0.13.7:

```bash
uv sync --locked --no-default-groups --python 3.12
uv run --no-sync python -m benchmarks.core \
  --requests 2048 --warmup 256 --repetitions 3 --outstanding 128 \
  --batch-sizes 1 8 32 --concurrencies 1 4 \
  --fixed-delay 0.001 --per-item-delay 0.00002 --queue-time 0.001 \
  --lag-interval 0.001 --seed 42 \
  --output /tmp/async-batcher-core-2026-11-07.json
```

Each operation waits for `1 ms + 0.02 ms × item count`, then doubles its integer inputs. The direct
baseline calls that same operation with one item and the same call-concurrency limit. Async mode
uses `asyncio.sleep`; thread mode uses `time.sleep` in a pool whose size equals call concurrency.
The thread mode models blocking I/O, not CPU-bound Python. The sampler interval is 1 ms in this
run, overriding the harness's 5 ms default. The per-trial timeout is the default 30 seconds.

Every timing and memory trial has 256 warmup requests followed by 2,048 measured requests.
Three timing trials and one separate memory trial ran per case, in deterministically shuffled
order. In total: 98,304 timed requests, 32,768 memory-pass requests, and 16,384 warmup requests.
All 147,456 results matched the expected doubled input; no trial failed or timed out.

## Throughput and request latency

Throughput is the median of three trials, with the observed minimum–maximum in parentheses.
The ratio compares medians with the direct baseline at the **same workload and concurrency**.
Each latency cell is the median of the corresponding per-trial percentile, not a pooled
percentile. Latency includes waiting behind other requests in the closed-loop workload.

### Async sleep

| Concurrency | Batch | Requests/s, median (min–max) | Ratio | p50 ms | p95 ms | p99 ms |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | Direct | 843.0 (840.3–854.9) | 1.00× | 151.228 | 155.217 | 156.310 |
| 1 | 1 | 801.5 (800.9–838.3) | 0.95× | 159.136 | 167.033 | 167.791 |
| 1 | 8 | 5,322.8 (5,202.8–5,325.8) | 6.31× | 23.499 | 26.227 | 26.791 |
| 1 | 32 | 14,899.4 (14,443.7–15,488.7) | 17.67× | 8.532 | 9.209 | 9.350 |
| 4 | Direct | 3,434.5 (3,414.8–3,460.1) | 1.00× | 36.944 | 38.194 | 38.629 |
| 4 | 1 | 3,203.9 (3,189.9–3,299.1) | 0.93× | 38.232 | 44.245 | 44.741 |
| 4 | 8 | 19,089.0 (18,587.8–19,647.1) | 5.56× | 6.676 | 7.297 | 7.570 |
| 4 | 32 | 48,803.3 (39,930.6–57,772.5) | 14.21× | 2.494 | 3.360 | 3.684 |

### Thread sleep

| Concurrency | Batch | Requests/s, median (min–max) | Ratio | p50 ms | p95 ms | p99 ms |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | Direct | 700.0 (694.8–705.9) | 1.00× | 181.366 | 190.160 | 192.725 |
| 1 | 1 | 703.7 (633.2–724.6) | 1.01× | 179.989 | 190.107 | 194.385 |
| 1 | 8 | 4,362.0 (4,159.9–4,488.8) | 6.23× | 29.553 | 32.142 | 32.524 |
| 1 | 32 | 13,088.7 (11,890.8–14,066.8) | 18.70× | 9.510 | 11.314 | 11.399 |
| 4 | Direct | 2,690.0 (2,673.7–2,751.3) | 1.00× | 46.513 | 51.019 | 52.520 |
| 4 | 1 | 2,560.6 (2,374.4–2,765.4) | 0.95× | 48.826 | 56.918 | 58.926 |
| 4 | 8 | 16,687.8 (16,646.0–16,801.8) | 6.20× | 7.381 | 8.778 | 9.245 |
| 4 | 32 | 41,056.3 (40,228.0–46,777.5) | 15.26× | 2.900 | 3.924 | 4.135 |

Batch size 1 ranged from 0.93× to 1.01× direct median throughput. The near-equality of the
single-thread result does not establish a speedup: its trial range overlaps the direct baseline,
and thread scheduling varied. Batch sizes 8 and 32 amortize the modeled fixed delay; this result
should not be transferred to an operation that has no efficient native bulk primitive.

The fastest case, async batch 32 at concurrency 4, varied from 39,931 to 57,772 requests/s. Its
trials lasted only about 35–51 ms. That spread and the short runs limit precision; the table
reports it rather than presenting one best run. Longer trials and repeated sessions are needed
before using small differences for a tuning decision. No confidence interval is claimed.

## Event-loop lag and traced memory

Lag values below are medians of the three timing-trial percentiles. The sample-count range is
included because faster, shorter cases collect fewer samples. In particular, p99 lag from only
31–39 samples has weak tail resolution. It is a descriptive measurement, not a tail-latency SLA.
The complete JSON also records every trial's maximum lag and percentile values.

Memory is one separate pass's peak **incremental Python allocations**, in KiB (1,024 bytes).
It excludes initial operation/batcher construction and native allocations, and includes harness
request tasks and latency/lag lists. Longer trials collect more lag samples. These numbers are
not total batcher memory, process RSS, or repeated memory estimates.

| Workload | Concurrency | Batch | Lag samples, min–max | p95 lag ms | p99 lag ms | Peak traced KiB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| async | 1 | Direct | 2051–2051 | 0.232 | 0.325 | 331.0 |
| async | 1 | 1 | 2144–2256 | 0.196 | 0.260 | 365.9 |
| async | 1 | 8 | 335–342 | 0.253 | 0.414 | 302.8 |
| async | 1 | 32 | 116–123 | 0.262 | 0.421 | 300.4 |
| async | 4 | Direct | 512–518 | 0.185 | 0.276 | 288.4 |
| async | 4 | 1 | 560–573 | 0.211 | 0.347 | 316.5 |
| async | 4 | 8 | 91–95 | 0.302 | 0.502 | 303.8 |
| async | 4 | 32 | 31–32 | 0.490 | 1.281 | 306.2 |
| thread | 1 | Direct | 2554–2579 | 0.226 | 0.308 | 358.5 |
| thread | 1 | 1 | 2564–2823 | 0.174 | 0.232 | 392.9 |
| thread | 1 | 8 | 400–417 | 0.279 | 0.401 | 312.1 |
| thread | 1 | 32 | 128–143 | 0.298 | 0.471 | 306.1 |
| thread | 4 | Direct | 670–681 | 0.256 | 0.397 | 303.1 |
| thread | 4 | 1 | 673–751 | 0.237 | 0.403 | 336.7 |
| thread | 4 | 8 | 106–107 | 0.311 | 0.501 | 315.9 |
| thread | 4 | 32 | 36–39 | 0.864 | 0.985 | 316.1 |

## Bounded work and interpretation

Each measured trial created 128 request-worker tasks and one sampler. Each batched trial also
started one dispatcher. Direct and batch-size-1 cases executed 2,048 operations; batch sizes 8
and 32 executed 256 and 64 operations respectively. Every measured batch reached its configured
size in this saturated setup. Batched worker-task creation matched those operation counts.
Async cases used no executor; thread pools had exactly the selected limit of one or four threads.
Internal futures and callbacks are not counted as tasks.

The outstanding-request cap stayed at 128. Workers submitted a new request only after their
previous one completed, so these percentiles do not describe an independent arrival stream or
an overloaded service. The configured 1 ms collection delay is not an end-to-end latency bound.
This run does not measure low-arrival partial batches, external adapters, failures/retries,
CPU-bound models, sustained thermal behavior, or a deployment's useful concurrency limit.

The harness and smoke tests can reproduce the procedure. Performance numbers belong to the
recorded workload and environment; CI checks correctness without a throughput threshold.
