# Open-loop SQLite measurements on Apple M2 Max

Controlled local measurements of one SQLite writer, four implementations, and three offered-arrival profiles. These results describe this checkout, machine, workload, and admission limit; they are not general capacity or production-latency guarantees.

## Recorded experiment

- Source commit: `37126439f42eee5e14fc16df74c54b54ac992e97`; working tree clean.
- Run: 2026-11-07T21:50:21.472548+00:00 to 2026-11-07T21:52:10.926092+00:00.
- Environment: 3.11.18 (main, Oct 25 2026, 19:41:29) [Clang 21.0.0 (clang-2100.3.34.2)], SQLite 3.54.1; macOS-27.2-arm64-arm-64bit.
- Hardware/power: Apple M2 Max, 12 logical CPUs, 64 GiB RAM; AC power, battery full, low-power mode disabled. Other agents paused local tests and measurements. Normal desktop background work remained.
- Raw observations: [2026-11-07-sqlite-macos.json.gz](2026-11-07-sqlite-macos.json.gz) (14,591,688 compressed bytes); SHA-256 `25e1b0c2efbfa8f19b10fb33eccca946a2899e419294efbdae62e358a6dbd5a2`.
- SQLite settings: WAL, synchronous=FULL (2), fullfsync=0, 4,096-byte pages, 1,000-page automatic checkpoint threshold. Storage was the system temporary directory recorded in JSON; this is not a power-loss test.
- Only the editable core package was installed; the raw environment records no optional dependencies.
- See [the harness guide](../SQLITE.md) for exact baseline, warmup, admission, timing, and cleanup definitions.

```bash
uv sync --locked --no-default-groups --python 3.11.18
uv run --no-sync python -m benchmarks.sqlite_open_loop \
  --duration 3 --repetitions 3 --outstanding 128 --warmup 32 \
  --batch-size 32 --queue-time 0.002 --payload-bytes 256 \
  --sparse-rate 25 --burst-rate 1000 --burst-size 32 --saturated-rate 12000 \
  --keys 4 --hot-fraction 0.8 --grouped --seed 42 \
  --output /tmp/sqlite-open-loop.json.gz
```

The offered rates were frozen before these repetitions. A [separate one-second pilot](2026-11-07-sqlite-pilot.json.gz) used older clean commit `26d1fff8c7b794d032a98bdc29acc04c70adc287`. At 20,000 offered requests/s it showed roughly 8,000/s direct completion and 8–11 ms p99 generator lateness. The final 12,000/s rate was selected to overload the direct writer while reducing catch-up pressure. The pilot is not pooled into these results. Every mode receives the same timestamp/key sequence; case order is shuffled per repetition.

## Completed work and rejection

Values are medians of three trials; completed/s includes drain. Its range is shown in brackets. Rejections come from the identical 128-outstanding harness boundary. They are neither backend failures nor direct measurements of the library queue capacity.

| Profile | Mode | Completed/s [min–max] | Rejected % [min–max] | p95 latency ms | p95 queue ms | p99 launch lateness ms |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| sparse | direct | 25.0 [25.0–25.0] | 0.00 [0.00–0.00] | 1.374 | 0.224 | 0.733 |
| sparse | application | 25.0 [25.0–25.0] | 0.00 [0.00–0.00] | 3.920 | 2.658 | 0.687 |
| sparse | library | 25.0 [25.0–25.0] | 0.00 [0.00–0.00] | 4.254 | 2.813 | 0.704 |
| sparse | grouped | 25.0 [25.0–25.0] | 0.00 [0.00–0.00] | 4.282 | 2.881 | 0.720 |
| burst | direct | 999.9 [999.7–999.9] | 0.00 [0.00–0.00] | 8.463 | 7.452 | 1.149 |
| burst | application | 999.8 [999.6–999.9] | 0.00 [0.00–0.00] | 2.326 | 0.675 | 1.054 |
| burst | library | 999.8 [999.7–999.9] | 0.00 [0.00–0.00] | 3.337 | 1.357 | 1.249 |
| burst | grouped | 999.7 [999.7–999.8] | 0.00 [0.00–0.00] | 7.638 | 5.934 | 1.375 |
| saturated | direct | 9,090.5 [8,532.9–9,301.6] | 23.97 [22.17–28.60] | 17.510 | 17.374 | 0.169 |
| saturated | application | 11,992.3 [11,629.9–11,997.6] | 0.00 [0.00–1.37] | 2.275 | 2.049 | 0.088 |
| saturated | library | 11,970.9 [11,965.6–11,992.3] | 0.16 [0.00–0.23] | 2.309 | 2.065 | 0.123 |
| saturated | grouped | 11,978.0 [11,834.3–11,993.3] | 0.16 [0.00–1.32] | 2.537 | 2.204 | 0.180 |

## Latency and scheduling spread

End-to-end latency starts at the scheduled arrival, so late generation is included. These are medians of per-trial percentiles, not pooled percentiles or confidence intervals. Empty lag samples would remain absent.

| Profile | Mode | p50 / p95 / p99 latency ms | p95 latency range ms | p99 loop lag ms | Drain ms |
| --- | --- | ---: | ---: | ---: | ---: |
| sparse | direct | 0.764 / 1.374 / 1.763 | 1.283–1.421 | 0.878 | 0.604 |
| sparse | application | 3.447 / 3.920 / 4.016 | 3.904–4.008 | 0.851 | 0.427 |
| sparse | library | 3.422 / 4.254 / 5.650 | 4.015–4.565 | 0.820 | 0.446 |
| sparse | grouped | 3.663 / 4.282 / 4.618 | 4.065–4.474 | 0.833 | 0.610 |
| burst | direct | 3.827 / 8.463 / 9.477 | 5.441–8.594 | 0.915 | 0.349 |
| burst | application | 1.039 / 2.326 / 4.871 | 1.467–3.784 | 0.907 | 0.468 |
| burst | library | 1.358 / 3.337 / 6.107 | 3.082–18.436 | 1.241 | 0.689 |
| burst | grouped | 5.513 / 7.638 / 13.962 | 7.248–7.992 | 1.481 | 0.750 |
| saturated | direct | 13.168 / 17.510 / 35.189 | 15.673–21.970 | 0.235 | 12.351 |
| saturated | application | 1.342 / 2.275 / 3.230 | 2.275–2.284 | 0.105 | 1.915 |
| saturated | library | 1.381 / 2.309 / 3.049 | 2.280–2.317 | 0.122 | 1.914 |
| saturated | grouped | 1.633 / 2.537 / 3.525 | 2.526–5.406 | 0.294 | 1.669 |

## Grouped skew and per-key tails

Key 0 has an 80% assignment probability; the other keys share the remainder. Global library batching can mix keys; grouped batching cannot. Both share one SQLite transaction semaphore and one executor thread. Counts below are median completed samples per trial. Sparse cold-key p99 values have very few samples and should not be treated as stable tail estimates.

| Profile | Key | Global samples | Grouped samples | Global p99 ms | Grouped p99 ms | Global rejected % | Grouped rejected % |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| sparse | 0 | 64 | 64 | 5.769 | 4.698 | 0.00 | 0.00 |
| sparse | 1 | 4 | 4 | 4.637 | 4.034 | 0.00 | 0.00 |
| sparse | 2 | 3 | 3 | 3.769 | 3.934 | 0.00 | 0.00 |
| sparse | 3 | 4 | 4 | 3.733 | 4.065 | 0.00 | 0.00 |
| burst | 0 | 2419 | 2419 | 6.111 | 12.945 | 0.00 | 0.00 |
| burst | 1 | 191 | 191 | 3.976 | 8.495 | 0.00 | 0.00 |
| burst | 2 | 205 | 205 | 5.488 | 14.148 | 0.00 | 0.00 |
| burst | 3 | 185 | 185 | 5.625 | 8.888 | 0.00 | 0.00 |
| saturated | 0 | 28768 | 28768 | 3.167 | 3.435 | 0.17 | 0.17 |
| saturated | 1 | 2359 | 2361 | 2.904 | 5.891 | 0.17 | 0.08 |
| saturated | 2 | 2431 | 2430 | 3.010 | 3.650 | 0.16 | 0.21 |
| saturated | 3 | 2383 | 2383 | 2.589 | 4.084 | 0.08 | 0.08 |

## Interpretation

At 12,000 scheduled arrivals/s, the direct writer rejected a median 23.97% and completed
9,090.5 requests/s including drain. The library completed 11,970.9/s with 0.16% median
rejection. Transactions explain the difference in this workload: the saturated library
trials used 1,433–1,435 transactions for roughly 36,000 accepted rows, while direct writes
committed each accepted row separately.

Sparse arrivals show the opposite latency tradeoff. Every mode sustained the offered 25/s
without rejection, but direct p95 latency was 1.374 ms versus 4.254 ms for the library with
a 2 ms collection budget. The small application batcher also had lower median p95 latency
than the library in sparse and burst trials. These measurements do not establish a speed
advantage for the general library over that simpler batcher.

Grouping fragmented compatible work: each 3,000-request burst trial used 346 grouped
transactions versus 94 global library transactions. Grouped p95 latency was 7.638 ms versus
3.337 ms globally, and the per-key table preserves the cold-key tail costs. No additional
SQLite execution parallelism was granted to grouped children.

Variation matters. One global-library burst trial had p95 latency 18.436 ms, despite the
3.337 ms median. One saturated application trial rejected 492 requests and had maximum
generator lateness 76.474 ms and a 53.168 ms drain; its other two trials rejected none.
Saturated library rejection counts were 0, 84, and 59; grouped counts were 58, 0, and 477.
Those small rejection differences cannot be attributed to batching implementation alone:
this in-process generator, Python allocation/collection, and desktop scheduling also
contribute. The p99 lateness values in the table do not erase rarer maximum delays in the raw data.

The 36 trials offered 468,900 measured requests: 440,826 completed and 28,074 were rejected
at the common admission boundary. Another 1,152 requests warmed the implementations and
are excluded from those totals. All trials are retained, including the slow ones.

All accepted IDs, group keys, and byte payloads were checked against the actual database after each trial. No trial failed or was dropped. Warmup, setup, shutdown, verification, and JSON compression are outside measured elapsed time. Automatic checkpoints remain enabled; measured commits include any checkpoints they trigger. The run does not model multiple database connections, network delay, process crashes, or independent external load generation.
