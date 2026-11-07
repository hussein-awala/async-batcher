# Open-loop SQLite experiment

This harness measures real SQLite transactions with arrivals scheduled independently of
completion. It complements the [synthetic closed-loop benchmark](README.md). It does not
claim a universal speedup, database capacity limit, or production tail-latency guarantee.

## Run

Only the core checkout and Python standard library are required:

```bash
uv sync --locked --no-default-groups --python 3.12
uv run --no-sync python -m unittest discover -s benchmarks -p 'test_*.py'
uv run --no-sync python -m benchmarks.sqlite_open_loop \
  --duration 3 --repetitions 3 --outstanding 128 --warmup 32 \
  --batch-size 32 --queue-time 0.002 --payload-bytes 256 \
  --sparse-rate 25 --burst-rate 500 --burst-size 32 --saturated-rate 5000 \
  --keys 4 --hot-fraction 0.8 --grouped --seed 42 \
  --output /tmp/sqlite-open-loop.json.gz
```

The default example takes at least 108 seconds with `--grouped`: four implementations ×
three arrival profiles × three repetitions × three seconds, plus warmup, drain, and setup.
Use an otherwise idle machine and record power mode and storage. `--storage DIRECTORY`
selects the parent for temporary database files; the directory must already exist. Each
trial creates a fresh database and removes it after verifying its contents. Store the
report outside the checkout so it does not affect the recorded dirty-tree flag.

The rates are explicit experiment inputs, not automatically detected capacity. The profile
named `saturated` is only saturated if the measurements show overload at the chosen rate.
A short disclosed pilot can select representative rates before freezing them for all modes
and repetitions. Keep its results separate from the measured repetitions; do not retune
rates per implementation. A `.gz` output suffix writes compressed JSON without dropping
raw observations. Read it with `gzip.open(path, "rt", encoding="utf-8")` and `json.load`.

A quick smoke run can use `--duration 0.05 --repetitions 1 --warmup 4 --outstanding 8
--batch-size 4 --sparse-rate 100 --burst-rate 200 --saturated-rate 1000`.

## Shared work and baselines

All modes use the same SQLite table, fixed-size byte payload, `executemany` insert function,
and one connection owned by one executor thread. SQLite uses WAL and `synchronous=FULL`;
the report records the actual journal, synchronous, `fullfsync`, page-size, and automatic
checkpoint settings. SQLite version, filesystem, OS caches, checkpoints, and storage can
substantially change results. This does not test power-loss durability or concurrent database
clients. See SQLite's [synchronous settings](https://www.sqlite.org/pragma.html#pragma_synchronous)
and [WAL behavior](https://www.sqlite.org/wal.html).

- **Direct:** one transaction per accepted row, with the shared backend semaphore limiting
  execution to one transaction at a time.
- **Application:** a small queue/count/deadline batcher included in the harness. It uses the
  same maximum batch size and oldest-item collection deadline as the library case. It is a
  deliberately limited comparison, without the library's full lifecycle or API contracts.
- **Library:** `AsyncBatcher.from_callable` around the same transaction function, with one
  concurrent batch and the configured count/deadline limits.
- **Grouped (optional):** one library child per key, with the same per-child limits and a
  shared one-transaction backend semaphore. This is a separate grouping/skew experiment;
  keys do not add database execution parallelism. The other modes can mix keys in a batch.

All modes share one admission boundary: at most `outstanding` accepted request tasks may
remain unfinished. An arrival finding that boundary full is recorded as rejected immediately;
it is never silently delayed or retried. These are **harness admission rejections**, not
`QueueFullException` or a claim about each library queue's capacity. The library queue limit
is also `outstanding`, so it does not reject an already admitted harness request. Grouped
children share the same global limit; `max_groups` equals the configured key count.

Warmup requests use negative IDs and the same connection/implementation as the measured
phase, with at most `warmup <= outstanding` requests. They remain in the database to preserve
its warmed state but are excluded from measured transaction/request counts. Every measured
accepted ID, key, and payload is verified in SQLite before a report is returned. Database
errors, wrong result IDs, missing rows, and incomplete requests fail the experiment rather
than becoming apparently good performance results.

## Arrival process and measurements

Sparse and saturated profiles use regular intervals. Bursts place `burst_size` arrivals at
the same scheduled instant, then wait `burst_size / rate` seconds; a final burst may be partial.
Offered counts are bounded at 250,000 per trial. One seeded key sequence is shared across
implementations: key 0 gets `hot_fraction`, and remaining keys split the rest randomly.

The clock schedule never moves because requests are slow. If the event loop falls behind,
the generator catches up and retains each original timestamp; it yields every 64 overdue
arrivals so generation itself cannot monopolize the loop indefinitely. Launch lateness is
recorded for **all** arrivals, including rejected ones. Large lateness means the local load
generator is a bottleneck, so the intended offered rate was not delivered on time. This is
an in-process load generator, not an external network client.

The JSON includes every request's scheduled and launched time, key and admission outcome;
completed requests also include backend start and caller completion times. It retains every
transaction's size/start/end, loop-lag samples, shuffled case order, configuration, commit,
dirty flag, Python/platform/dependency versions, SQLite settings, and UTC run times.

- **End-to-end latency:** caller completion minus the original scheduled arrival. It includes
  launch lateness, queue/batch formation, backend waiting, transaction time, and result routing.
- **Queue residence:** backend start minus actual launch. It includes batch collection and
  waiting for the common backend semaphore/executor. It is not just the library input queue.
- **Completed/s:** completions divided by arrival-window plus drain elapsed time. Also report
  completions within the arrival window, offered rate, rejections, and drain time; throughput
  alone can hide overload or delayed results.
- **Per-key tails:** offered/completed/rejected counts and latency/queue p50/p95/p99 for every
  observed key. Low sample counts make cold-key tails unstable; report counts and do not
  interpret one rare request as a well-measured p99.
- **Event-loop lag:** the same 5 ms sampler as the synthetic harness. Empty samples remain
  `null`, never zero. Request and transaction recording adds common harness overhead.

Percentiles use linear interpolation within each trial. The console prints medians across
trial summaries, not pooled percentiles or confidence intervals. Preserve raw repetitions
and spread, including losing cases: batching can add collection delay at sparse arrival rates,
and grouping can fragment otherwise compatible batches. There are no timing/performance CI
thresholds. Standard-library smoke tests check schedule/metric arithmetic, common admission,
real persisted rows, all modes, and cleanup on failure or cancellation.
