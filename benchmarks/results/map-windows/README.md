# Pending-window scan experiment

This local experiment isolates ordered `map()` overhead with a cheap asynchronous bulk function.
It is not an application throughput forecast. Every trial submits 16,384 integers, verifies each
routed result, uses batch size 64 and concurrency 4, and produces exactly 256 batches. There is
one warmup of 1,024 items before each measured case. Three repetitions shuffle window order with
seed 17; no test or other benchmark process ran concurrently in the coordinated local window.

| Pending window | Baseline median (seconds) | Completion-driven median (seconds) |
| --- | ---: | ---: |
| 128 | 0.1636 | 0.1255 |
| 1,024 | 0.4598 | 0.1423 |
| 8,192 | 1.9483 | 0.1767 |

The baseline scanned every pending task before every yielded result. The candidate records the
first failure as the request finishes and checks that single value during iteration. These timings
justify removing the scan for large windows; they do not promise an 11x improvement for a real
service, where backend I/O or computation can dominate. There is no timing assertion in CI.

The JSON files retain every raw trial, case order, Python/platform details and SHA-256 hashes of
both runtime files. The baseline used a clean checkout at `f553ea7685d092d662368810750574bae54648c9`;
the candidate was an uncommitted runtime patch on that checkout, recorded as dirty with its own
source hashes. Subsequent test/benchmark additions do not change those measured runtime files.
Power mode and thermal state were not controlled, so the three short local trials are evidence of
the algorithmic scaling issue, not a precision comparison between machines or releases.

Reproduce on either revision (run from its checkout; the benchmark helper can also be copied to an
older checkout):

```bash
uv run --no-sync python -m benchmarks.map_windows \
  --repo . --output /tmp/map-windows.json --label your-revision \
  --requests 16384 --windows 128 1024 8192 --repetitions 3 --seed 17
```

Each trial has a 60-second bound, cancels/drains its batcher on failure, and saves results only after
all requested trials finish successfully. No optional backend or service is needed.
