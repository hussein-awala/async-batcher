# Change-to-test matrix

Commands are repository-relative and use the current `uv.lock`. Choose the rows the diff affects.
[CONTRIBUTING.md](../../../../CONTRIBUTING.md) owns the full adapter install table and service setup;
`.github/workflows/ci.yml` and `.github/scripts/ci_plan.py` own the CI matrix. Read those files if
this reference and the current checkout differ. The current endpoints are Python 3.11 and 3.15.

| Change | Evidence before PR creation | Additional hosted / merge evidence |
| --- | --- | --- |
| Core admission, dispatch, lifecycle, cost, cancellation, context, stats | Focused regression, full `tests/core`; both interpreter endpoints for interpreter-sensitive changes | Core checks on Windows and both macOS architectures; affected adapters/recipes for shared contracts |
| Map, grouped batching, coalescing | Matching stream/ownership regressions and full core suite; cover early exit, source/item failure, cancellation and bounded work as applicable | Core matrix and installed public typing when signatures change |
| Public constructors, annotations, exports, `py.typed` | Core typing gate; AWS/Kafka installed consumers when shared options or those adapters change | Corresponding installed SDK gates and distribution probes |
| Adapter request/response/retry behavior | Install its extra, preflight imports, run its adapter directory; run relevant core tests if options/lifecycle changed | Local-service integration suite when one exists and backend behavior is affected |
| Standard-library recipes | Matching `tests/core/test_*recipe*` or specific consumer/stream tests, plus affected core tests | Core platform checks, especially file/SQLite/process behavior |
| FastAPI/HTTP recipes | `tests/examples` plus the relevant core recipe tests | HTTP jobs at both interpreter endpoints |
| Cohere/Pinecone pipeline | `tests/provider_recipes`; Pinecone adapter tests if shared adapter behavior changes | Both provider jobs; local HTTP tests do not prove paid-provider availability |
| Legacy Keras/gRPC application examples | Relevant Keras adapter example tests and core lifecycle/gRPC tests | Keras endpoint jobs; follow example README for a standalone application environment |
| Package/dependency/build changes | Lock check, affected suites, distribution checker; intentional lock audit after dependency changes | Full matrix and applicable locked-dependency audit |
| Workflow/filter/helper changes | CI helper unit tests, actionlint, representative selection cases | Verify changed paths select intended suites; a `.github` change selects the full matrix |
| Benchmark harness | Benchmark unit tests and a small correctness smoke run | No speed threshold; published comparisons need a controlled run with raw data/environment |
| Pure documentation or agent instructions | Strict MkDocs build and rendered link check from `docs/documentation.md`; API/command references and runnable snippets if changed; skill validation for instruction changes | Documentation build on every PR; no new runtime tests for prose |

## Core and quality

```bash
uv sync --locked --python 3.11 --group lint
uv run --no-sync python -m pytest tests/core -ra
uv run --no-sync pre-commit run --all-files --show-diff-on-failure
```

Repeat the core run with the other endpoint when needed, using a separate environment or a
sequential resync. The core suite needs no optional SDKs. Generated lifecycle tests can print a
seed and reduced replay trace; use the replay instructions in `CONTRIBUTING.md` instead of hiding
a timing failure with repeated reruns. Check warnings and expected interpreter-specific skips.

## Adapter and installed-type contracts

Use the appropriate install command in `CONTRIBUTING.md`. For example, Kafka with its typing gate:

```bash
uv sync --locked --python 3.11 --no-default-groups --group test --group typecheck --extra kafka
uv run --no-sync python .github/scripts/check_adapter.py kafka
uv run --no-sync python -m pytest tests/adapters/kafka -ra
uv run --no-sync python .github/scripts/check_public_typing.py --adapter kafka
uv run --no-sync python .github/scripts/check_distributions.py --adapter kafka
```

AWS uses `aws` in the same commands. The installed typing helper exports locked SDK dependencies
and runs positive, negative, and missing-marker controls in isolation. An ordinary mypy run on the
checkout does not replace it. Other adapters use the import/distribution probe but have no separate
SDK typing gate. Run the adapter distribution probe last, or resync after it.

For the core typing/packaging gates:

```bash
uv sync --locked --no-default-groups --group typecheck
uv run --no-sync python .github/scripts/check_public_typing.py
uv lock --check
uv run --no-sync python .github/scripts/check_distributions.py
```

Without `--adapter`, the distribution checker builds wheel and sdist, rebuilds the wheel from
the sdist, checks metadata/source/typing marker, and probes installed imports outside the checkout.
With `--adapter`, it builds/probes the direct wheel in the selected SDK environment. It requires uv
on PATH. A successful editable import alone is insufficient packaging evidence.

## HTTP and provider recipes

```bash
uv sync --locked --python 3.11 --no-default-groups --group test --group example-test
uv run --no-sync python -c 'import fastapi, httpx2, uvicorn; from fastapi.testclient import TestClient'
uv run --no-sync python -m pytest tests/examples -ra
```

For the provider pipeline, add the Pinecone extra; these tests use local HTTP servers and dummy credentials:

```bash
uv sync --locked --python 3.11 --no-default-groups --group test --group example-test --extra pinecone
uv run --no-sync python -c 'import httpx2; from pinecone import AsyncIndex, Vector'
uv run --no-sync python -m pytest tests/provider_recipes -ra
```

These directories are absent from default pytest discovery. Repeat at the other endpoint when
changing client/interpreter compatibility. No Cohere SDK is needed: the recipe uses locked HTTPX2.

## Live services

Install the adapter first. Map AWS to `dynamodb.yml` / `tests/integration/aws`, SQLAlchemy to
`postgres.yml` / `tests/integration/sqlalchemy`, Scylla to `scylladb.yml` / `tests/integration/scylladb`,
and MongoDB/Kafka to their matching YAML files and test directories. SQLAlchemy needs `adapter-test`.
The readiness helper takes `aws`, `sqlalchemy`, `scylladb`, `mongodb`, or `kafka`.

Use a dedicated Compose project for the run so cleanup cannot remove someone else's services.
The development configurations expose fixed ports: avoid concurrent instances with the same ports.
Capture diagnostics and tear down this run's containers/volumes even if tests fail. Do not tear down
pre-existing user services. If Docker is unavailable, record that limitation and inspect the hosted
service job; do not label unit mocks as a live integration run.

## CI, dependencies, and benchmarks

```bash
uv sync --locked --no-default-groups --group test --group lint --group typecheck
uv run --no-sync python -m unittest discover -s .github/scripts -p 'test_*.py'
uv run --no-sync python -m unittest discover -s benchmarks -p 'test_*.py'
uv lock --check
```

On Linux amd64, `bash .github/scripts/check-workflows.sh` fetches and verifies the pinned actionlint
binary. On macOS/Windows use that actionlint version's native binary (and ShellCheck where available)
or rely on the hosted Linux job; the helper downloads a Linux binary and is not portable.

For dependency changes, choose `uv lock`, targeted `uv lock --upgrade-package PACKAGE`, or
`uv lock --upgrade` according to scope; commit requirements and lock changes together. Run
`python .github/scripts/audit_lock.py` with network access. An incomplete audit is not success.
Do not refresh dependencies merely to run an unrelated change's tests.

For a small SQLite benchmark correctness exercise (not published performance evidence):

```bash
benchmark_results=$(mktemp -d)
uv run --no-sync python -m benchmarks.sqlite_open_loop \
  --duration 0.05 --repetitions 1 --warmup 4 --outstanding 8 \
  --batch-size 4 --sparse-rate 100 --burst-rate 200 --saturated-rate 1000 \
  --output "$benchmark_results/smoke.json.gz"
```

This uses the smoke parameters from `benchmarks/SQLITE.md`; the harness verifies persisted data. For controlled results follow that guide, keep output outside the checkout, and
record the exact clean source, environment, all repetitions, and losing cases.
