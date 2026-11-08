# Contributing Guidelines

Use Python 3.11 or newer and [uv 0.13.7 or newer](https://docs.astral.sh/uv/getting-started/installation/).
The core library has no runtime dependencies. Optional adapters and development tools are installed separately.

## Core development

From the repository root, select your interpreter and install the locked test and lint tools into `.venv`:

```bash
uv sync --locked --python 3.12 --group lint
uv run --no-sync python -m pytest tests/core
uv run --no-sync pre-commit run --all-files
```

The `test` dependency group is enabled by default. `uv sync --locked` checks that `uv.lock`
matches `pyproject.toml` and installs the selected dependencies without changing the lockfile.
Use `uv run --no-sync` after syncing to reuse that environment, including any selected adapter
extras. Repeat the appropriate sync command when you change dependencies or switch test suites.

The core tests include cancellation, shutdown, restart, queue limits, and example lifecycle checks.
They do not require database drivers or ML backends. Python 3.11 is the minimum supported version;
CI also checks the current stable Python releases through 3.15.

## Replaying lifecycle traces

`tests/core/test_generated_cost_lifecycle.py` combines cached cost limits, a retained FIFO
head, bounded admission, cancellation before and after acceptance, mixed per-item outcomes,
and graceful/forced stop followed by restart. It checks public snapshots and recorded
processor inputs against an independent outcome model. Event gates control progress;
there are no sleep-based timing or throughput assertions.

Run the fixed seed set with `uv run --no-sync python -m pytest tests/core/test_generated_cost_lifecycle.py`.
A failure includes the seed, task-factory mode, and a JSON trace. Assertion failures also
try a bounded reduction that removes whole stop/restart cycles while preserving the same
failing assertion. Re-run a seed with, for example,
`uv run --no-sync python -m pytest 'tests/core/test_generated_cost_lifecycle.py::test_generated_cost_admission_and_outcome_traces[normal-3]'`.
To replay the emitted JSON directly, save the array as `trace.json` and run:

```bash
uv run --no-sync python - <<'PY'
import asyncio
import json
from pathlib import Path
from tests.core.test_generated_cost_lifecycle import Cycle, replay

cycles = [Cycle(**cycle) for cycle in json.loads(Path("trace.json").read_text(encoding="utf-8"))]
asyncio.run(replay(cycles, eager=False))  # Use the mode reported by the failure.
PY
```

The eager mode requires Python 3.12 or newer. These bounded traces complement the existing
multi-concurrency stress tests; they are regression checks, not a proof of every interleaving.

## Public types

The [public typing guide](docs/typing.md) documents a separate installed-wheel consumer gate:

```bash
uv sync --locked --no-default-groups --group typecheck
uv run --no-sync python .github/scripts/check_public_typing.py
```

It checks strict positive examples, exact negative diagnostics, and a missing-marker control.
CI runs this gate in its quality job. Resync the desired test groups before running runtime tests.

## Adapter tests

Install only the extra and test group needed for the adapter you are changing. Each command
syncs `.venv` to the specified locked groups and extra, removing packages outside that selection.

| Adapter | Install | Run |
| --- | --- | --- |
| Kafka | `uv sync --locked --no-default-groups --group test --extra kafka` | `uv run --no-sync python -m pytest tests/adapters/kafka` |
| AWS | `uv sync --locked --no-default-groups --group test --extra aws` | `uv run --no-sync python -m pytest tests/adapters/aws` |
| SQLAlchemy | `uv sync --locked --no-default-groups --group test --group adapter-test --extra sqlalchemy` | `uv run --no-sync python -m pytest tests/adapters/sqlalchemy` |
| ScyllaDB | `uv sync --locked --no-default-groups --group test --extra scylla` | `uv run --no-sync python -m pytest tests/adapters/scylla` |
| Keras | `uv sync --locked --no-default-groups --group test --group ml-test --extra keras` | `KERAS_BACKEND=jax uv run --no-sync python -m pytest tests/adapters/keras` |
| scikit-learn | `uv sync --locked --no-default-groups --group test --extra sklearn` | `uv run --no-sync python -m pytest tests/adapters/sklearn` |
| MongoDB | `uv sync --locked --no-default-groups --group test --extra mongodb` | `uv run --no-sync python -m pytest tests/adapters/mongodb` |
| Pinecone | `uv sync --locked --no-default-groups --group test --extra pinecone` | `uv run --no-sync python -m pytest tests/adapters/pinecone` |

Pinecone tests use real SDK types and a local HTTP endpoint. They do not need live credentials
and do not verify server-side persistence or eventual consistency.

To install every adapter and run all unit tests:

```bash
uv sync --locked --no-default-groups --group test --group adapter-test --group ml-test --all-extras
KERAS_BACKEND=jax uv run --no-sync python -m pytest
```

`uv run --no-sync python -m pytest` discovers `tests/core` and `tests/adapters`. Missing optional
dependencies produce explicit skips. CI installs each adapter's dependencies before testing it. Integration tests are
outside default discovery, so collecting core tests never imports service fixtures.

Async tests and fixtures use function-scoped event loops by default. Shared SQLAlchemy fixtures
explicitly use a session-scoped loop. Use `loop_scope` when choosing an async test loop.

## Coverage

After installing the desired test environment, measure statement and branch coverage:

```bash
uv run --no-sync python -m coverage run -m pytest
uv run --no-sync python -m coverage report --show-missing
uv run --no-sync python -m coverage html
```

Open `htmlcov/index.html` to inspect uncovered lines and branches. A core-only environment skips
optional adapters, so its whole-package percentage is not a full coverage measurement. Use the
all-adapter environment above for the service-free suite; live integration cases add service behavior
that those tests do not exercise. This local command starts a fresh coverage dataset.
For a complete all-adapter dataset, enforce the CI threshold with
`uv run --no-sync python -m coverage report --fail-under=95`.

CI measures coverage in the existing core, adapter, and live-service jobs, using
`coverage run --parallel-mode -m pytest <suite> -ra`. It uploads uniquely named data artifacts,
checks that every selected matrix job supplied data, and combines them without rerunning the suites.
The aggregate gate requires **95% combined statement-and-branch coverage**. Full adapter runs
measure every `async_batcher` source file. Scoped runs measure the core and selected adapter
directories, including shared ML helpers when relevant; the report identifies that scope.

The combined `coverage-report` artifact contains `html/`, `coverage.xml`, `coverage.json`, and
`summary.md`. Download it to inspect failures; an unselected adapter is not counted as tested by
a scoped pull request. Missing coverage artifacts fail the gate instead of producing a partial report.

## Integration tests

Install the corresponding adapter first, then start its service and select its integration directory:

```bash
docker compose -f docker-compose/dynamodb.yml up -d --wait
uv run --no-sync python .github/scripts/wait_for_service.py aws
uv run --no-sync python -m pytest tests/integration/aws
docker compose -f docker-compose/dynamodb.yml down --volumes
```

For PostgreSQL, use `docker-compose/postgres.yml` and `tests/integration/sqlalchemy` with the
`adapter-test` group and `sqlalchemy` extra. For ScyllaDB, use `docker-compose/scylladb.yml` and
`tests/integration/scylladb` with the `scylla` extra. Stop services after testing; CI collects service
logs on failure and tears services down even if tests fail.

For MongoDB, use `docker-compose/mongodb.yml` and `tests/integration/mongodb` with the `mongodb`
extra. Tests use PyMongo's native async client and verify write results, ordered partial failures,
and validation before writes reach the local service.

For Kafka, use `uv sync --locked --no-default-groups --group test --extra kafka`, `docker-compose/kafka.yml`, and
`tests/integration/kafka`. The development broker is a single-node KRaft server on `127.0.0.1:9092`.
The integration suite verifies native batches, per-record offsets, both timestamp modes, and a
broker-rejected oversized record alongside successful records. Unit tests use the actual aiokafka
producer and batch builder, replacing only the broker enqueue/delivery boundary.

The readiness helper accepts `aws`, `sqlalchemy`, `scylladb`, `mongodb`, or `kafka` and checks
the service protocol before tests start. Run it after `docker compose up` for the corresponding
suite. These integration suites require local Docker services; AWS stream and Pinecone unit
tests use SDK/local HTTP boundaries and do not assert live cloud persistence.

## Documentation and examples

The [README](README.md) introduces the current checkout; the [API guide](docs/api.md) and adapter
guides document its contracts. Clearly distinguish unreleased source behavior from a published
release when adding features. Keep resource ownership, error/partial-success behavior, and
shutdown examples consistent with the implementation.

The application examples have additional dependencies outside `uv.lock`. Follow their README
instructions to create separate example environments. Preserve the repository's isolated test
environments instead of adding example web servers or TensorFlow to the core test group.

## Dependency and CI maintenance

`pyproject.toml` declares the public extras and separate `test`, `adapter-test`, `ml-test`, `lint`,
`typing` (SDK stubs), and `typecheck` (public API checker) dependency groups. `uv.lock` records their resolved versions across supported
interpreters and extras. After changing dependency requirements, refresh and check the lock, then
validate the affected suites and build both distributions:

```bash
uv lock
uv lock --check
uv build
```

`uv lock` preserves compatible locked versions. Use `uv lock --upgrade` for an intentional
refresh, or `uv lock --upgrade-package PACKAGE` for a targeted update. Commit `pyproject.toml`
and `uv.lock` together when requirements change. `uv build` uses the native `uv_build` backend
and writes the wheel and source distribution to `dist/`.

The pre-commit Ruff version and the locked Ruff version must agree. GitHub Actions are pinned to
commit SHAs; keep their version comments current when updating them. Dependabot checks Python
packages and Actions weekly.

Every pull request runs the Linux core gate on Python 3.11 and 3.15, with adapter suites selected
by changed paths. Core implementation, core test, shared core fixture, and infrastructure changes
also run macOS ARM and Intel core checks, plus Windows on Python 3.11 and 3.15. Every main push
retains all supported Linux core interpreters, both macOS architectures, and both Windows endpoints. Workflow or dependency changes, scheduled runs, and manual runs
exercise the full matrix. Superseded runs on the same PR or on `main` are cancelled; separate PRs and
scheduled/manual runs remain independent.

## Locked dependency security audit

The `Locked dependency audit` workflow queries [OSV](https://osv.dev/) weekly,
on demand, and when its inputs or `uv.lock` change. It checks every exact
package version in the lockfile, including optional groups and alternative
versions selected on other Python interpreters. It does not install packages.
A known-vulnerable Keras version is queried as a positive control, never installed.

Run the same check with Python 3.11 or newer:

```bash
python .github/scripts/audit_lock.py
```

The check exits with `0` for a completed audit without unaccepted findings, `1`
for vulnerable locked versions, and `2` when the audit is incomplete. Network
errors, malformed or incomplete responses, failed positive controls, unsupported
package sources, and pagination errors fail the check. The JSON report is saved
as `lock-audit.json` and uploaded by CI even when the audit fails.

There is currently no accepted advisory debt. New findings fail by default.
Prefer updating the affected dependency; if remediation cannot be completed yet,
a reviewed entry in `.github/lock-audit-exceptions.json` must name the exact
package, locked version, OSV advisory ID, reason, and expiry date (`YYYY-MM-DD`).
Exceptions expire at the start of that UTC date. They do not cover other package
versions or newly published advisories. Accepted findings remain visible in the
report, and unused exceptions are reported for removal. Do not generate a blanket
baseline from the scanner output.

OSV coverage is evidence about known advisories, not proof that a dependency has
no vulnerabilities. This gate complements Dependabot and application-specific
security review; it does not audit undeclared example requirements or system images.

## Agent workflows

[AGENTS.md](AGENTS.md) records repository-wide conventions, including signed commits and the
boundary between PR work and tag-triggered publication. The repo-local skills provide focused workflows:

- [async-batcher-navigate](.agents/skills/async-batcher-navigate/SKILL.md) maps a request to code,
  ownership contracts, tests, and documentation.
- [async-batcher-verify](.agents/skills/async-batcher-verify/SKILL.md) selects change-appropriate
  tests, checks installed API/package behavior, and prepares PR validation evidence.
- [async-batcher-release](.agents/skills/async-batcher-release/SKILL.md) checks release history,
  prepares versioned builds and notes, and separates preparation from authorized publication.

In Codex, invoke `$async-batcher-navigate` or `$async-batcher-verify`, or let the task select the
relevant skill. For release preparation, ask `$async-batcher-release` to prepare notes and a
verified publication plan; that does not authorize pushing a tag or posting an announcement.
The skills live in the repository's discoverable `.agents/skills` directory;
see the [official skills guide](https://learn.chatgpt.com/docs/build-skills). They can also be read
as ordinary maintenance documentation. Keep their commands aligned with the workflows above.

## Recipe coverage

CI instruments core, HTTP, and provider recipe runs and includes every selected job's coverage
artifact in the combined report. `coverage-report/recipes/` contains separate JSON, XML, HTML,
and text summaries for all `examples/recipes` Python sources. The summary names the selected
suites; unselected suites can leave modules unexecuted. Execution inside crash/replay child
processes is not instrumented by this report. These application examples have a diagnostic
coverage report, while the package's 95% combined statement-and-branch gate stays unchanged.
A missing selected recipe artifact fails the report rather than silently inflating its percentage.

For a complete local recipe measurement, install the `test` and `example-test` groups
plus the `pinecone` extra in an isolated environment, then run from the repository root:

```bash
uv run --no-sync python -m coverage run --branch --source=examples/recipes -m pytest \
  tests/core tests/examples tests/provider_recipes -ra
uv run --no-sync python -m coverage report --show-missing
```

This intentionally measures recipes separately; it is not a full-package or live-service report.

## Documentation site

The [documentation site](https://hussein.awala.fr/async-batcher/) uses MkDocs Material
and mike, following the versioned documentation setup in
[spark-on-k8s](https://github.com/hussein-awala/spark-on-k8s).
Build the existing Markdown guides without installing optional adapter SDKs:

```bash
uv sync --locked --no-default-groups --python 3.12 --group docs
uv run --no-sync python -m unittest discover -s docs/tests -p 'test_*.py'
uv run --no-sync mkdocs build --strict
uv run --no-sync python .github/scripts/check_docs.py site
uv run --no-sync mkdocs serve
```

`mkdocs serve` starts a local preview; it does not publish. Keep every guide in
`mkdocs.yml` navigation. Relative links to repository files work on GitHub and
are rewritten to the selected source revision in the built site. Local previews
use `main` unless `DOCS_SOURCE_SHA` is set. CI checks documentation on every PR,
including prose-only edits, and saves a `documentation-preview` artifact.

See [documentation publication](docs/documentation.md) for channels, signed archive
commits, initial Pages setup, deployment verification, and recovery. After selecting
a different test suite, resync its dependencies; the docs environment is independent
of the runtime test environment.
