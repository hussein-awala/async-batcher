# Contributing Guidelines

Use Python 3.11 or newer and Poetry 2.5.1. The core library has no runtime dependencies.
Optional adapters and development tools are installed separately.

## Core development

From the repository root, select your interpreter and install the locked test and lint tools:

```bash
poetry env use python3.12
poetry sync --only main,test,lint
poetry run pytest tests/core
poetry run pre-commit run --all-files
```

The core tests include cancellation, shutdown, restart, queue limits, and example lifecycle checks.
They do not require database drivers or ML backends. Python 3.11 is the minimum supported version;
CI also checks the current stable Python releases through 3.15.

## Adapter tests

Install only the extra and test group needed for the adapter you are changing. Each command
replaces the current environment's selected dependencies with the specified locked groups.

| Adapter | Install | Run |
| --- | --- | --- |
| AWS | `poetry sync --only main,test -E aws` | `poetry run pytest tests/adapters/aws` |
| SQLAlchemy | `poetry sync --only main,test,adapter-test -E sqlalchemy` | `poetry run pytest tests/adapters/sqlalchemy` |
| ScyllaDB | `poetry sync --only main,test -E scylla` | `poetry run pytest tests/adapters/scylla` |
| Keras | `poetry sync --only main,test,ml-test -E keras` | `KERAS_BACKEND=jax poetry run pytest tests/adapters/keras` |
| scikit-learn | `poetry sync --only main,test -E sklearn` | `poetry run pytest tests/adapters/sklearn` |

To install every adapter and run all unit tests:

```bash
poetry sync --only main,test,adapter-test,ml-test --all-extras
KERAS_BACKEND=jax poetry run pytest
```

Plain `pytest` discovers `tests/core` and `tests/adapters`. Missing optional dependencies produce
explicit skips. CI installs each adapter's dependencies before testing it. Integration tests are
outside default discovery, so collecting core tests never imports service fixtures.

Async tests and fixtures use function-scoped event loops by default. Shared SQLAlchemy fixtures
explicitly use a session-scoped loop. Use `loop_scope` when choosing an async test loop.

## Integration tests

Install the corresponding adapter first, then start its service and select its integration directory:

```bash
docker compose -f docker-compose/dynamodb.yml up -d --wait
poetry run pytest tests/integration/aws
docker compose -f docker-compose/dynamodb.yml down --volumes
```

For PostgreSQL, use `docker-compose/postgres.yml` and `tests/integration/sqlalchemy` with the
`adapter-test` group and `sqlalchemy` extra. For ScyllaDB, use `docker-compose/scylladb.yml` and
`tests/integration/scylladb` with the `scylla` extra. Stop services after testing; CI collects service
logs on failure and tears services down even if tests fail.

## Dependency and CI maintenance

`pyproject.toml` declares the public extras and separate `test`, `adapter-test`, `ml-test`, `lint`,
and `typing` groups. `poetry.lock` records their resolved versions. Update both declarations and
lockfile when changing dependency requirements, then validate the affected suites:

```bash
poetry update
poetry check --lock
poetry build
```

The pre-commit Ruff version and the locked Ruff version must agree. GitHub Actions are pinned to
commit SHAs; keep their version comments current when updating them. Dependabot checks Python
packages and Actions weekly.

Every pull request runs the Linux core gate on Python 3.11 and 3.15, with adapter suites selected
by changed paths. Core implementation, core test, shared core fixture, and infrastructure changes
also run macOS ARM and Intel core checks. Every main push retains all supported core interpreters
and both macOS architectures. Workflow or dependency changes, scheduled runs, and manual runs
exercise the full matrix. Superseded runs on the same PR or on `main` are cancelled; separate PRs and
scheduled/manual runs remain independent.

## Locked dependency security audit

The `Locked dependency audit` workflow queries [OSV](https://osv.dev/) weekly,
on demand, and when its inputs or `poetry.lock` change. It checks every exact
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
