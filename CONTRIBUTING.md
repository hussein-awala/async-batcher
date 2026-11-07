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

CI runs a fast core gate for pull requests, selects adapter suites by changed paths, and runs broader
Python coverage for changes to the workflow or dependencies. Scheduled and manual runs exercise
the full matrix. Superseded runs on the same PR are cancelled; separate PRs remain independent.
