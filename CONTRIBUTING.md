# Contributing Guidelines

We welcome contributions from the community and are happy to have them.
Please follow this guide when logging issues or making changes.

## Development tools

Run `pre-commit` in a Python 3.11 or newer environment. The patched `filelock`
release used by the development tools requires Python 3.11; the library and its
tests continue to support Python 3.10. Poetry omits the lint environment tools
when installing development dependencies on Python 3.10.

```bash
poetry install
poetry run pre-commit run --all-files
```

## Testing

We use pytest 9 and pytest-asyncio 1 for testing. Async fixtures and tests use
function-scoped event loops by default; shared database fixtures explicitly use
a session-scoped loop. Use `loop_scope`, rather than the deprecated `scope`
argument, when choosing an async test loop.

Install the project and development dependencies with all adapter extras from the
repository root:

```bash
poetry install --all-extras
poetry run pytest
```

Integration tests are skipped by default. To work on the core batcher without the optional adapter extras,
install the declared development dependencies and run only the core tests:

```bash
poetry install
poetry run pytest tests/test_batcher*.py
```

For integration tests, start the required services using the provided Compose files:

```bash
docker compose -f docker-compose/dynamodb.yml -f docker-compose/postgres.yml -f docker-compose/scylladb.yml up -d --wait
poetry run pytest -m integration
```

You can also select individual integrations after starting the corresponding service:

```bash
poetry run pytest -m integration_dynamodb
poetry run pytest -m integration_postgres
poetry run pytest -m integration_scylladb
```

Run the formatting and lint checks before submitting a change:

```bash
poetry run pre-commit run --all-files
```
