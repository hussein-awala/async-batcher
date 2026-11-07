# Contributing Guidelines

We welcome contributions from the community and are happy to have them.
Please follow this guide when logging issues or making changes.

## Testing

We use `pytest` for testing. Install the project and development dependencies with all adapter extras from the
repository root:

```bash
poetry install --all-extras
poetry run pytest
```

Integration tests are skipped by default. To work on the core batcher without installing database or machine
learning dependencies, use a virtual environment and run only the core tests:

```bash
python -m pip install -e . "pytest>=8,<9" "pytest-asyncio>=0.23.5,<0.24"
python -m pytest tests/test_batcher*.py
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
