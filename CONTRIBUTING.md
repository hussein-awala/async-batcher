# Contributing Guidelines

We welcome contributions from the community and are happy to have them.
Please follow this guide when logging issues or making changes.

## Testing

We use `pytest` for testing. To run the tests, you can use the following command:

```bash
pytest
```

For integration tests, we provide some `docker-compose` files to start the required services.
You can use the following command to start the services:

```bash
docker compose -f docker-compose/dynamodb.yml -f docker-compose/scylladb.yml up -d
```

And then run the tests with the following command:

```bash
pytest -m integration
```

You can also select specific integration tests:

```bash
pytest -m integration_dynamodb
```
or
```bash
pytest -m integration_scylladb
```
