# DynamoDB with FastAPI

This example batches `/put` and `/get` requests against DynamoDB Local. It uses dummy
credentials, region `us-west-2`, and `http://localhost:8000`; the application listens on 8001.
Both batchers drain during FastAPI shutdown. Their two-second queue delay makes batching visible
in the demo; tune it for your application. See the [DynamoDB guide](../../docs/dynamodb.md).

## Install and start the local database

From the repository root:

```bash
uv venv --python 3.12 examples/dynamodb/.venv
uv pip install --python examples/dynamodb/.venv/bin/python -e '.[aws]' 'fastapi>=0.162,<1' 'uvicorn>=0.54,<1'
docker compose -f docker-compose/dynamodb.yml up -d --wait
examples/dynamodb/.venv/bin/python .github/scripts/wait_for_service.py aws
```

These example dependencies are separate from the repository's locked environment. Invoke this
interpreter directly to retain the additional packages. On Windows, use `.venv/Scripts/python.exe`
in place of `.venv/bin/python`.

Create the demo tables (existing tables are kept):

```bash
examples/dynamodb/.venv/bin/python - <<'PYTHON'
import asyncio
import aioboto3


async def main():
    session = aioboto3.Session(
        region_name="us-west-2",
        aws_access_key_id="DUMMYIDEXAMPLE",
        aws_secret_access_key="DUMMYEXAMPLEKEY",
    )
    async with session.client("dynamodb", endpoint_url="http://localhost:8000") as client:
        for name, fields in [("test-table", ["Key"]), ("multi-keys-table", ["key1", "key2"])]:
            try:
                await client.create_table(
                    TableName=name,
                    AttributeDefinitions=[{"AttributeName": field, "AttributeType": "S"} for field in fields],
                    KeySchema=[
                        {"AttributeName": field, "KeyType": "HASH" if index == 0 else "RANGE"}
                        for index, field in enumerate(fields)
                    ],
                    BillingMode="PAY_PER_REQUEST",
                )
            except client.exceptions.ResourceInUseException:
                pass
            await client.get_waiter("table_exists").wait(TableName=name)


asyncio.run(main())
PYTHON
```

## Run and submit requests

Start the application:

```bash
cd examples/dynamodb
.venv/bin/python -m uvicorn main:app --reload --port 8001
```

In another terminal, submit concurrent writes and wait before reading them:

```bash
curl --fail-with-body -X POST http://127.0.0.1:8001/put \
  -H 'Content-Type: application/json' \
  -d '{"table_name":"test-table","data":{"Key":"one","value":"first"}}' &
curl --fail-with-body -X POST http://127.0.0.1:8001/put \
  -H 'Content-Type: application/json' \
  -d '{"table_name":"test-table","data":{"Key":"two","value":"second"}}' &
wait
curl --fail-with-body -X POST http://127.0.0.1:8001/get \
  -H 'Content-Type: application/json' \
  -d '{"table_name":"test-table","key":{"Key":"one"}}' &
curl --fail-with-body -X POST http://127.0.0.1:8001/get \
  -H 'Content-Type: application/json' \
  -d '{"table_name":"test-table","key":{"Key":"two"}}' &
wait
```

Successful writes return JSON `null`; reads return an item or `null` if missing. For
`multi-keys-table`, include both `key1` and `key2` in each item's data or read key. Reads in this
example are eventually consistent; set `consistent_read=True` when your application needs it.

Stop Uvicorn with Ctrl+C to drain the batchers. From the repository root, remove the demo service
and its stored data when finished:

```bash
docker compose -f docker-compose/dynamodb.yml down --volumes
```
