# DynamoDB Get and Write Example

This example shows how to use `AsyncDynamoDbGetBatcher` and `AsyncDynamoDbWriteBatcher` to optimize the read and write
operations in DynamoDB when using FastAPI.

## How to use
```bash
# Install from the repository root
pip install -e ".[aws]" fastapi uvicorn

# Run the example on port 8001; DynamoDB Local uses port 8000
cd examples/dynamodb
uvicorn main:app --reload --port 8001
```
You can use the `dynamodb-local` container ([dynamodb.yml](../../docker-compose/dynamodb.yml)) to test the example
locally.
```bash
docker compose -f ../../docker-compose/dynamodb.yml up -d
```
Before sending requests, create `test-table` with string partition key `Key`, and `multi-keys-table` with string
partition key `key1` and string sort key `key2` in DynamoDB Local. The example uses region `us-west-2` and dummy
credentials, which you can also use with the AWS CLI and `--endpoint-url http://localhost:8000`.

Then, use the following commands to put some items in the DynamoDB tables.
```bash
curl -X 'POST' \
  'http://127.0.0.1:8001/put' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{"table_name": "test-table", "data": {"Key": "key1", "attr1": "something1", "attr2": "something1"}}' &
curl -X 'POST' \
  'http://127.0.0.1:8001/put' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{"table_name": "test-table", "data": {"Key": "key2", "attr1": "something2", "attr2": "something2"}}' &
curl -X 'POST' \
  'http://127.0.0.1:8001/put' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{"table_name": "multi-keys-table", "data": {"key1": "value1", "key2": "value2", "attr2": "something3"}}' &
```
After that, you can use the following command to get the items from the DynamoDB table.
```bash
curl -X 'POST' \
  'http://127.0.0.1:8001/get' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{"table_name": "test-table", "key": {"Key": "key1"}}' &
curl -X 'POST' \
  'http://127.0.0.1:8001/get' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{"table_name": "test-table", "key": {"Key": "key2"}}' &
curl -X 'POST' \
  'http://127.0.0.1:8001/get' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{"table_name": "multi-keys-table", "key": {"key1": "value1", "key2": "value2"}}' &
curl -X 'POST' \
  'http://127.0.0.1:8001/get' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{"table_name": "multi-keys-table", "key": {"key1": "value1", "key2": "value3"}}' &
```
