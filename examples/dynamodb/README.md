# DynamoDB Get and Write Example

This example shows how to use `AsyncDynamoDbGetBatcher` and `AsyncDynamoDbWriteBatcher` to optimize the read and write
operations in DynamoDB when using FastAPI.

## How to use
```bash
# Install the aws extra dependencies from root directory
pip install -r ".[aws]"

# Run the FastAPI server with Uvicorn
PYTHONPATH=$PYTHONPATH:$(git rev-parse --show-toplevel) uvicorn main:app --reload
```
You can use the `dynamodb-local` container ([[dynamodb.yml](../../docker-compose/dynamodb.yml)]) to test the example
locally.
```bash
mkdir ../../dynamodb-data
docker-compose -f ../../docker-compose/dynamodb.yml up
```
Then, you can use the following commands to test the put some items in the DynamoDB table.
```bash
curl -X 'POST' \
  'http://127.0.0.1:8000/put' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{"Key": "key1", "attr1": "something1", "attr2": "something2"}'
curl -X 'POST' \
  'http://127.0.0.1:8000/put' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{"Key": "key2", "attr1": "something2", "attr2": "something2"}'
curl -X 'POST' \
  'http://127.0.0.1:8000/put' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{"Key": "key3", "attr1": "something3", "attr2": "something3"}'
```
After that, you can use the following command to get the items from the DynamoDB table.
```bash
curl -X 'POST' \
  'http://127.0.0.1:8000/get' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{"Key": "key1"}'
curl -X 'POST' \
  'http://127.0.0.1:8000/get' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{"Key": "key2"}'
curl -X 'POST' \
  'http://127.0.0.1:8000/get' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{"Key": "key3"}'
```