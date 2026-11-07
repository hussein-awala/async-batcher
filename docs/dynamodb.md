# DynamoDB reads and writes

From the [current checkout](../README.md#install), run `uv sync --locked --extra aws`.
The adapters use aioboto3 resource values: Python strings, integers/`Decimal`, bytes, lists,
and dictionaries, rather than low-level AttributeValue wrappers. Floats are not supported by
boto3's DynamoDB serializer.

```python
import asyncio

from async_batcher.aws.dynamodb.get import AsyncDynamoDbGetBatcher, GetItem
from async_batcher.aws.dynamodb.write import AsyncDynamoDbWriteBatcher, WriteOperation


async def write_then_read():
    # Configure AWS credentials normally; the table and its Key partition key must exist.
    writer = AsyncDynamoDbWriteBatcher(region_name="us-west-2")
    reader = AsyncDynamoDbGetBatcher(region_name="us-west-2", consistent_read=True)
    try:
        await asyncio.gather(
            writer.process(WriteOperation(operation="PUT", table_name="test-table", data={"Key": "one"})),
            writer.process(WriteOperation(operation="PUT", table_name="test-table", data={"Key": "two"})),
        )
        return await asyncio.gather(
            reader.process(GetItem(table_name="test-table", key={"Key": "one"})),
            reader.process(GetItem(table_name="test-table", key={"Key": "two"})),
        )
    finally:
        await asyncio.gather(writer.stop(), reader.stop())
```

Reads return an item dictionary or `None` for a missing item. Requests for the same table must
use the same primary-key attribute names. Duplicate keys are deduplicated for the service call
and mapped back to every caller. The response order from DynamoDB does not affect caller order.

Writes accept `WriteOperation(operation="PUT", ...)` with a full item or `operation="DELETE"`
with its key. They return `None`; deleting a missing item succeeds. Conditional writes and updates
are outside this batch API. Repeated operations on the same item in a write batch are not deduplicated
and can be rejected by DynamoDB.

## Limits, ownership, and retries

Read `max_batch_size` defaults to 100 and accepts 1–100; write defaults to 25 and accepts 1–25.
The count applies across all tables in the dispatch. Other [core options](api.md#configuration)
are available. AWS can also reject requests for byte limits or invalid keys.

Both adapters accept `region_name`, `endpoint_url`, `use_ssl`, `verify`, `config`, and an optional
`aioboto3_session`. TLS is enabled by default. Each dispatch opens and closes its own DynamoDB
resource context from that session; there is no client for the caller to close through the batcher.
Use the [local FastAPI example](../examples/dynamodb/README.md) to avoid contacting AWS.

Unprocessed keys/items are retried up to `max_retries` times after the first request (default 5;
0 disables retries). Backoff starts at 0.1 seconds and caps at 5 seconds. Exhaustion raises
`DynamoDBRetryError`, available from `async_batcher.aws.dynamodb.exceptions`, for the whole
dispatch. Previously completed writes are not rolled back, and partial read results are not
returned when the dispatch fails. SDK retries are configured separately on the client/session.

## Read consistency

Reads are eventually consistent by default. `consistent_read=True` applies strongly consistent
reads to every requested table and is preserved for unprocessed-key retries. Strong reads have
different capacity costs; see [DynamoDB read consistency](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.ReadConsistency.html).

## Projected reads

Use `projection_expression` and optional `expression_attribute_names` to select fields for every
requested table. Include **every primary-key attribute as a complete top-level field** so the
adapter can map unordered responses, including duplicate requests. It validates this before
opening a connection and does not insert hidden fields into the returned projection.

```python
from async_batcher.aws.dynamodb.get import AsyncDynamoDbGetBatcher

reader = AsyncDynamoDbGetBatcher(
    projection_expression="#pk, #sk, details.#name, tags[0]",
    expression_attribute_names={"#pk": "partitionKey", "#sk": "sortKey", "#name": "name"},
)
```

Nested paths and list indexes are forwarded to DynamoDB but do not count as complete key fields.
The alias mapping is copied at construction; retries preserve projection and aliases. Without a
projection, reads return all attributes. A projection reduces returned data,
[not read capacity consumption](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.ProjectionExpressions.html).
