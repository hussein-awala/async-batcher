from __future__ import annotations

from typing import Any

import aioboto3
from async_batcher.aws.dynamodb.get import AsyncDynamoDbGetBatcher, GetItem
from async_batcher.aws.dynamodb.write import AsyncDynamoDbWriteBatcher, WriteOperation

from fastapi import FastAPI

app = FastAPI()
aioboto3_session = aioboto3.Session(
    region_name="us-west-2",
    aws_access_key_id="DUMMYIDEXAMPLE",
    aws_secret_access_key="DUMMYEXAMPLEKEY",
)

get_batcher = AsyncDynamoDbGetBatcher(
    endpoint_url="http://localhost:8000", aioboto3_session=aioboto3_session, batch_size=200, sleep_time=0.0001
)

write_batcher = AsyncDynamoDbWriteBatcher(
    endpoint_url="http://localhost:8000", aioboto3_session=aioboto3_session, batch_size=200, sleep_time=0.0001
)


@app.on_event("startup")
async def startup_event():
    get_batcher.start()
    write_batcher.start()


@app.on_event("shutdown")
def shutdown_event():
    get_batcher.stop()
    write_batcher.stop()


@app.post("/put")
async def put(data: dict[str, Any]):
    return await write_batcher.process(
        item=WriteOperation(operation="PUT", table_name="test-table", data=data)
    )


@app.post("/get")
async def get(key: dict[str, Any]):
    return await get_batcher.process(item=GetItem(table_name="test-table", key=key))
