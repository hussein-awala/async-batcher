from __future__ import annotations

from typing import Any

import aioboto3
from async_batcher.aws.dynamodb.get import AsyncDynamoDbGetBatcher, GetItem
from async_batcher.aws.dynamodb.write import AsyncDynamoDbWriteBatcher, WriteOperation
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI()

aioboto3_session = aioboto3.Session(
    region_name="us-west-2",
    aws_access_key_id="DUMMYIDEXAMPLE",
    aws_secret_access_key="DUMMYEXAMPLEKEY",
)

get_batcher = AsyncDynamoDbGetBatcher(
    endpoint_url="http://localhost:8000",
    aioboto3_session=aioboto3_session,
    max_queue_time=2,  # just for testing
)

write_batcher = AsyncDynamoDbWriteBatcher(
    endpoint_url="http://localhost:8000",
    aioboto3_session=aioboto3_session,
    max_queue_time=2,  # just for testing
)


class PutRequestModel(BaseModel):
    table_name: str
    data: dict[str, Any]


class GetRequestModel(BaseModel):
    table_name: str
    key: dict[str, Any]


@app.on_event("shutdown")
def shutdown_event():
    get_batcher.stop()
    write_batcher.stop()


@app.post("/put")
async def put(request_data: PutRequestModel):
    try:
        return await write_batcher.process(
            item=WriteOperation(operation="PUT", table_name=request_data.table_name, data=request_data.data)
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


@app.post("/get")
async def get(request_data: GetRequestModel):
    try:
        return await get_batcher.process(
            item=GetItem(table_name=request_data.table_name, key=request_data.key)
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
