from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

import aioboto3

from async_batcher.batcher import AsyncBatcher

if TYPE_CHECKING:
    from aiobotocore.config import AioConfig
    from types_aiobotocore_dynamodb import DynamoDBServiceResource
    from types_aiobotocore_dynamodb.type_defs import TableAttributeValueTypeDef


@dataclass(kw_only=True)
class WriteOperation:
    operation: Literal["PUT", "DELETE"]
    table_name: str
    data: dict[str, TableAttributeValueTypeDef]


class AsyncDynamoDbWriteBatcher(AsyncBatcher[WriteOperation, None]):
    def __init__(
        self,
        *,
        region_name: str | None = None,
        use_ssl: bool | None = None,
        verify: bool | None = None,
        endpoint_url: str | None = None,
        config: AioConfig | None = None,
        aioboto3_session: aioboto3.Session | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.region_name = region_name
        self.use_ssl = use_ssl
        self.verify = verify
        self.endpoint_url = endpoint_url
        self.config = config
        self.aioboto3_session = aioboto3_session or aioboto3.Session()

    async def process_batch(self, batch: list[WriteOperation]) -> list[None]:
        request_items = {}
        for operation in batch:
            if operation.table_name not in request_items:
                request_items[operation.table_name] = []
            request = {}
            if operation.operation == "PUT":
                request["PutRequest"] = {"Item": operation.data}
            elif operation.operation == "DELETE":
                request["DeleteRequest"] = {"Key": operation.data}
            request_items[operation.table_name].append(request)

        dynamodb: DynamoDBServiceResource
        async with self.aioboto3_session.resource(
            "dynamodb",
            region_name=self.region_name,
            use_ssl=self.use_ssl,
            verify=self.verify,
            endpoint_url=self.endpoint_url,
            config=self.config,
        ) as dynamodb:
            # TODO: return something useful
            await dynamodb.batch_write_item(
                RequestItems=request_items,
                ReturnConsumedCapacity="NONE",
                ReturnItemCollectionMetrics="NONE",
            )
        return [None] * len(batch)
