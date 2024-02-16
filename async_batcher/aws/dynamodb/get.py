from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import aioboto3

from async_batcher.batcher import AsyncBatcher

if TYPE_CHECKING:
    from aiobotocore.config import AioConfig
    from types_aiobotocore_dynamodb import DynamoDBServiceResource
    from types_aiobotocore_dynamodb.type_defs import TableAttributeValueTypeDef


@dataclass(kw_only=True)
class GetItem:
    table_name: str
    key: dict[str, TableAttributeValueTypeDef]


class AsyncDynamoDbGetBatcher(AsyncBatcher[GetItem, dict[str, Any]]):
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

    async def process_batch(self, batch: list[GetItem]) -> list[dict[str, TableAttributeValueTypeDef]]:
        batch_result_keys = []
        request_items = {}
        for item in batch:
            if item.table_name not in request_items:
                request_items[item.table_name] = {"Keys": []}
            # TODO: support ProjectionExpression and ConsistentRead
            request_items[item.table_name]["Keys"].append(item.key)
            batch_result_keys.append((item.table_name, len(request_items[item.table_name]["Keys"]) - 1))

        dynamodb: DynamoDBServiceResource
        async with self.aioboto3_session.resource(
            "dynamodb",
            region_name=self.region_name,
            use_ssl=self.use_ssl,
            verify=self.verify,
            endpoint_url=self.endpoint_url,
            config=self.config,
        ) as dynamodb:
            response = await dynamodb.batch_get_item(
                RequestItems=request_items,
                ReturnConsumedCapacity="NONE",
            )
            return [response["Responses"][table_name][index] for table_name, index in batch_result_keys]
