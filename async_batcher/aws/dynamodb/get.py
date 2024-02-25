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
    """Batcher for DynamoDB GetItem operation. It uses aioboto3 to interact with DynamoDB.

    Args:
        region_name: The region to use.
        use_ssl: Whether to use SSL/TLS.
        verify: Whether to verify SSL certificates.
        endpoint_url: The complete URL to use for the constructed client. This is useful for local testing.
        config: The configuration for the session.
        aioboto3_session: The aioboto3 session to use. If not provided, a new session is created.
        max_batch_size (int, optional): The max number of items to process in a batch.The default is 100
            items, which is the maximum number of items that can be processed in a single batch.
        max_queue_time (float, optional): The max time for a task to stay in the queue before processing
            it if the batch is not full and the number of running batches is less than the concurrency.
            Defaults to 0.01.
        concurrency (int, optional): The max number of concurrent batches to process. Defaults to 1.
            If -1, it will process all batches concurrently.
    """

    def __init__(
        self,
        *,
        region_name: str | None = None,
        use_ssl: bool | None = None,
        verify: bool | None = None,
        endpoint_url: str | None = None,
        config: AioConfig | None = None,
        aioboto3_session: aioboto3.Session | None = None,
        max_batch_size: int = 100,
        max_queue_time: float = 0.01,
        concurrency: int = 1,
        **kwargs,
    ):
        super().__init__(
            max_batch_size=max_batch_size, max_queue_time=max_queue_time, concurrency=concurrency, **kwargs
        )
        self.region_name = region_name
        self.use_ssl = use_ssl
        self.verify = verify
        self.endpoint_url = endpoint_url
        self.config = config
        self.aioboto3_session = aioboto3_session or aioboto3.Session()

    async def process_batch(self, batch: list[GetItem]) -> list[dict[str, TableAttributeValueTypeDef]]:
        indexed_items: dict[tuple, int] = {}
        request_items = {}
        tables_keys = {}
        for ind, item in enumerate(batch):
            if item.table_name not in request_items:
                request_items[item.table_name] = {"Keys": []}
                tables_keys[item.table_name] = sorted(item.key.keys())
            # TODO: support ProjectionExpression and ConsistentRead
            request_items[item.table_name]["Keys"].append(item.key)
            indexed_items[(item.table_name, *[item.key[key] for key in tables_keys[item.table_name]])] = ind

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
            result: list[None | dict[str, TableAttributeValueTypeDef]] = [None] * len(batch)
            # TODO: handle UnprocessedKeys
            for table in response["Responses"]:
                for item in response["Responses"][table]:
                    index = indexed_items[(table, *[item[key] for key in tables_keys[table]])]
                    result[index] = item
            return result
