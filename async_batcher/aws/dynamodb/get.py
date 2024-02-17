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
        batch_size: The maximum number of items to process in a single batch. The default is 100 items,
            which is the maximum number of items that can be processed in a single batch.
        sleep_time (float, optional): The time to sleep between checking if the result is ready in seconds.
            Defaults to 0.01. Set it to a value close to the expected time to process a batch
        buffering_time (float, optional): The time to sleep after processing a batch or checking the buffer
            in seconds. Defaults to 0.001.
            You can increase this value if you don't need a low latency, but want to reduce the number of
            processed batches.
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
        batch_size: int = 100,
        sleep_time: float = 0.01,
        buffering_time: float = 0.001,
    ):
        super().__init__(batch_size=batch_size, sleep_time=sleep_time, buffering_time=buffering_time)
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
