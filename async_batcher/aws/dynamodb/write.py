from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

import aioboto3

from async_batcher.batcher import AsyncBatcher

if TYPE_CHECKING:
    from aiobotocore.config import AioConfig
    from types_aiobotocore_dynamodb import DynamoDBServiceResource
    from types_aiobotocore_dynamodb.type_defs import TableAttributeValueTypeDef

# For Python 3.8 and 3.9 compatibility
KW_ONLY_DATACLASS = {"kw_only": True} if "kw_only" in dataclass.__kwdefaults__ else {}


@dataclass(**KW_ONLY_DATACLASS)
class WriteOperation:
    operation: Literal["PUT", "DELETE"]
    table_name: str
    data: dict[str, TableAttributeValueTypeDef]


class AsyncDynamoDbWriteBatcher(AsyncBatcher[WriteOperation, None]):
    """Batcher for DynamoDB WriteOperation. It uses aioboto3 to interact with DynamoDB.

    Args:
        region_name: The region to use.
        use_ssl: Whether to use SSL/TLS.
        verify: Whether to verify SSL certificates.
        endpoint_url: The complete URL to use for the constructed client. This is useful for local testing.
        config: The configuration for the session.
        aioboto3_session: The aioboto3 session to use. If not provided, a new session is created.
        batch_size: The maximum number of items to process in a single batch. The default is 25 items,
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
        max_batch_size: int = 25,
        max_queue_time: float = 0.001,
    ):
        super().__init__(max_batch_size=max_batch_size, max_queue_time=max_queue_time)
        self.region_name = region_name
        self.use_ssl = use_ssl
        self.verify = verify
        self.endpoint_url = endpoint_url
        self.config = config
        self.aioboto3_session = aioboto3_session or aioboto3.Session()

    async def process_batch(self, batch: list[WriteOperation]) -> None:
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
