from __future__ import annotations

import re
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import aioboto3
from boto3.dynamodb.types import Binary, TypeSerializer

from async_batcher.aws.dynamodb._retry import batch_responses, validate_max_retries
from async_batcher.batcher import AsyncBatcher

if TYPE_CHECKING:
    from aiobotocore.config import AioConfig
    from types_aiobotocore_dynamodb import DynamoDBServiceResource
    from types_aiobotocore_dynamodb.type_defs import TableAttributeValueTypeDef


@dataclass(kw_only=True)
class GetItem:
    table_name: str
    key: dict[str, TableAttributeValueTypeDef]


class AsyncDynamoDbGetBatcher(AsyncBatcher[GetItem, dict[str, Any] | None]):
    """Batcher for DynamoDB GetItem operation. It uses aioboto3 to interact with DynamoDB.

    Args:
        region_name: The region to use.
        use_ssl: Whether to use SSL/TLS. Defaults to True when omitted or None.
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
        consistent_read (bool, optional): Use strongly consistent reads for every table.
            Defaults to False (eventually consistent reads).
        projection_expression: Optional projection applied to every requested table. It must
            include every primary-key attribute in full so unordered results can be matched.
        expression_attribute_names: Aliases used by the projection, for reserved words or
            attribute names containing special characters. The mapping is copied.
        max_retries (int, optional): Number of retries for unprocessed requests, with exponential backoff.
            Defaults to 5. Exhaustion raises DynamoDBRetryError.
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
        max_retries: int = 5,
        consistent_read: bool = False,
        projection_expression: str | None = None,
        expression_attribute_names: dict[str, str] | None = None,
        **kwargs,
    ):
        super().__init__(
            max_batch_size=max_batch_size, max_queue_time=max_queue_time, concurrency=concurrency, **kwargs
        )
        validate_max_retries(max_retries)
        self.max_retries = max_retries
        if not isinstance(consistent_read, bool):
            raise ValueError("consistent_read must be a boolean")
        self.consistent_read = consistent_read
        if projection_expression is not None and (
            not isinstance(projection_expression, str) or not projection_expression.strip()
        ):
            raise ValueError("projection_expression must be a nonempty string or None")
        if expression_attribute_names is not None:
            if projection_expression is None:
                raise ValueError("expression_attribute_names requires projection_expression")
            if not isinstance(expression_attribute_names, dict) or any(
                not isinstance(alias, str)
                or re.fullmatch(r"#[a-zA-Z0-9_]+", alias) is None
                or not isinstance(name, str)
                or not name
                for alias, name in expression_attribute_names.items()
            ):
                raise ValueError("expression_attribute_names must map #aliases to nonempty attribute names")
        self.projection_expression = projection_expression
        self.expression_attribute_names = dict(expression_attribute_names or {})
        if not 1 <= self.max_batch_size <= 100:
            raise ValueError("DynamoDB get max_batch_size must be between 1 and 100")
        self.region_name = region_name
        self.use_ssl = True if use_ssl is None else use_ssl
        self.verify = verify
        self.endpoint_url = endpoint_url
        self.config = config
        self.aioboto3_session = aioboto3_session or aioboto3.Session()

    @staticmethod
    def _item_key(table: str, item: dict, key_names: list[str]) -> tuple:
        values = []
        for key in key_names:
            value = item[key]
            if isinstance(value, Binary):
                value = value.value
            if isinstance(value, bytearray):
                value = bytes(value)
            values.append(value)
        return (table, *values)

    async def process_batch(self, batch: list[GetItem]) -> list[dict[str, TableAttributeValueTypeDef] | None]:
        if not batch:
            return []
        indexed_items: dict[tuple, list[int]] = {}
        request_items = {}
        tables_keys = {}
        serializer = TypeSerializer()
        projection = self.projection_expression
        aliases = self.expression_attribute_names.copy()
        projected_attributes = set()
        if projection is not None:
            # Only complete top-level attributes can identify a scalar primary key.
            # A path such as "id.child" or "id[0]" does not include the key "id".
            for path in projection.split(","):
                path = path.strip()
                if path in aliases:
                    projected_attributes.add(aliases[path])
                elif re.fullmatch(r"[a-zA-Z][a-zA-Z0-9_]*", path):
                    projected_attributes.add(path)
        for ind, item in enumerate(batch):
            key_names = sorted(item.key)
            if not key_names:
                raise ValueError("DynamoDB keys must contain at least one attribute")
            if item.table_name in tables_keys and key_names != tables_keys[item.table_name]:
                raise ValueError("Requests for the same DynamoDB table must use the same key attributes")
            # Validate before deduplication: Python considers True and 1.0 equal to 1.
            for value in item.key.values():
                serialized = serializer.serialize(value)
                if not serialized.keys() <= {"S", "N", "B"}:
                    raise ValueError("DynamoDB key attributes must be strings, numbers, or binary values")
            if projection is not None and not set(key_names) <= projected_attributes:
                raise ValueError("projection_expression must include every complete primary-key attribute")
            if item.table_name not in request_items:
                request_items[item.table_name] = {"Keys": []}
                if projection is not None:
                    request_items[item.table_name]["ProjectionExpression"] = projection
                    if aliases:
                        request_items[item.table_name]["ExpressionAttributeNames"] = aliases.copy()
                if self.consistent_read:
                    request_items[item.table_name]["ConsistentRead"] = True
                tables_keys[item.table_name] = key_names
            key = self._item_key(item.table_name, item.key, tables_keys[item.table_name])
            if key not in indexed_items:
                request_items[item.table_name]["Keys"].append(item.key)
                indexed_items[key] = []
            indexed_items[key].append(ind)

        dynamodb: DynamoDBServiceResource
        async with self.aioboto3_session.resource(
            "dynamodb",
            region_name=self.region_name,
            use_ssl=self.use_ssl,
            verify=self.verify,
            endpoint_url=self.endpoint_url,
            config=self.config,
        ) as dynamodb:
            result: list[None | dict[str, TableAttributeValueTypeDef]] = [None] * len(batch)
            async for response in batch_responses(
                dynamodb.batch_get_item,
                request_items,
                "UnprocessedKeys",
                self.max_retries,
                ReturnConsumedCapacity="NONE",
            ):
                for table, items in response.get("Responses", {}).items():
                    for item in items:
                        key = self._item_key(table, item, tables_keys[table])
                        for index in indexed_items[key]:
                            result[index] = item
            return result
