from __future__ import annotations

from typing import Any

from async_batcher.exceptions import AsyncBatchException


class DynamoDBRetryError(AsyncBatchException):
    """A batch still contains unprocessed requests after its retry budget is exhausted.

    Requests omitted from ``unprocessed_items`` may already have succeeded. A failed
    batch write is not rolled back.
    """

    def __init__(self, response_key: str, unprocessed_items: dict[str, Any], max_retries: int):
        self.unprocessed_items = unprocessed_items
        super().__init__(f"DynamoDB returned {response_key} after {max_retries} retries")
