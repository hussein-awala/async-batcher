from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

from async_batcher.aws.dynamodb.exceptions import DynamoDBRetryError

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Callable


def validate_max_retries(max_retries: int) -> None:
    if isinstance(max_retries, bool) or not isinstance(max_retries, int) or max_retries < 0:
        raise ValueError("max_retries must be a non-negative integer")


async def batch_responses(
    operation: Callable[..., Awaitable[dict[str, Any]]],
    request_items: dict[str, Any],
    unprocessed_key: str,
    max_retries: int,
    **kwargs: Any,
) -> AsyncIterator[dict[str, Any]]:
    delay = 0.1
    for attempt in range(max_retries + 1):
        response = await operation(RequestItems=request_items, **kwargs)
        yield response
        request_items = response.get(unprocessed_key, {})
        if not request_items:
            return
        if attempt == max_retries:
            raise DynamoDBRetryError(unprocessed_key, request_items, max_retries)
        await asyncio.sleep(delay)
        delay = min(delay * 2, 5.0)
