from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable


class RecordBatchError(Exception):
    """A record rejected by Kinesis or Firehose after bounded retries.

    ``code`` and ``message`` are the service's error fields. ``attempts`` counts
    calls containing this record, including the initial call. No payload is kept.
    """

    def __init__(self, code: str, message: str, attempts: int):
        self.code = code
        self.message = message
        self.attempts = attempts
        super().__init__(f"{code}: {message} (after {attempts} attempt(s))")


def validate_options(max_batch_size: int, max_retries: int) -> None:
    if not 1 <= max_batch_size <= 500:
        raise ValueError("max_batch_size must be between 1 and 500")
    if isinstance(max_retries, bool) or not isinstance(max_retries, int) or max_retries < 0:
        raise ValueError("max_retries must be a non-negative integer")


async def send_records(
    operation: Callable[..., Awaitable[dict[str, Any]]],
    records: list[dict[str, Any]],
    sizes: list[int],
    *,
    max_bytes: int,
    max_retries: int,
    response_key: str,
    success_keys: tuple[str, ...],
    retry_codes: set[str],
    **kwargs: Any,
) -> list[dict[str, Any] | Exception]:
    """Split by service limits and retry only explicitly rejected records."""
    results: list[dict[str, Any] | Exception] = []
    start = 0
    while start < len(records):
        end = start
        size = 0
        while end < len(records) and end - start < 500 and size + sizes[end] <= max_bytes:
            size += sizes[end]
            end += 1
        if end == start:
            raise ValueError("record exceeds the request byte limit")
        pending = list(range(start, end))
        # Every slot is replaced by an acknowledgement or an error below.
        results.extend(RuntimeError("record was not processed") for _ in pending)
        for attempt in range(max_retries + 1):
            try:
                response = await operation(Records=[records[index] for index in pending], **kwargs)
                entries = response.get(response_key)
                if not isinstance(entries, list) or len(entries) != len(pending):
                    raise ValueError(f"{response_key} must contain one result per submitted record")
                for entry in entries:
                    if not isinstance(entry, dict) or (
                        not isinstance(entry.get("ErrorCode"), str)
                        and not all(isinstance(entry.get(key), str) and entry[key] for key in success_keys)
                    ):
                        raise ValueError("record response lacks an acknowledgement or error code")
            except Exception as error:
                # An earlier response may already have acknowledged other records.
                # Do not replace those outcomes or resend them after a request error.
                for index in pending:
                    results[index] = error
                break
            retry = []
            for index, entry in zip(pending, entries, strict=True):
                code = entry.get("ErrorCode")
                if code is None:
                    results[index] = entry
                elif code in retry_codes and attempt < max_retries:
                    retry.append(index)
                else:
                    results[index] = RecordBatchError(code, entry.get("ErrorMessage", ""), attempt + 1)
            if not retry:
                break
            pending = retry
            await asyncio.sleep(min(0.1 * 2 ** min(attempt, 6), 5.0))
        start = end
    return results
