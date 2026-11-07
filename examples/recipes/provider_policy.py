"""Application-owned shared pacing and finite retries for explicit throttling."""

from __future__ import annotations

import asyncio
import json
import math
import random
from asyncio import sleep
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from time import monotonic

from async_batcher import AsyncBatcher


def positive(name, value):
    if (
        isinstance(value, bool)
        or not isinstance(value, int | float)
        or not math.isfinite(value)
        or value <= 0
    ):
        raise ValueError(f"{name} must be positive and finite")


class Throttled(Exception):
    """Raise only when the application permits replay of this rejected operation."""

    def __init__(self, retry_after: str | None = None):
        super().__init__("Provider throttled this operation")
        self.retry_after = retry_after


def retry_after_seconds(value: str | None) -> float | None:
    if value is None:
        return None
    value = value.strip()
    if value.isascii() and value.isdigit():
        try:
            return float(value)
        except (ValueError, OverflowError):
            return math.inf
    try:
        date = parsedate_to_datetime(value)
        if date.tzinfo is None:
            return None
        return max(0.0, (date - datetime.now(UTC)).total_seconds())
    except (ValueError, TypeError, OverflowError):
        return None


class Pacer:
    """Share one instance across calls/groups on one event loop; no burst credit."""

    def __init__(self, *, requests_per_second: float, units_per_second: float | None = None):
        positive("requests_per_second", requests_per_second)
        if units_per_second is not None:
            positive("units_per_second", units_per_second)
        self.requests_per_second, self.units_per_second = requests_per_second, units_per_second
        self._next_start = 0.0
        self._lock = asyncio.Lock()

    def defer(self, seconds: float):
        # Synchronous on the owning loop: a 429 can extend the deadline while a
        # different caller holds the lock and sleeps. That caller rechecks it.
        self._next_start = max(self._next_start, monotonic() + seconds)

    async def acquire(self, units: float = 0):
        if (
            isinstance(units, bool)
            or not isinstance(units, int | float)
            or not math.isfinite(units)
            or units < 0
        ):
            raise ValueError("units must be nonnegative and finite")
        interval = max(
            1 / self.requests_per_second, units / self.units_per_second if self.units_per_second else 0
        )
        positive("pacing interval", interval)
        async with self._lock:
            while (remaining := self._next_start - monotonic()) > 0:
                await sleep(remaining)
            # Cancelled waiters spend no future slot. Every actual attempt does,
            # even if its provider response fails or the caller later cancels.
            self._next_start = monotonic() + interval


class ProviderPolicy:
    def __init__(
        self, pacer: Pacer, *, max_attempts: int = 3, base_delay: float = 0.5, max_delay: float = 30
    ):
        if type(max_attempts) is not int or max_attempts < 1:
            raise ValueError("max_attempts must be a positive integer")
        positive("base_delay", base_delay)
        positive("max_delay", max_delay)
        if base_delay > max_delay:
            raise ValueError("base_delay must not exceed max_delay")
        self.pacer, self.max_attempts = pacer, max_attempts
        self.base_delay, self.max_delay = base_delay, max_delay
        self.attempts = self.retry_attempts = self.throttled_responses = 0

    async def run(self, operation, *, units: float = 0, total_timeout: float = 60):
        """Retry only explicit Throttled; borrow a cancellation-cooperative async call."""
        positive("total_timeout", total_timeout)
        async with asyncio.timeout(total_timeout):
            for attempt in range(self.max_attempts):
                await self.pacer.acquire(units)
                self.attempts += 1
                self.retry_attempts += attempt > 0
                try:
                    return await operation()
                except Throttled as error:
                    self.throttled_responses += 1
                    server_delay = retry_after_seconds(error.retry_after)
                    if server_delay is not None and server_delay > self.max_delay:
                        # Never shorten a server's longer Retry-After to our cap.
                        raise
                    ceiling = min(self.max_delay, self.base_delay * 2 ** min(attempt, 60))
                    delay = max(server_delay or 0, random.uniform(0, ceiling))
                    self.pacer.defer(delay)
                    if attempt + 1 == self.max_attempts:
                        raise


async def demo():
    policy = ProviderPolicy(Pacer(requests_per_second=100, units_per_second=1000), base_delay=0.01)
    calls = 0

    async def processor(items):
        async def local_operation():
            nonlocal calls
            calls += 1
            if calls == 1:
                raise Throttled("0")
            return [item * 2 for item in items]

        return await policy.run(local_operation, units=len(items), total_timeout=2)

    async with AsyncBatcher.from_callable(processor, max_batch_size=4, max_queue_size=8) as batcher:
        results = [result async for result in batcher.map(range(8), max_pending=8)]
    return {
        "results": results,
        "attempts": policy.attempts,
        "retry_attempts": policy.retry_attempts,
        "throttled_responses": policy.throttled_responses,
    }


if __name__ == "__main__":
    print(json.dumps(asyncio.run(demo()), indent=2))
