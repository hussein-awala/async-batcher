"""Collate variable-length token rows and restore each request's original shape."""

from __future__ import annotations

import asyncio
import inspect
import json
import math
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from contextvars import Context
from dataclasses import dataclass
from itertools import pairwise
from numbers import Real
from typing import TYPE_CHECKING

from async_batcher import AsyncBatcher, GroupedBatcher

if TYPE_CHECKING:
    from collections.abc import Sequence


@dataclass(frozen=True, init=False)
class TokenRequest:
    request_id: str
    tenant: str
    model_revision: str
    tokens: tuple[int, ...]

    def __init__(
        self, request_id: str, tenant: str, model_revision: str, tokens: list[int] | tuple[int, ...]
    ):
        if not all(isinstance(value, str) and value for value in (request_id, tenant, model_revision)):
            raise ValueError("Request ID, tenant and model revision must be nonempty strings")
        if not isinstance(tokens, list | tuple) or not tokens:
            raise ValueError("Tokens must be a nonempty list or tuple")
        if any(isinstance(token, bool) or not isinstance(token, int) or token < 0 for token in tokens):
            raise ValueError("Token IDs must be nonnegative integers")
        object.__setattr__(self, "request_id", request_id)
        object.__setattr__(self, "tenant", tenant)
        object.__setattr__(self, "model_revision", model_revision)
        object.__setattr__(self, "tokens", tuple(tokens))


@dataclass(frozen=True)
class ModelBatch:
    tenant: str
    model_revision: str
    input_ids: tuple[tuple[int, ...], ...]
    attention_mask: tuple[tuple[int, ...], ...]


@dataclass(frozen=True)
class TokenPrediction:
    request_id: str
    values: tuple[float, ...]


def collate(requests: Sequence[TokenRequest]) -> ModelBatch:
    """Pad to this batch's longest row, preserving original row order."""
    if not requests:
        raise ValueError("Cannot collate an empty batch")
    first = requests[0]
    if any((item.tenant, item.model_revision) != (first.tenant, first.model_revision) for item in requests):
        raise ValueError("All rows must use one tenant and model revision")
    width = max(len(item.tokens) for item in requests)
    return ModelBatch(
        first.tenant,
        first.model_revision,
        tuple(item.tokens + (0,) * (width - len(item.tokens)) for item in requests),
        tuple((1,) * len(item.tokens) + (0,) * (width - len(item.tokens)) for item in requests),
    )


def unbatch(requests: Sequence[TokenRequest], batch: ModelBatch, predictions) -> list[TokenPrediction]:
    """Validate the complete dense result before restoring unpadded outputs."""
    if not isinstance(predictions, list | tuple) or len(predictions) != len(requests):
        raise ValueError("Model must return one list/tuple row per input")
    width = len(batch.input_ids[0])
    results = []
    for request, row in zip(requests, predictions, strict=True):
        if not isinstance(row, list | tuple) or len(row) != width:
            raise ValueError("Model rows must match the padded width")
        values = []
        for value in row:
            if isinstance(value, bool) or not isinstance(value, Real):
                raise ValueError("Model values must be finite numbers")
            try:
                number = float(value)
            except (ValueError, OverflowError) as error:
                raise ValueError("Model values must be finite numbers") from error
            if not math.isfinite(number):
                raise ValueError("Model values must be finite numbers")
            values.append(number)
        results.append(TokenPrediction(request.request_id, tuple(values[: len(request.tokens)])))
    return results


def inference_groups(
    model,
    *,
    executor,
    bucket_limits=(16, 64, 256),
    padded_token_budget=256,
    max_batch_size=16,
    max_groups=12,
    max_queue_time=0.01,
):
    """Use conservative per-row costs and a caller-owned pool for sync inference.

    The model receives ModelBatch and returns a list/tuple of list/tuple numeric
    rows. Adapt array/tensor outputs explicitly, e.g. with .tolist().
    """
    if (
        not callable(model)
        or inspect.iscoroutinefunction(model)
        or inspect.iscoroutinefunction(model.__call__)
    ):
        raise TypeError("This recipe requires a synchronous model callable")
    if not isinstance(executor, ThreadPoolExecutor):
        raise TypeError("Provide a caller-owned ThreadPoolExecutor")
    limits = tuple(bucket_limits)
    if (
        not limits
        or any(isinstance(value, bool) or not isinstance(value, int) or value < 1 for value in limits)
        or any(left >= right for left, right in pairwise(limits))
    ):
        raise ValueError("Bucket limits must be strictly increasing positive integers")
    if (
        isinstance(padded_token_budget, bool)
        or not isinstance(padded_token_budget, int)
        or padded_token_budget < limits[-1]
    ):
        raise ValueError("Padded token budget must cover the largest bucket")

    def key(item):
        for limit in limits:
            if len(item.tokens) <= limit:
                return item.tenant, item.model_revision, limit
        raise ValueError("Request exceeds the largest length bucket; split explicitly upstream")

    def factory(group):
        def process(requests):
            batch = collate(requests)
            predictions = model(batch)
            if inspect.isawaitable(predictions):
                if inspect.iscoroutine(predictions):
                    predictions.close()
                raise TypeError("This recipe requires synchronous model results")
            return unbatch(requests, batch, predictions)

        return AsyncBatcher.from_callable(
            process,
            executor=executor,
            max_batch_size=max_batch_size,
            max_batch_cost=padded_token_budget,
            item_cost=lambda item: group[2],
            max_queue_size=64,
            max_queue_time=max_queue_time,
            concurrency=1,
            processor_context=Context(),
            log_exceptions=False,
        )

    return GroupedBatcher(factory, group_key=key, max_groups=max_groups, log_exceptions=False)


class DemoTokenModel:
    """Doubles token IDs; this is not a trained or semantic model."""

    def __init__(self):
        self._shapes = Counter()
        self._lock = threading.Lock()

    def __call__(self, batch):
        with self._lock:
            self._shapes[len(batch.input_ids), len(batch.input_ids[0])] += 1
        return [[float(token * 2) for token in row] for row in batch.input_ids]

    def report(self):
        with self._lock:
            return [
                {"rows": rows, "padded_width": width, "calls": count}
                for (rows, width), count in sorted(self._shapes.items())
            ]


async def demo(pool):
    model = DemoTokenModel()
    cohort = [
        TokenRequest(str(index), "demo-tenant", "demo-v1", tuple(range(length)))
        for index, length in enumerate([1, 3, 16, 17, 33, 64, 65, 129] * 4)
    ]
    started = time.perf_counter()
    async with inference_groups(model, executor=pool) as groups:
        results = await asyncio.gather(*(groups.process(item) for item in cohort))
    return {
        "demo_model": True,
        "completed_requests": len(results),
        "elapsed_seconds": time.perf_counter() - started,
        "actual_shapes": model.report(),
    }


if __name__ == "__main__":
    with ThreadPoolExecutor(max_workers=1) as pool:
        report = asyncio.run(demo(pool))
    print(json.dumps(report, indent=2))
