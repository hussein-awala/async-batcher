from __future__ import annotations

import asyncio

import pytest

from tests.core_helpers import RecordingBatcher


@pytest.mark.asyncio
async def test_batch_size_one_dispatches_without_waiting(harness):
    batcher = harness.create(max_batch_size=1, max_queue_time=60)
    requests = harness.submit(batcher, 1, 2, 3)
    assert await asyncio.wait_for(asyncio.gather(*requests), timeout=1) == [2, 4, 6]
    assert batcher.batches == [[1], [2], [3]]


@pytest.mark.parametrize("value", [0, -2, None, 1.5])
def test_deprecated_batch_size_is_validated(value):
    with pytest.warns(Warning, match="batch_size"), pytest.raises((TypeError, ValueError)):
        RecordingBatcher(batch_size=value)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"max_batch_size": 0},
        {"max_batch_size": -2},
        {"max_batch_size": 1.5},
        {"max_batch_size": True},
        {"concurrency": 0},
        {"concurrency": -2},
        {"concurrency": 1.5},
        {"concurrency": True},
        {"max_queue_size": -2},
        {"max_queue_size": 1.5},
        {"max_queue_size": True},
        {"max_queue_time": -1},
        {"max_queue_time": float("nan")},
        {"max_queue_time": float("inf")},
        {"max_queue_time": None},
        {"max_queue_time": True},
    ],
)
def test_invalid_configuration_fails_before_starting(kwargs):
    with pytest.raises(ValueError):
        RecordingBatcher(**kwargs)


def test_unknown_options_are_not_silently_ignored():
    with pytest.raises(TypeError, match="max_batch_sze"):
        RecordingBatcher(max_batch_sze=10)
