from __future__ import annotations

import asyncio
import threading

import pytest

np = pytest.importorskip("numpy")
pytest.importorskip("sklearn")

from async_batcher.ml.sklearn.model import SklearnAsyncBatcher  # noqa: E402
from sklearn.base import BaseEstimator  # noqa: E402


class RecordingEstimator(BaseEstimator):
    def __init__(self):
        self.batches = []
        self.threads = []

    def predict(self, samples):
        self.batches.append([sample.tolist() for sample in samples])
        self.threads.append(threading.get_ident())
        return np.asarray(samples).sum(axis=1)


@pytest.mark.asyncio
async def test_cost_limits_and_hooks_apply_to_actual_prediction_batches():
    model = RecordingEstimator()
    samples = [np.asarray(row) for row in ([2, 4], [1, 5], [2, 6], [3, 7])]
    evaluated = []
    events = []
    loop_thread = threading.get_ident()

    def item_cost(sample):
        evaluated.append(sample)
        return int(sample[0])

    def on_batch(event):
        assert threading.get_ident() == loop_thread
        events.append(event)

    async with SklearnAsyncBatcher(
        model=model,
        max_batch_size=3,
        max_queue_time=0,
        max_batch_cost=3,
        item_cost=item_cost,
        on_batch=on_batch,
    ) as batcher:
        results = await asyncio.wait_for(asyncio.gather(*(batcher.process(row) for row in samples)), 2)

    assert results == [6, 6, 8, 10]
    assert model.batches == [[[2, 4], [1, 5]], [[2, 6]], [[3, 7]]]
    assert len(evaluated) == len(samples)
    assert all(actual is original for actual, original in zip(evaluated, samples, strict=True))
    assert all(thread != loop_thread for thread in model.threads)
    assert [event.size for event in events] == [2, 1, 1]
    assert all(event.succeeded == event.size and event.failed == event.cancelled == 0 for event in events)
    assert batcher.stats().accepted == batcher.stats().succeeded == 4
    assert batcher.stats().pending_items == batcher.stats().active_batches == 0


@pytest.mark.asyncio
async def test_invalid_sample_cost_is_rejected_before_prediction_or_hook():
    model = RecordingEstimator()
    events = []
    async with SklearnAsyncBatcher(
        model=model, max_batch_cost=1, item_cost=lambda sample: 2, on_batch=events.append
    ) as batcher:
        with pytest.raises(ValueError, match="exceeds max_batch_cost"):
            await batcher.process(np.asarray([1, 2]))
    assert batcher.stats().accepted == 0
    assert model.batches == events == []


@pytest.mark.parametrize(
    "options, error, message",
    [
        ({"max_batch_cost": 0, "item_cost": lambda sample: 1}, ValueError, "max_batch_cost"),
        ({"max_batch_cost": 3, "item_cost": 2}, TypeError, "item_cost"),
        ({"on_batch": 2}, TypeError, "on_batch"),
        ({"unknown_option": True}, TypeError, "Unexpected arguments"),
    ],
)
def test_additional_options_use_core_validation(options, error, message):
    with pytest.raises(error, match=message):
        SklearnAsyncBatcher(model=RecordingEstimator(), **options)
