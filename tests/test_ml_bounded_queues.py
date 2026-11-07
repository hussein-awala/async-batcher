from __future__ import annotations

import asyncio
import sys
from unittest.mock import MagicMock

import pytest
from async_batcher.exceptions import QueueFullException
from async_batcher.ml.keras.model import KerasAsyncBatcher
from async_batcher.ml.sklearn.model import SklearnAsyncBatcher


@pytest.mark.parametrize(
    "batcher_class",
    [
        pytest.param(
            KerasAsyncBatcher,
            marks=pytest.mark.skipif(
                sys.version_info < (3, 11), reason="Supported Keras requires Python 3.11+"
            ),
        ),
        SklearnAsyncBatcher,
    ],
)
@pytest.mark.asyncio
async def test_model_batchers_support_bounded_queues(batcher_class):
    model = MagicMock()
    if batcher_class is KerasAsyncBatcher:
        np = pytest.importorskip("numpy")
        model.predict.side_effect = lambda batch, **kwargs: np.asarray(batch) * 2
    else:
        model.predict.side_effect = lambda batch, **kwargs: [item * 2 for item in batch]
    batcher = batcher_class(model=model, max_queue_size=1, max_queue_time=0)
    try:
        results = await asyncio.wait_for(
            asyncio.gather(batcher.process(1), batcher.process(2), return_exceptions=True), timeout=2
        )
        assert results[0] == 2
        assert isinstance(results[1], QueueFullException)
        model.predict.assert_called_once()
    finally:
        await batcher.stop()
