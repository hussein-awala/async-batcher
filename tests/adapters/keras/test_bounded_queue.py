from __future__ import annotations

import asyncio
from unittest.mock import MagicMock

import pytest
from async_batcher.exceptions import QueueFullException
from async_batcher.ml.keras.model import KerasAsyncBatcher


@pytest.mark.asyncio
async def test_keras_batcher_supports_bounded_queues():
    np = pytest.importorskip("numpy")
    model = MagicMock()
    model.predict.side_effect = lambda batch, **kwargs: np.asarray(batch) * 2
    batcher = KerasAsyncBatcher(model=model, max_queue_size=1, max_queue_time=0)
    try:
        results = await asyncio.wait_for(
            asyncio.gather(batcher.process(1), batcher.process(2), return_exceptions=True), timeout=2
        )
        assert results[0] == 2
        assert isinstance(results[1], QueueFullException)
        model.predict.assert_called_once()
    finally:
        await batcher.stop()
