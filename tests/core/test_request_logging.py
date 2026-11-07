from __future__ import annotations

import logging

import pytest

from tests.core_helpers import RecordingBatcher


@pytest.mark.asyncio
async def test_request_contents_are_not_logged(harness, caplog):
    caplog.set_level(logging.DEBUG)
    batcher = harness.create(RecordingBatcher, max_batch_size=1)
    assert await batcher.process("private-request-payload") == "private-request-payload" * 2
    await batcher.stop()
    assert "private-request-payload" not in caplog.text
