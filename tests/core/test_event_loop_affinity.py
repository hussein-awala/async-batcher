from __future__ import annotations

import asyncio

import pytest

from tests.core_helpers import RecordingBatcher


@pytest.mark.parametrize("operation", ["process", "run", "stop", "force_stop"])
def test_foreign_loop_operation_is_rejected_without_mutating_owner(operation):
    batcher = RecordingBatcher(max_batch_size=1)

    async def reject_foreign_operation():
        if operation == "process":
            pending = batcher.process(2)
        elif operation == "run":
            pending = batcher.run()
        else:
            pending = batcher.stop(force=operation == "force_stop")
        with pytest.raises(RuntimeError, match="same event loop"):
            await asyncio.wait_for(pending, timeout=1)

    with asyncio.Runner() as owner, asyncio.Runner() as foreign:
        try:
            assert owner.run(batcher.process(1)) == 2
            dispatcher = batcher._current_task
            foreign.run(reject_foreign_operation())
            assert batcher._current_task is dispatcher
            assert not dispatcher.cancelling()
            assert not batcher._stop.is_set()
            assert not batcher._force_stopped
            assert batcher._queue.empty()
            assert not batcher._pending_futures
            assert owner.run(batcher.process(3)) == 6
            assert batcher.batches == [[1], [3]]
        finally:
            owner.run(batcher.stop(force=True))


def test_foreign_loop_cannot_restart_a_stopped_batcher():
    batcher = RecordingBatcher(max_batch_size=1)

    async def reject_foreign_restart():
        with pytest.raises(RuntimeError, match="same event loop"):
            await asyncio.wait_for(batcher.process(2), timeout=1)

    with asyncio.Runner() as owner, asyncio.Runner() as foreign:
        try:
            assert owner.run(batcher.process(1)) == 2
            owner.run(batcher.stop())
            foreign.run(reject_foreign_restart())
            assert batcher._stop.is_set()
            assert batcher._queue.empty()
            assert not batcher._pending_futures
            assert owner.run(batcher.process(3)) == 6
            assert batcher.batches == [[1], [3]]
        finally:
            if batcher._current_task is None or batcher._current_task.get_loop() is owner.get_loop():
                owner.run(batcher.stop(force=True))
