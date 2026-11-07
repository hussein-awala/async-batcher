from __future__ import annotations

import asyncio

import pytest

from tests.core_helpers import RecordingBatcher, wait_until


@pytest.mark.asyncio
@pytest.mark.parametrize("force", [False, True])
@pytest.mark.parametrize(
    "eager",
    [
        False,
        pytest.param(
            True,
            marks=pytest.mark.skipif(
                not hasattr(asyncio, "eager_task_factory"), reason="Requires Python 3.12+"
            ),
        ),
    ],
)
async def test_direct_run_shutdown_does_not_wait_for_its_callers_remaining_work(force, eager):
    batcher = RecordingBatcher(max_batch_size=1)
    run_returned = asyncio.Event()
    allow_caller_finish = asyncio.Event()

    async def caller():
        try:
            await batcher.run()
        except asyncio.CancelledError:
            if not force:
                raise
        run_returned.set()
        await allow_caller_finish.wait()

    loop = asyncio.get_running_loop()
    previous_factory = loop.get_task_factory()
    if eager:
        loop.set_task_factory(asyncio.eager_task_factory)
    calling = asyncio.create_task(caller())
    stopping = None
    try:
        await wait_until(lambda: batcher._is_running.is_set())
        stopping = asyncio.create_task(batcher.stop(force=force))
        await asyncio.wait_for(run_returned.wait(), timeout=1)
        assert not calling.done()
        await asyncio.wait_for(asyncio.shield(stopping), timeout=1)
        assert not calling.done()
        assert not await batcher.is_running()
        assert await asyncio.wait_for(batcher.process(3), timeout=1) == 6
    finally:
        allow_caller_finish.set()
        tasks = [calling]
        if stopping is not None:
            tasks.append(stopping)
        try:
            await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), timeout=2)
            await asyncio.wait_for(batcher.stop(force=True), timeout=1)
        finally:
            loop.set_task_factory(previous_factory)
