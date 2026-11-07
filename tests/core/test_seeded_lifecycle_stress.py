from __future__ import annotations

import asyncio
import random
from collections import Counter
from dataclasses import dataclass, field

import pytest
from async_batcher.batcher import AsyncBatcher
from async_batcher.exceptions import QueueFullException


@dataclass
class BatchControl:
    items: tuple[int, ...]
    release: asyncio.Event = field(default_factory=asyncio.Event)
    finished: bool = False


class ControlledBatcher(AsyncBatcher[int, tuple[str, int]]):
    def __init__(self, trace, **kwargs):
        super().__init__(**kwargs)
        self.trace = trace
        self.batches: list[BatchControl] = []
        self.active = 0
        self.peak_active = 0
        self.cleanup = asyncio.Event()
        self.cleanup.set()
        self.drain = False
        self.repeated_cancellation = 0

    async def process_batch(self, batch):
        control = BatchControl(tuple(batch))
        self.batches.append(control)
        self.active += 1
        self.peak_active = max(self.peak_active, self.active)
        self.trace.append(f"start batch {control.items}")
        cancelled_while_processing = False
        try:
            if not self.drain:
                await control.release.wait()
            await asyncio.sleep(0)
            return [("result", item) for item in batch]
        except asyncio.CancelledError:
            cancelled_while_processing = True
            raise
        finally:
            try:
                await self.cleanup.wait()
            except asyncio.CancelledError:
                # A first force cancellation may interrupt normal cleanup. Only
                # a second cancellation of cleanup already handling it is a bug.
                if cancelled_while_processing:
                    self.repeated_cancellation += 1
                raise
            finally:
                self.active -= 1
                control.finished = True
                self.trace.append(f"finish batch {control.items}")

    def release_all(self):
        self.drain = True
        self.cleanup.set()
        for control in self.batches:
            control.release.set()


async def schedule(turns):
    for _ in range(turns):
        await asyncio.sleep(0)


async def wait_for_active(batcher, count):
    async with asyncio.timeout(2):
        while batcher.active < count:
            await asyncio.sleep(0)


@pytest.mark.asyncio
@pytest.mark.parametrize("seed", [7, 31, 101, 65537])
@pytest.mark.parametrize("concurrency", [1, 2, -1], ids=["serial", "limited", "unlimited"])
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
    ids=["normal", "eager"],
)
async def test_seeded_lifecycle_interleavings_preserve_request_and_cleanup_invariants(
    seed, concurrency, eager
):
    rng = random.Random(seed)
    trace = []
    requests = {}
    callers_cancelled = set()
    shutdown_waiters = []
    cancelled_shutdown_waiters = set()
    next_item = 0
    batcher = ControlledBatcher(
        trace,
        max_batch_size=rng.choice([1, 2, 4]),
        max_queue_size=rng.choice([1, 3, 7]),
        max_queue_time=0,
        concurrency=concurrency,
    )
    loop = asyncio.get_running_loop()
    old_factory = loop.get_task_factory()
    original_tasks = asyncio.all_tasks()
    if eager:
        loop.set_task_factory(asyncio.eager_task_factory)

    def submit(count=1):
        nonlocal next_item
        for _ in range(count):
            item = next_item
            next_item += 1
            trace.append(f"submit {item}")
            requests[item] = asyncio.create_task(batcher.process(item))

    try:
        async with asyncio.timeout(15):
            for cycle in range(6):
                trace.append(f"cycle {cycle}")
                cycle_start = next_item
                batcher.drain = False
                # Saturate bounded concurrency before an overload burst. Gates,
                # rather than elapsed time, hold the processors in this state.
                for count in range(1, (concurrency if concurrency > 0 else 3) + 1):
                    submit()
                    await wait_for_active(batcher, count)
                submit(batcher._queue.maxsize + 2)
                await schedule(4)
                if concurrency > 0:
                    assert any(
                        task.done()
                        and not task.cancelled()
                        and isinstance(task.exception(), QueueFullException)
                        for item, task in requests.items()
                        if item >= cycle_start
                    )

                for _ in range(20):
                    action = rng.choice(["submit", "burst", "cancel", "release", "yield"])
                    trace.append(action)
                    if action in {"submit", "burst"}:
                        submit(1 if action == "submit" else rng.randint(2, 5))
                    elif action == "cancel":
                        pending = [(item, task) for item, task in requests.items() if not task.done()]
                        if pending:
                            item, task = rng.choice(pending)
                            trace.append(f"cancel request {item}")
                            callers_cancelled.add(item)
                            task.cancel()
                    elif action == "release":
                        held = [
                            control
                            for control in batcher.batches
                            if not control.release.is_set() and not control.finished
                        ]
                        if held:
                            control = rng.choice(held)
                            trace.append(f"release batch {control.items}")
                            control.release.set()
                    await schedule(rng.randrange(4))

                force = (cycle + seed) % 2 == 0
                trace.append(f"stop force={force}")
                # Keep cleanup suspended while shutdown waiters overlap. A
                # second force stop must not cancel a processor's finally block.
                batcher.cleanup.clear()
                stopping = asyncio.create_task(batcher.stop(force=force))
                shutdown_waiters.append(stopping)
                await schedule(4)
                if batcher.active:
                    with pytest.raises(RuntimeError, match="stopping"):
                        await asyncio.wait_for(batcher.process(-cycle - 1), timeout=2)
                    assert not stopping.done()
                if cycle % 3 == 0:
                    trace.append("cancel shutdown waiter")
                    if stopping.cancel():
                        cancelled_shutdown_waiters.add(stopping)
                    await asyncio.gather(stopping, return_exceptions=True)
                # This also joins cleanup after cancellation of a stop caller.
                joining = asyncio.create_task(batcher.stop(force=force))
                shutdown_waiters.append(joining)
                await schedule(rng.randint(1, 4))
                batcher.release_all()
                await asyncio.wait_for(asyncio.gather(*shutdown_waiters, return_exceptions=True), timeout=2)
                for waiter in shutdown_waiters:
                    if waiter.cancelled():
                        assert waiter in cancelled_shutdown_waiters
                    else:
                        assert waiter.result() is None
                assert not batcher.repeated_cancellation
                assert not batcher.active
                assert all(control.finished for control in batcher.batches)
                assert not await batcher.is_running()
                assert not batcher._running_batches
                assert not batcher._processing_tasks
                assert not batcher._pending_futures
                assert batcher._queue.empty()

                executions = Counter(item for control in batcher.batches for item in control.items)
                assert all(count == 1 for count in executions.values())
                for item, task in requests.items():
                    if item < cycle_start:
                        continue
                    assert task.done(), f"request {item} never settled"
                    if task.cancelled():
                        assert force or item in callers_cancelled
                    elif isinstance(task.exception(), QueueFullException):
                        assert item not in executions
                    else:
                        assert task.result() == ("result", item)
                        assert executions[item] == 1
                if concurrency > 0:
                    assert batcher.peak_active <= concurrency
                assert not (asyncio.all_tasks() - original_tasks)
    except BaseException as error:
        error.add_note(
            f"Replay seed={seed}, concurrency={concurrency}, eager={eager}, "
            f"max_batch_size={batcher.max_batch_size}, max_queue_size={batcher._queue.maxsize}\n"
            + "\n".join(trace)
        )
        raise
    finally:
        batcher.release_all()
        try:
            try:
                await asyncio.wait_for(batcher.stop(force=True), timeout=2)
            finally:
                tasks = [*requests.values(), *shutdown_waiters]
                for task in tasks:
                    if not task.done():
                        task.cancel()
                await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), timeout=2)
        finally:
            loop.set_task_factory(old_factory)
