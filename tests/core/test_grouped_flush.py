from __future__ import annotations

import asyncio
import gc

import pytest
from async_batcher import AsyncBatcher, GroupedBatcher

from tests.core_helpers import wait_until


@pytest.mark.asyncio
@pytest.mark.parametrize("eager", [False, True])
async def test_flush_dispatches_each_partial_child_and_leaves_manager_open(eager):
    if eager and not hasattr(asyncio, "eager_task_factory"):
        pytest.skip("Eager tasks require Python 3.12+")
    loop = asyncio.get_running_loop()
    previous = loop.get_task_factory()
    children = {}
    events = []

    def factory(key):
        async def process(items):
            return [ValueError("bad item") if item[1] < 0 else item[1] for item in items]

        children[key] = AsyncBatcher.from_callable(
            process, max_batch_size=10, max_queue_time=3600, on_batch=events.append
        )
        return children[key]

    if eager:
        loop.set_task_factory(asyncio.eager_task_factory)
    grouped = GroupedBatcher(factory, group_key=lambda item: item[0])
    requests = [asyncio.create_task(grouped.process(item)) for item in (("a", 1), ("b", -1))]
    try:
        await wait_until(
            lambda: len(children) == 2 and all(child.stats().accepted for child in children.values())
        )
        await asyncio.wait_for(grouped.flush(), 1)
        results = await asyncio.gather(*requests, return_exceptions=True)
        assert results[0] == 1 and isinstance(results[1], ValueError)
        assert len(events) == 2
        assert all(child.stats().active_batches == 0 for child in children.values())
        later = asyncio.create_task(grouped.process(("a", 2)))
        requests.append(later)
        await wait_until(lambda: children["a"].stats().accepted == 2)
        await asyncio.wait_for(grouped.flush(), 1)
        assert await later == 2
    finally:
        await grouped.stop(force=True)
        await asyncio.gather(*requests, return_exceptions=True)
        loop.set_task_factory(previous)


@pytest.mark.asyncio
@pytest.mark.parametrize("abort_before_waiter", [False, True])
async def test_snapshot_precedes_child_waiter_scheduling_and_excludes_later_same_and_new_keys(
    abort_before_waiter,
):
    children = {}
    gates = {item: asyncio.Event() for item in (1, 2, 3)}
    captured = asyncio.Event()
    start_waiter = asyncio.Event()

    def factory(key):
        async def process(items):
            await gates[items[0][1]].wait()
            return items

        children[key] = AsyncBatcher.from_callable(process, max_batch_size=1, concurrency=3)
        return children[key]

    grouped = GroupedBatcher(factory, group_key=lambda item: item[0])
    requests = [asyncio.create_task(grouped.process(("a", 1)))]
    flushing = None
    try:
        await wait_until(lambda: children and children["a"].stats().active_batches == 1)
        flush_accepted = children["a"]._flush_accepted

        async def delay_waiter(target, generation):
            captured.set()
            await start_waiter.wait()
            await flush_accepted(target, generation)

        children["a"]._flush_accepted = delay_waiter
        flushing = asyncio.create_task(grouped.flush())
        await asyncio.wait_for(captured.wait(), 1)
        requests.extend(asyncio.create_task(grouped.process(item)) for item in (("a", 2), ("b", 3)))
        await wait_until(
            lambda: len(children) == 2 and sum(child.stats().accepted for child in children.values()) == 3
        )
        if abort_before_waiter:
            await children["a"].stop(force=True)
            start_waiter.set()
            with pytest.raises(RuntimeError, match="forced shutdown"):
                await asyncio.wait_for(flushing, 1)
            assert not requests[2].done()
        else:
            start_waiter.set()
            gates[1].set()
            await asyncio.wait_for(flushing, 1)
            assert await requests[0] == ("a", 1)
            assert all(not request.done() for request in requests[1:])
    finally:
        start_waiter.set()
        for gate in gates.values():
            gate.set()
        await grouped.stop(force=True)
        await asyncio.gather(*requests, *([flushing] if flushing else []), return_exceptions=True)


@pytest.mark.asyncio
async def test_flush_excludes_manager_reservation_waiting_for_child_admission():
    gates = {item: asyncio.Event() for item in (1, 2, 3)}

    async def process(items):
        await gates[items[0]].wait()
        return items

    child = AsyncBatcher.from_callable(process, max_batch_size=1, max_queue_size=1)
    grouped = GroupedBatcher(lambda key: child, group_key=lambda item: "a", max_pending_items=3)
    requests = [asyncio.create_task(grouped.process(1))]
    flushing = None
    try:
        await wait_until(lambda: child.stats().active_batches == 1)
        requests.append(asyncio.create_task(grouped.process(2)))
        await wait_until(lambda: child.stats().queued_items == 1)
        requests.append(asyncio.create_task(grouped.process(3, wait_for_space=True)))
        await wait_until(lambda: child.stats().admission_waiters == 1)
        assert grouped.pending_items == 3 and child.stats().accepted == 2
        flushing = asyncio.create_task(grouped.flush())
        await wait_until(lambda: bool(child._flush_targets))
        gates[1].set()
        gates[2].set()
        await asyncio.wait_for(flushing, 1)
        assert await asyncio.gather(*requests[:2]) == [1, 2]
        assert not requests[2].done()
    finally:
        for gate in gates.values():
            gate.set()
        await grouped.stop(force=True)
        await asyncio.gather(*requests, *([flushing] if flushing else []), return_exceptions=True)


@pytest.mark.asyncio
async def test_snapshot_registers_partial_batch_boundary_before_child_waiter_starts():
    captured = asyncio.Event()
    start_waiter = asyncio.Event()
    release_later = asyncio.Event()
    batches = []

    async def process(items):
        batches.append(items)
        if 2 in items:
            await release_later.wait()
        return items

    child = AsyncBatcher.from_callable(process, max_batch_size=10, max_queue_time=3600, concurrency=2)
    grouped = GroupedBatcher(lambda key: child, group_key=lambda item: "a")
    requests = [asyncio.create_task(grouped.process(1))]
    flushing = None
    try:
        await wait_until(lambda: child.stats().accepted == 1 and child.stats().queued_items == 0)
        flush_accepted = child._flush_accepted

        async def delay_waiter(target, generation):
            captured.set()
            await start_waiter.wait()
            await flush_accepted(target, generation)

        child._flush_accepted = delay_waiter
        flushing = asyncio.create_task(grouped.flush())
        await asyncio.wait_for(captured.wait(), 1)
        requests.append(asyncio.create_task(grouped.process(2)))
        await wait_until(lambda: child.stats().accepted == 2 and child.stats().queued_items == 0)
        start_waiter.set()
        await asyncio.wait_for(flushing, 1)
        assert batches == [[1]]
        assert await requests[0] == 1 and not requests[1].done()
    finally:
        start_waiter.set()
        release_later.set()
        await grouped.stop(force=True)
        await asyncio.gather(*requests, *([flushing] if flushing else []), return_exceptions=True)


@pytest.mark.asyncio
async def test_flush_excludes_eviction_waiter_without_releasing_retiring_group():
    closing = asyncio.Event()
    release_close = asyncio.Event()
    children = []

    class SlowStop(AsyncBatcher):
        async def process_batch(self, batch):
            return batch

        async def stop(self, *args, **kwargs):
            closing.set()
            await release_close.wait()
            await super().stop(*args, **kwargs)

    def factory(key):
        child = SlowStop(max_batch_size=1)
        children.append(child)
        return child

    grouped = GroupedBatcher(factory, group_key=str, max_groups=1)
    waiting = None
    try:
        assert await grouped.process(1) == 1
        waiting = asyncio.create_task(grouped.process(2))
        await asyncio.wait_for(closing.wait(), 1)
        assert grouped.pending_items == 1
        await asyncio.wait_for(grouped.flush(), 1)
        assert grouped.group_count == 1 and len(children) == 1 and not waiting.done()
        release_close.set()
        assert await asyncio.wait_for(waiting, 1) == 2
    finally:
        release_close.set()
        await grouped.stop(force=True)
        await asyncio.gather(*([waiting] if waiting else []), return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("ending", ["timeout", "cancel"])
async def test_abandoned_flush_wait_and_cancelled_caller_preserve_child_work(ending):
    release = asyncio.Event()
    events = []

    async def process(items):
        await release.wait()
        return items

    child = AsyncBatcher.from_callable(process, max_batch_size=1, on_batch=events.append)
    grouped = GroupedBatcher(lambda key: child, group_key=str)
    request = asyncio.create_task(grouped.process(1))
    flushing = None
    try:
        await wait_until(lambda: child.stats().active_batches == 1)
        request.cancel()
        await asyncio.gather(request, return_exceptions=True)
        flushing = asyncio.create_task(grouped.flush(timeout=0.02 if ending == "timeout" else None))
        await wait_until(lambda: bool(child._flush_targets))
        if ending == "cancel":
            flushing.cancel()
        with pytest.raises(TimeoutError if ending == "timeout" else asyncio.CancelledError):
            await flushing
        assert not child._flush_targets
        assert child.stats().pending_items == 1 and grouped.pending_items == 1
        release.set()
        await asyncio.wait_for(grouped.flush(), 1)
        assert len(events) == 1 and events[0].succeeded == 1
    finally:
        release.set()
        await grouped.stop(force=True)
        await asyncio.gather(request, *([flushing] if flushing else []), return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("force", [False, True])
async def test_flush_includes_owned_children_already_closing(force):
    release = asyncio.Event()

    async def process(items):
        await release.wait()
        return items

    child = AsyncBatcher.from_callable(process, max_batch_size=1)
    grouped = GroupedBatcher(lambda key: child, group_key=str)
    request = asyncio.create_task(grouped.process(1))
    stopping = flushing = None
    try:
        await wait_until(lambda: child.stats().active_batches == 1)
        stopping = asyncio.create_task(grouped.stop())
        await wait_until(lambda: grouped._groups["1"].closing)
        flushing = asyncio.create_task(grouped.flush())
        await wait_until(lambda: bool(child._flush_targets))
        assert not flushing.done()
        if force:
            await grouped.stop(force=True)
            with pytest.raises(RuntimeError, match="forced shutdown"):
                await flushing
        else:
            release.set()
            await asyncio.wait_for(flushing, 1)
            assert await request == 1
        await asyncio.wait_for(stopping, 1)
        assert grouped.group_count == 0
        await grouped.flush()
    finally:
        release.set()
        await grouped.stop(force=True)
        await asyncio.gather(
            request, *[task for task in (stopping, flushing) if task], return_exceptions=True
        )


@pytest.mark.asyncio
async def test_one_forced_child_interrupts_flush_without_cancelling_peer_work():
    children = {}
    release = asyncio.Event()

    def factory(key):
        async def process(items):
            await release.wait()
            return items

        children[key] = AsyncBatcher.from_callable(process, max_batch_size=1)
        return children[key]

    grouped = GroupedBatcher(factory, group_key=str)
    requests = [asyncio.create_task(grouped.process(item)) for item in (1, 2)]
    flushing = None
    try:
        await wait_until(
            lambda: len(children) == 2 and all(child.stats().active_batches for child in children.values())
        )
        flushing = asyncio.create_task(grouped.flush())
        await wait_until(lambda: all(child._flush_targets for child in children.values()))
        await children["1"].stop(force=True)
        with pytest.raises(RuntimeError, match="forced shutdown"):
            await asyncio.wait_for(flushing, 1)
        assert all(not child._flush_targets for child in children.values())
        assert not requests[1].done()
        release.set()
        assert await requests[1] == 2
    finally:
        release.set()
        await grouped.stop(force=True)
        await asyncio.gather(*requests, *([flushing] if flushing else []), return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("descendant", [False, True])
async def test_processor_cannot_flush_its_own_manager(descendant):
    async def process(items):
        if descendant:
            await asyncio.create_task(grouped.flush())
        else:
            await grouped.flush()
        return items

    grouped = GroupedBatcher(lambda key: AsyncBatcher.from_callable(process, max_batch_size=1), group_key=str)
    try:
        with pytest.raises(RuntimeError, match="outside the processor"):
            await asyncio.wait_for(grouped.process(1), 1)
    finally:
        await grouped.stop()


@pytest.mark.asyncio
async def test_snapshot_failure_releases_previously_registered_child_boundaries():
    children = {}
    release = asyncio.Event()

    def factory(key):
        async def process(items):
            if key == 0:
                await release.wait()
            else:
                await grouped.flush()
            return items

        children[key] = AsyncBatcher.from_callable(process, max_batch_size=1)
        return children[key]

    grouped = GroupedBatcher(factory, group_key=lambda item: item)
    peer = asyncio.create_task(grouped.process(0))
    try:
        await wait_until(lambda: children and children[0].stats().active_batches == 1)
        with pytest.raises(RuntimeError, match="outside the processor"):
            await asyncio.wait_for(grouped.process(1), 1)
        assert not children[0]._flush_targets and not peer.done()
        release.set()
        assert await peer == 0
    finally:
        release.set()
        await grouped.stop(force=True)
        await asyncio.gather(peer, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("timeout", [-1, True, float("inf"), float("nan"), "soon"])
async def test_invalid_timeout_is_rejected_before_starting_flush(timeout):
    grouped = GroupedBatcher(lambda key: None, group_key=str)
    with pytest.raises(ValueError, match="timeout"):
        await grouped.flush(timeout)
    assert grouped.group_count == 0


@pytest.mark.asyncio
async def test_empty_flush_binds_loop_without_closing_or_creating_children():
    grouped = GroupedBatcher(lambda key: None, group_key=str)
    await grouped.flush(timeout=0)
    assert grouped.group_count == 0 and not grouped._closed
    with pytest.raises(RuntimeError, match="same event loop"):
        await asyncio.to_thread(lambda: asyncio.run(grouped.flush()))
    await grouped.stop()
    await grouped.flush()


@pytest.mark.asyncio
async def test_refused_flush_waiter_creation_closes_coroutine_without_cancelling_work(recwarn):
    loop = asyncio.get_running_loop()
    previous = loop.get_task_factory()
    release = asyncio.Event()

    async def process(items):
        await release.wait()
        return items

    child = AsyncBatcher.from_callable(process, max_batch_size=1)
    grouped = GroupedBatcher(lambda key: child, group_key=str)
    request = asyncio.create_task(grouped.process(1))

    def refuse_waiter(loop, coroutine, **kwargs):
        if coroutine.__name__ == "_flush_accepted":
            raise RuntimeError("waiter creation refused")
        return asyncio.Task(coroutine, loop=loop, **kwargs)

    try:
        await wait_until(lambda: child.stats().active_batches == 1)
        loop.set_task_factory(refuse_waiter)
        with pytest.raises(RuntimeError, match="waiter creation refused"):
            await grouped.flush()
        loop.set_task_factory(previous)
        gc.collect()
        assert not [warning for warning in recwarn if issubclass(warning.category, RuntimeWarning)]
        assert not request.done() and not child._flush_targets
        release.set()
        await grouped.flush()
        assert await request == 1
    finally:
        loop.set_task_factory(previous)
        release.set()
        await grouped.stop(force=True)
        await asyncio.gather(request, return_exceptions=True)
