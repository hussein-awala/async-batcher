from __future__ import annotations

import asyncio

import pytest
from async_batcher import AsyncBatcher, GroupedBatcher, GroupLimitError

from tests.core_helpers import wait_until


def echo_factory(key):
    return AsyncBatcher.from_callable(lambda items: items, max_queue_time=0)


@pytest.mark.asyncio
async def test_groups_separate_compatible_requests_and_route_results():
    batches = []
    children = []

    def factory(key):
        async def process(items):
            assert all(item[0] == key for item in items)
            batches.append((key, items))
            return [item[1] * 2 for item in items]

        child = AsyncBatcher.from_callable(process, max_batch_size=2, max_queue_time=1)
        children.append(child)
        return child

    async with GroupedBatcher(factory, group_key=lambda item: item[0]) as grouped:
        values = [("a", 1), ("b", 2), ("a", 3), ("b", 4)]
        assert await asyncio.wait_for(asyncio.gather(*(grouped.process(item) for item in values)), 2) == [
            2,
            4,
            6,
            8,
        ]
        assert grouped.group_count == 2
        assert len(batches) == 2
    assert grouped.group_count == 0
    assert not any(await asyncio.gather(*(child.is_running() for child in children)))


@pytest.mark.asyncio
async def test_busy_groups_respect_cap_and_idle_slot_is_reused():
    release = asyncio.Event()
    started = asyncio.Event()
    children = []

    def factory(key):
        async def process(items):
            started.set()
            await release.wait()
            return items

        child = AsyncBatcher.from_callable(process, max_queue_time=0)
        children.append(child)
        return child

    async with GroupedBatcher(factory, group_key=str, max_groups=1, idle_timeout=60) as grouped:
        first = asyncio.create_task(grouped.process(1))
        await asyncio.wait_for(started.wait(), 1)
        with pytest.raises(GroupLimitError):
            await grouped.process(2)
        assert len(children) == 1
        release.set()
        assert await first == 1
        await asyncio.sleep(0)
        assert await grouped.process(2) == 2
        assert len(children) == 2
        assert grouped.group_count == 1
        assert not await children[0].is_running()


@pytest.mark.asyncio
async def test_idle_timeout_stops_and_removes_group():
    grouped = GroupedBatcher(echo_factory, group_key=str, idle_timeout=0)
    assert await grouped.process(1) == 1
    await asyncio.wait_for(wait_until(lambda: grouped.group_count == 0), 1)
    await grouped.stop()


@pytest.mark.asyncio
async def test_reused_group_cancels_previous_idle_timer():
    created = []

    def factory(key):
        child = echo_factory(key)
        created.append(child)
        return child

    async with GroupedBatcher(factory, group_key=str, idle_timeout=60) as grouped:
        assert await grouped.process(1) == 1
        assert await grouped.process(1) == 1
        assert len(created) == 1


@pytest.mark.asyncio
async def test_cancelled_caller_keeps_group_busy_until_accepted_work_finishes():
    started = asyncio.Event()
    release = asyncio.Event()
    finished = []

    async def compute(items):
        started.set()
        await release.wait()
        finished.extend(items)
        return items

    grouped = GroupedBatcher(
        lambda key: AsyncBatcher.from_callable(compute, max_queue_time=0), group_key=str, max_groups=1
    )
    caller = asyncio.create_task(grouped.process(1))
    await asyncio.wait_for(started.wait(), 1)
    caller.cancel()
    with pytest.raises(asyncio.CancelledError):
        await caller
    with pytest.raises(GroupLimitError):
        await grouped.process(2)
    release.set()
    await asyncio.wait_for(grouped.stop(), 1)
    assert finished == [1]
    assert grouped.group_count == 0


@pytest.mark.asyncio
async def test_shutdown_timeout_then_force_joins_cleanup():
    started = asyncio.Event()
    cleanup_started = asyncio.Event()
    cleanup_release = asyncio.Event()

    async def compute(items):
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            cleanup_started.set()
            await cleanup_release.wait()

    grouped = GroupedBatcher(lambda key: AsyncBatcher.from_callable(compute, max_queue_time=0), group_key=str)
    caller = asyncio.create_task(grouped.process(1))
    await asyncio.wait_for(started.wait(), 1)
    with pytest.raises(TimeoutError):
        await grouped.stop(timeout=0)
    with pytest.raises(RuntimeError, match="closed"):
        await grouped.process(2)
    stopping = asyncio.create_task(grouped.stop(force=True))
    await asyncio.wait_for(cleanup_started.wait(), 1)
    assert not stopping.done()
    cleanup_release.set()
    await asyncio.wait_for(stopping, 1)
    with pytest.raises(asyncio.CancelledError):
        await caller
    assert grouped.group_count == 0
    await grouped.stop(force=True)


@pytest.mark.asyncio
async def test_cancelled_shutdown_waiter_preserves_drain():
    started = asyncio.Event()
    release = asyncio.Event()

    async def compute(items):
        started.set()
        await release.wait()
        return items

    grouped = GroupedBatcher(lambda key: AsyncBatcher.from_callable(compute, max_queue_time=0), group_key=str)
    caller = asyncio.create_task(grouped.process(1))
    await asyncio.wait_for(started.wait(), 1)
    stopping = asyncio.create_task(grouped.stop())
    await asyncio.sleep(0)
    stopping.cancel()
    with pytest.raises(asyncio.CancelledError):
        await stopping
    release.set()
    await asyncio.wait_for(grouped.stop(), 1)
    assert await caller == 1


@pytest.mark.asyncio
async def test_processor_cannot_stop_its_own_group_manager():
    async def compute(items):
        await grouped.stop()
        return items

    async with GroupedBatcher(
        lambda key: AsyncBatcher.from_callable(compute, max_queue_time=0), group_key=str
    ) as grouped:
        with pytest.raises(RuntimeError, match="grouped processor"):
            await grouped.process(1)


@pytest.mark.asyncio
async def test_context_errors_drain_and_close_without_suppressing():
    grouped = GroupedBatcher(echo_factory, group_key=str)
    with pytest.raises(ValueError, match="body"):
        async with grouped:
            with pytest.raises(RuntimeError, match="already entered"):
                async with grouped:
                    pytest.fail("nested entry")
            assert await grouped.process(1) == 1
            raise ValueError("body")
    with pytest.raises(RuntimeError, match="closed"):
        async with grouped:
            pytest.fail("closed entry")


@pytest.mark.parametrize(
    "options",
    [
        {"max_groups": 0},
        {"max_groups": True},
        {"max_groups": 1.5},
        {"idle_timeout": -1},
        {"idle_timeout": True},
        {"idle_timeout": "1"},
        {"idle_timeout": float("nan")},
        {"idle_timeout": float("inf")},
        {"idle_timeout": 10**1000},
    ],
)
def test_invalid_limits(options):
    with pytest.raises(ValueError):
        GroupedBatcher(echo_factory, group_key=str, **options)


def test_invalid_callbacks():
    with pytest.raises(TypeError, match="callable"):
        GroupedBatcher(None, group_key=str)
    with pytest.raises(TypeError, match="callable"):
        GroupedBatcher(echo_factory, group_key=None)

    async def asynchronous(value):
        return value

    with pytest.raises(TypeError, match="synchronous"):
        GroupedBatcher(asynchronous, group_key=str)
    with pytest.raises(TypeError, match="synchronous"):
        GroupedBatcher(echo_factory, group_key=asynchronous)


@pytest.mark.asyncio
async def test_invalid_keys_and_factory_results_do_not_allocate_slots():
    async def asynchronous(value):
        return value

    for key in (lambda item: [], lambda item: asynchronous(item)):
        async with GroupedBatcher(echo_factory, group_key=key) as grouped:
            with pytest.raises(TypeError):
                await grouped.process(1)
            assert grouped.group_count == 0
    for factory in (lambda key: None, lambda key: asynchronous(key)):
        async with GroupedBatcher(factory, group_key=str) as grouped:
            with pytest.raises(TypeError, match="unused AsyncBatcher"):
                await grouped.process(1)
            assert grouped.group_count == 0


@pytest.mark.asyncio
async def test_factory_must_return_a_new_unused_child():
    child = echo_factory("a")
    async with GroupedBatcher(lambda key: child, group_key=str) as grouped:
        assert await grouped.process(1) == 1
        with pytest.raises(ValueError, match="fresh unused"):
            await grouped.process(2)


def test_group_manager_rejects_foreign_event_loop():
    grouped = GroupedBatcher(echo_factory, group_key=str)
    asyncio.run(grouped.stop())
    with pytest.raises(RuntimeError, match="same event loop"):
        asyncio.run(grouped.process(1))


@pytest.mark.asyncio
async def test_eager_task_factory_keeps_group_ownership_consistent():
    factory = getattr(asyncio, "eager_task_factory", None)
    if factory is None:
        pytest.skip("eager tasks require Python 3.12+")
    loop = asyncio.get_running_loop()
    previous = loop.get_task_factory()
    loop.set_task_factory(factory)
    try:
        async with GroupedBatcher(echo_factory, group_key=str, idle_timeout=0, max_groups=2) as grouped:
            for i in range(20):
                assert await grouped.process(i) == i
        assert grouped.group_count == 0
    finally:
        loop.set_task_factory(previous)


@pytest.mark.asyncio
async def test_same_key_waits_for_idle_cleanup_before_recreating_group():
    closing = asyncio.Event()
    release = asyncio.Event()
    children = []

    class SlowStop(AsyncBatcher):
        async def process_batch(self, batch):
            return batch

        async def stop(self, *args, **kwargs):
            closing.set()
            await release.wait()
            await super().stop(*args, **kwargs)

    def factory(key):
        child = SlowStop(max_queue_time=0)
        children.append(child)
        return child

    grouped = GroupedBatcher(factory, group_key=str, idle_timeout=0, max_groups=1)
    assert await grouped.process(1) == 1
    await asyncio.wait_for(closing.wait(), 1)
    waiting = asyncio.create_task(grouped.process(1))
    await asyncio.sleep(0)
    assert not waiting.done()
    assert grouped.group_count == 1
    release.set()
    assert await asyncio.wait_for(waiting, 1) == 1
    assert len(children) == 2
    await grouped.stop()


@pytest.mark.asyncio
async def test_cancel_waiting_for_eviction_does_not_cancel_cleanup():
    closing = asyncio.Event()
    release = asyncio.Event()

    class SlowStop(AsyncBatcher):
        async def process_batch(self, batch):
            return batch

        async def stop(self, *args, **kwargs):
            closing.set()
            await release.wait()
            await super().stop(*args, **kwargs)

    grouped = GroupedBatcher(lambda key: SlowStop(max_queue_time=0), group_key=str, max_groups=1)
    assert await grouped.process(1) == 1
    waiting = asyncio.create_task(grouped.process(2))
    await asyncio.wait_for(closing.wait(), 1)
    waiting.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiting
    release.set()
    await asyncio.wait_for(grouped.stop(), 1)
    assert grouped.group_count == 0


@pytest.mark.asyncio
async def test_failed_eviction_retains_group_capacity_until_cleanup_succeeds():
    children = []
    fail_cleanup = True

    class FailingStop(AsyncBatcher):
        async def process_batch(self, batch):
            return batch

        async def stop(self, *args, **kwargs):
            if fail_cleanup:
                raise ValueError("cleanup failed")
            await super().stop(*args, **kwargs)

    def factory(key):
        child = FailingStop(max_queue_time=0)
        children.append(child)
        return child

    grouped = GroupedBatcher(factory, group_key=str, max_groups=1)
    assert await grouped.process(1) == 1
    for key in (2, 3):
        with pytest.raises(ValueError, match="cleanup failed"):
            await grouped.process(key)
        assert grouped.group_count == 1
        assert len(children) == 1
    with pytest.raises(ValueError, match="cleanup failed"):
        await grouped.stop()
    assert grouped.group_count == 1
    fail_cleanup = False
    await grouped.stop(force=True)
    assert grouped.group_count == 0
    assert not await children[0].is_running()
