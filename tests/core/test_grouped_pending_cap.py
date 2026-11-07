from __future__ import annotations

import asyncio
import inspect

import pytest
from async_batcher import AsyncBatcher, GroupedBatcher, GroupLimitError, QueueFullException

from tests.core_helpers import wait_until


@pytest.mark.parametrize("limit", [0, -1, True, 1.5, "3"])
def test_pending_limit_requires_positive_integer_or_none(limit):
    with pytest.raises(ValueError, match="max_pending_items"):
        GroupedBatcher(
            lambda key: AsyncBatcher.from_callable(lambda items: items),
            group_key=lambda item: item,
            max_pending_items=limit,
        )


@pytest.mark.asyncio
async def test_cap_spans_groups_and_cancelled_callers_keep_their_permits():
    release = asyncio.Event()
    started = asyncio.Queue()

    def factory(key):
        async def process(items):
            started.put_nowait(key)
            await release.wait()
            return items

        return AsyncBatcher.from_callable(process, max_batch_size=1)

    grouped = GroupedBatcher(factory, group_key=lambda item: item, max_pending_items=2)
    calls = [asyncio.create_task(grouped.process(item)) for item in (1, 2)]
    try:
        await wait_until(lambda: started.qsize() == 2)
        assert grouped.pending_items == 2
        calls[0].cancel()
        with pytest.raises(asyncio.CancelledError):
            await calls[0]
        assert grouped.pending_items == 2
        with pytest.raises(QueueFullException, match="aggregate"):
            await grouped.process(3, wait_for_space=True)
        assert grouped.group_count == 2
        release.set()
        assert await calls[1] == 2
        await wait_until(lambda: grouped.pending_items == 0)
        assert await grouped.process(3) == 3
    finally:
        release.set()
        await grouped.stop(force=True)
        await asyncio.gather(*calls, return_exceptions=True)
    assert grouped.pending_items == 0


@pytest.mark.asyncio
async def test_child_admission_waits_reserve_capacity_and_force_stop_releases_it():
    release = asyncio.Event()
    children = []

    def factory(key):
        async def process(items):
            await release.wait()
            return items

        child = AsyncBatcher.from_callable(process, max_batch_size=1, max_queue_size=1)
        children.append(child)
        return child

    grouped = GroupedBatcher(factory, group_key=lambda item: 0, max_pending_items=3)
    calls = []
    try:
        calls.append(asyncio.create_task(grouped.process(1)))
        await wait_until(lambda: children and children[0].stats().active_batches == 1)
        calls.append(asyncio.create_task(grouped.process(2)))
        await wait_until(lambda: children[0].stats().queued_items == 1)
        calls.append(asyncio.create_task(grouped.process(3, wait_for_space=True)))
        await wait_until(lambda: grouped.pending_items == 3)
        calls[-1].cancel()
        with pytest.raises(asyncio.CancelledError):
            await calls[-1]
        with pytest.raises(QueueFullException):
            await grouped.process(4, wait_for_space=True, enqueue_timeout=0)
        await asyncio.wait_for(grouped.stop(force=True), 2)
        await asyncio.gather(*calls, return_exceptions=True)
        assert grouped.pending_items == grouped.group_count == 0
    finally:
        release.set()
        await grouped.stop(force=True)
        await asyncio.gather(*calls, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["key", "unhashable", "factory", "group_limit"])
async def test_pre_child_failures_return_reservations_exactly_once(failure):
    release = asyncio.Event()

    def key(item):
        if item == -1 and failure == "key":
            raise ValueError("bad key")
        return [] if item == -1 and failure == "unhashable" else item

    def factory(group):
        if group == -1 and failure == "factory":
            raise ValueError("bad factory")

        async def process(items):
            await release.wait()
            return items

        return AsyncBatcher.from_callable(process, max_batch_size=1)

    grouped = GroupedBatcher(factory, group_key=key, max_groups=1, max_pending_items=2)
    first = None
    try:
        if failure == "group_limit":
            first = asyncio.create_task(grouped.process(1))
            await wait_until(lambda: grouped.pending_items == 1)
        expected = (
            GroupLimitError
            if failure == "group_limit"
            else TypeError
            if failure == "unhashable"
            else ValueError
        )
        for _ in range(3):
            with pytest.raises(expected):
                await grouped.process(-1)
            assert grouped.pending_items == (1 if first else 0)
        release.set()
        if first:
            await first
        assert await grouped.process(2) == 2
        assert grouped.pending_items == 0
    finally:
        release.set()
        await grouped.stop(force=True)
        if first:
            await asyncio.gather(first, return_exceptions=True)


@pytest.mark.asyncio
async def test_cancel_during_group_eviction_returns_pre_child_reservation():
    closing = asyncio.Event()
    release = asyncio.Event()

    class SlowStop(AsyncBatcher):
        async def process_batch(self, batch):
            return batch

        async def stop(self, *args, **kwargs):
            closing.set()
            await release.wait()
            await super().stop(*args, **kwargs)

    grouped = GroupedBatcher(
        lambda key: SlowStop(max_batch_size=1), group_key=lambda item: item, max_groups=1, max_pending_items=1
    )
    request = None
    try:
        assert await grouped.process(1) == 1
        request = asyncio.create_task(grouped.process(2))
        await asyncio.wait_for(closing.wait(), 2)
        assert grouped.pending_items == 1
        request.cancel()
        with pytest.raises(asyncio.CancelledError):
            await request
        assert grouped.pending_items == 0
        release.set()
        assert await grouped.process(3) == 3
    finally:
        release.set()
        await grouped.stop(force=True)
        if request:
            await asyncio.gather(request, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("eager", [False, True])
async def test_immediate_completion_and_callbacks_never_double_release(eager):
    if eager and not hasattr(asyncio, "eager_task_factory"):
        pytest.skip("Eager tasks require Python 3.12+")
    loop = asyncio.get_running_loop()
    old_factory = loop.get_task_factory()
    if eager:
        loop.set_task_factory(asyncio.eager_task_factory)

    class Immediate(AsyncBatcher):
        async def process(self, item, **kwargs):
            if item < 0:
                raise ValueError("negative")
            return item

        async def process_batch(self, batch):
            return batch

    grouped = GroupedBatcher(lambda key: Immediate(), group_key=lambda item: 0, max_pending_items=1)
    try:
        for value in range(10):
            assert await grouped.process(value) == value
            assert grouped.pending_items == 0
            with pytest.raises(ValueError):
                await grouped.process(-1)
            assert grouped.pending_items == 0
        await asyncio.sleep(0)
        assert grouped.pending_items == 0
    finally:
        await grouped.stop()
        loop.set_task_factory(old_factory)


@pytest.mark.asyncio
async def test_aggregate_rejection_is_an_indexed_stream_outcome():
    release = asyncio.Event()
    started = asyncio.Event()

    async def process(items):
        started.set()
        await release.wait()
        return items

    grouped = GroupedBatcher(
        lambda key: AsyncBatcher.from_callable(process, max_batch_size=1),
        group_key=lambda item: 0,
        max_pending_items=1,
    )
    busy = asyncio.create_task(grouped.process(0))
    try:
        await asyncio.wait_for(started.wait(), 2)
        outcomes = [value async for value in grouped.map_outcomes([1, 2])]
        assert [value.index for value in outcomes] == [0, 1]
        assert all(isinstance(value.error, QueueFullException) for value in outcomes)
        assert grouped.pending_items == 1
    finally:
        release.set()
        await busy
        await grouped.stop()


@pytest.mark.asyncio
async def test_rejecting_task_factory_closes_unstarted_child_and_returns_reservation():
    loop = asyncio.get_running_loop()
    previous = loop.get_task_factory()
    rejected = []

    def reject(loop, coroutine, **kwargs):
        rejected.append(coroutine)
        raise RuntimeError("task creation refused")

    grouped = GroupedBatcher(
        lambda key: AsyncBatcher.from_callable(lambda batch: batch, max_batch_size=1),
        group_key=lambda item: item,
        max_pending_items=1,
    )
    try:
        loop.set_task_factory(reject)
        with pytest.raises(RuntimeError, match="creation refused"):
            await grouped.process(1)
        assert grouped.pending_items == 0
        assert len(rejected) == 1
        assert inspect.getcoroutinestate(rejected[0]) == inspect.CORO_CLOSED
    finally:
        loop.set_task_factory(previous)
    try:
        assert await grouped.process(1) == 1
        assert grouped.pending_items == 0
    finally:
        await grouped.stop()
