from __future__ import annotations

import asyncio
from contextlib import aclosing

import pytest
from async_batcher import AsyncBatcher, GroupedBatcher, GroupLimitError, QueueFullException

from tests.core_helpers import wait_until


@pytest.mark.asyncio
@pytest.mark.parametrize("outcomes", [False, True])
@pytest.mark.parametrize("ordered", [False, True])
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_grouped_maps_route_inputs_through_bounded_child_queues(outcomes, ordered, asynchronous):
    batches = []

    def factory(key):
        async def process(items):
            assert all(item % 2 == key for item in items)
            batches.append(items)
            return [str(item) for item in items]

        return AsyncBatcher.from_callable(process, max_batch_size=3, max_queue_size=1, max_queue_time=0)

    async def source():
        for item in range(21):
            yield item

    async with GroupedBatcher(factory, group_key=lambda item: item % 2, max_groups=2) as grouped:
        method = grouped.map_outcomes if outcomes else grouped.map
        results = [
            value
            async for value in method(source() if asynchronous else range(21), max_pending=4, ordered=ordered)
        ]
        if outcomes:
            assert all(value.succeeded and value.item == value.index for value in results)
            results = [value.result for value in results]
        assert (
            results == [str(item) for item in range(21)]
            if ordered
            else sorted(map(int, results)) == list(range(21))
        )
        assert await grouped.process(42) == "42"
    assert sum(map(len, batches)) == 22
    assert grouped.group_count == 0


@pytest.mark.asyncio
async def test_waiting_admission_is_forwarded_but_timeout_does_not_cover_processing():
    release = asyncio.Event()
    started = asyncio.Event()
    children = []

    def factory(key):
        async def process(items):
            started.set()
            await release.wait()
            return items

        child = AsyncBatcher.from_callable(process, max_batch_size=1, max_queue_size=1)
        children.append(child)
        return child

    grouped = GroupedBatcher(factory, group_key=lambda item: 0)
    tasks = []
    try:
        tasks.append(asyncio.create_task(grouped.process(1, wait_for_space=True, enqueue_timeout=0)))
        await asyncio.wait_for(started.wait(), 2)
        tasks.append(asyncio.create_task(grouped.process(2)))
        await wait_until(lambda: children[0].stats().queued_items == 1)
        with pytest.raises(QueueFullException):
            await grouped.process(3)
        with pytest.raises(TimeoutError):
            await grouped.process(3, wait_for_space=True, enqueue_timeout=0)
        tasks.append(asyncio.create_task(grouped.process(4, wait_for_space=True)))
        await asyncio.sleep(0)
        assert not tasks[-1].done()
        assert children[0].stats().accepted == 2
        release.set()
        assert await asyncio.wait_for(asyncio.gather(*tasks), 2) == [1, 2, 4]
    finally:
        release.set()
        await grouped.stop(force=True)
        await asyncio.gather(*tasks, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("outcomes", [False, True])
async def test_group_limit_is_explicit_even_when_map_uses_waiting_admission(outcomes):
    release = asyncio.Event()
    started = asyncio.Event()

    def factory(key):
        async def process(items):
            started.set()
            await release.wait()
            return items

        return AsyncBatcher.from_callable(process, max_batch_size=1)

    grouped = GroupedBatcher(factory, group_key=lambda item: item, max_groups=1)
    busy = asyncio.create_task(grouped.process(1))
    try:
        await asyncio.wait_for(started.wait(), 2)
        with pytest.raises(GroupLimitError):
            await grouped.process(2, wait_for_space=True)
        if outcomes:
            values = [value async for value in grouped.map_outcomes([2])]
            assert values[0].index == 0 and values[0].item == 2
            assert isinstance(values[0].error, GroupLimitError)
        else:
            with pytest.raises(GroupLimitError):
                await anext(grouped.map([2]))
    finally:
        release.set()
        await busy
        await grouped.stop()


@pytest.mark.asyncio
async def test_cancelled_admission_waiter_remains_owned_until_child_completion():
    release = asyncio.Event()
    started = asyncio.Event()
    processed = []
    children = []

    def factory(key):
        async def process(items):
            started.set()
            await release.wait()
            processed.extend(items)
            return items

        child = AsyncBatcher.from_callable(process, max_batch_size=1, max_queue_size=1)
        children.append(child)
        return child

    grouped = GroupedBatcher(factory, group_key=lambda item: 0, max_groups=1)
    tasks = []
    try:
        tasks.append(asyncio.create_task(grouped.process(1)))
        await asyncio.wait_for(started.wait(), 2)
        tasks.append(asyncio.create_task(grouped.process(2)))
        await wait_until(lambda: children[0].stats().queued_items == 1)
        waiting = asyncio.create_task(grouped.process(3, wait_for_space=True))
        tasks.append(waiting)
        await wait_until(lambda: len(grouped._groups[0].pending) == 3)
        waiting.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiting
        release.set()
        await asyncio.gather(*tasks, return_exceptions=True)
        await wait_until(lambda: processed == [1, 2, 3])
    finally:
        release.set()
        await grouped.stop(force=True)
        await asyncio.gather(*tasks, return_exceptions=True)


@pytest.mark.asyncio
async def test_closing_grouped_map_closes_source_without_stopping_manager_or_unrelated_work():
    release = asyncio.Event()
    started = asyncio.Queue()
    closed = asyncio.Event()

    def factory(key):
        async def process(items):
            started.put_nowait(items)
            await release.wait()
            return items

        return AsyncBatcher.from_callable(process, max_batch_size=1, concurrency=-1)

    async def source():
        try:
            yield 0
            await asyncio.Event().wait()
        finally:
            closed.set()

    grouped = GroupedBatcher(factory, group_key=lambda item: 0)
    unrelated = asyncio.create_task(grouped.process(99))
    request = None
    try:
        async with aclosing(grouped.map_outcomes(source(), max_pending=2)) as stream:
            request = asyncio.create_task(anext(stream))
            await wait_until(lambda: started.qsize() == 2)
            request.cancel()
            with pytest.raises(asyncio.CancelledError):
                await request
        assert closed.is_set()
        assert not unrelated.done()
        release.set()
        assert await unrelated == 99
        assert await grouped.process(4) == 4
    finally:
        release.set()
        await grouped.stop(force=True)
        await asyncio.gather(*[task for task in (unrelated, request) if task], return_exceptions=True)
