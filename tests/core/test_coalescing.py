from __future__ import annotations

import asyncio
import inspect

import pytest
from async_batcher import AsyncBatcher, CoalescingBatcher, QueueFullException

from tests.core_helpers import GatedBatcher, wait_until


@pytest.mark.asyncio
@pytest.mark.parametrize("eager", [False, True])
async def test_equal_keys_share_first_input_and_exact_result_object_without_completed_cache(harness, eager):
    if eager and not hasattr(asyncio, "eager_task_factory"):
        pytest.skip("Eager tasks require Python 3.12+")
    loop = asyncio.get_running_loop()
    old_factory = loop.get_task_factory()
    if eager:
        loop.set_task_factory(asyncio.eager_task_factory)
    release = asyncio.Event()
    processed = []

    class Lookup(AsyncBatcher):
        async def process_batch(self, batch):
            await release.wait()
            processed.extend(batch)
            return [{"value": item[1]} for item in batch]

    batcher = harness.create(Lookup, max_batch_size=1, concurrency=-1)
    joined = CoalescingBatcher(batcher, key=lambda item: item[0], max_keys=2, max_waiters=4)
    try:
        calls = [harness.spawn(joined.process(item)) for item in (("a", 1), ("a", 2), ("b", 3))]
        await wait_until(lambda: joined.waiting_callers == 3)
        assert joined.in_flight_keys == 2
        release.set()
        results = await asyncio.wait_for(asyncio.gather(*calls), 2)
        assert results[0] is results[1]
        assert results == [{"value": 1}, {"value": 1}, {"value": 3}]
        assert sorted(processed) == [("a", 1), ("b", 3)]
        assert joined.in_flight_keys == joined.waiting_callers == 0
        assert await joined.process(("a", 4)) == {"value": 4}
        assert processed[-1] == ("a", 4)
    finally:
        release.set()
        await joined.aclose()
        loop.set_task_factory(old_factory)
    assert await batcher.process(("a", 5)) == {"value": 5}


@pytest.mark.asyncio
async def test_cancelled_waiter_does_not_cancel_peers_or_free_key_capacity(harness):
    batcher = harness.create(GatedBatcher, max_batch_size=1, concurrency=-1)
    joined = CoalescingBatcher(batcher, key=lambda item: item, max_keys=1, max_waiters=2)
    calls = [harness.spawn(joined.process(1)) for _ in range(2)]
    try:
        await wait_until(lambda: joined.waiting_callers == 2)
        with pytest.raises(QueueFullException, match="waiting-caller"):
            await joined.process(1)
        calls[0].cancel()
        with pytest.raises(asyncio.CancelledError):
            await calls[0]
        assert joined.waiting_callers == 1 and joined.in_flight_keys == 1
        with pytest.raises(QueueFullException, match="key limit"):
            await joined.process(2)
        last = harness.spawn(joined.process(1))
        calls[1].cancel()
        with pytest.raises(asyncio.CancelledError):
            await calls[1]
        await wait_until(lambda: joined.waiting_callers == 1)
        assert not last.done()
        batcher.release.set()
        assert await last == 2
        assert batcher.batches == [[1]]
    finally:
        batcher.release.set()
        await joined.aclose()


@pytest.mark.asyncio
async def test_all_detached_callers_keep_underlying_work_and_allow_later_joiner(harness):
    batcher = harness.create(GatedBatcher, max_batch_size=1)
    joined = CoalescingBatcher(batcher, key=lambda item: item, max_keys=1)
    caller = harness.spawn(joined.process(1))
    try:
        await asyncio.wait_for(batcher.started.get(), 2)
        caller.cancel()
        with pytest.raises(asyncio.CancelledError):
            await caller
        assert joined.waiting_callers == 0 and joined.in_flight_keys == 1
        with pytest.raises(QueueFullException):
            await joined.process(2)
        later = harness.spawn(joined.process(1))
        await wait_until(lambda: joined.waiting_callers == 1)
        batcher.release.set()
        assert await later == 2
        assert batcher.batches == [[1]]
    finally:
        batcher.release.set()
        await joined.aclose()


@pytest.mark.asyncio
async def test_shared_failure_fans_out_then_next_request_retries(harness):
    error = ValueError("lookup failed")
    batcher = harness.create(GatedBatcher, max_batch_size=1, error=error)
    joined = CoalescingBatcher(batcher, key=lambda item: item)
    try:
        calls = [harness.spawn(joined.process(1)) for _ in range(3)]
        await wait_until(lambda: joined.waiting_callers == 3)
        batcher.release.set()
        results = await asyncio.gather(*calls, return_exceptions=True)
        assert all(result is error for result in results)
        assert joined.in_flight_keys == 0
        batcher.error = None
        assert await joined.process(1) == 2
    finally:
        batcher.release.set()
        await joined.aclose()


@pytest.mark.asyncio
async def test_abandoned_failure_is_observed_and_aclose_does_not_rethrow_it(harness):
    errors = []
    loop = asyncio.get_running_loop()
    previous = loop.get_exception_handler()
    loop.set_exception_handler(lambda loop, context: errors.append(context))
    batcher = harness.create(GatedBatcher, max_batch_size=1, error=ValueError("abandoned"))
    joined = CoalescingBatcher(batcher, key=lambda item: item)
    caller = harness.spawn(joined.process(1))
    try:
        await asyncio.wait_for(batcher.started.get(), 2)
        caller.cancel()
        with pytest.raises(asyncio.CancelledError):
            await caller
        batcher.release.set()
        await joined.aclose()
        assert joined.in_flight_keys == joined.waiting_callers == 0
        await asyncio.sleep(0)
        assert errors == []
    finally:
        batcher.release.set()
        await joined.aclose()
        loop.set_exception_handler(previous)


@pytest.mark.asyncio
async def test_close_waiter_cancellation_preserves_drain_and_borrowed_batcher(harness):
    batcher = harness.create(GatedBatcher, max_batch_size=1)
    joined = CoalescingBatcher(batcher, key=lambda item: item)
    caller = harness.spawn(joined.process(1))
    closing = None
    try:
        await asyncio.wait_for(batcher.started.get(), 2)
        closing = harness.spawn(joined.aclose())
        await wait_until(lambda: joined._closed)
        closing.cancel()
        with pytest.raises(asyncio.CancelledError):
            await closing
        with pytest.raises(RuntimeError, match="closed"):
            await joined.process(2)
        assert not caller.done()
        batcher.release.set()
        await asyncio.wait_for(joined.aclose(), 2)
        assert await caller == 2
        assert await batcher.is_running()
        assert await batcher.process(3) == 6
    finally:
        batcher.release.set()
        await joined.aclose()


@pytest.mark.asyncio
async def test_context_closes_wrapper_after_body_error_but_not_borrowed_batcher(harness):
    batcher = harness.create(max_batch_size=1)
    joined = CoalescingBatcher(batcher, key=lambda item: item)
    with pytest.raises(ValueError, match="body"):
        async with joined as entered:
            assert entered is joined
            with pytest.raises(RuntimeError, match="already entered"):
                await joined.__aenter__()
            assert await joined.process(1) == 2
            raise ValueError("body")
    with pytest.raises(RuntimeError, match="closed"):
        await joined.__aenter__()
    assert await batcher.process(2) == 4
    await joined.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("descendant", [False, True])
async def test_processor_cannot_wait_for_its_own_wrapper_to_close(harness, descendant):
    joined = None

    class SelfClose(AsyncBatcher):
        async def process_batch(self, batch):
            if descendant:
                await asyncio.create_task(joined.aclose())
            else:
                await joined.aclose()
            return batch

    batcher = harness.create(SelfClose, max_batch_size=1)
    joined = CoalescingBatcher(batcher, key=lambda item: item)
    try:
        with pytest.raises(RuntimeError, match="coalesced processor"):
            await asyncio.wait_for(joined.process(1), 2)
        assert not joined._closed
    finally:
        await joined.aclose()


@pytest.mark.asyncio
async def test_underlying_forced_stop_cancels_joiners_and_releases_key(harness):
    batcher = harness.create(GatedBatcher, max_batch_size=1)
    joined = CoalescingBatcher(batcher, key=lambda item: item)
    calls = [harness.spawn(joined.process(1)) for _ in range(2)]
    try:
        await wait_until(lambda: joined.waiting_callers == 2)
        await asyncio.wait_for(batcher.started.get(), 2)
        await batcher.stop(force=True)
        results = await asyncio.wait_for(asyncio.gather(*calls, return_exceptions=True), 2)
        assert all(isinstance(value, asyncio.CancelledError) for value in results)
        assert joined.in_flight_keys == joined.waiting_callers == 0
    finally:
        await joined.aclose()


@pytest.mark.parametrize("argument", ["max_keys", "max_waiters"])
@pytest.mark.parametrize("value", [0, -1, True, 1.5, None])
def test_limits_require_positive_integers(argument, value):
    batcher = AsyncBatcher.from_callable(lambda batch: batch)
    with pytest.raises(ValueError, match=argument):
        CoalescingBatcher(batcher, key=lambda item: item, **{argument: value})


def test_constructor_rejects_non_batcher_or_asynchronous_key():
    batcher = AsyncBatcher.from_callable(lambda batch: batch)

    async def key(item):
        return item

    with pytest.raises(TypeError, match="batcher"):
        CoalescingBatcher(None, key=lambda item: item)
    with pytest.raises(TypeError, match="callable"):
        CoalescingBatcher(batcher, key=None)
    with pytest.raises(TypeError, match="synchronous"):
        CoalescingBatcher(batcher, key=key)


@pytest.mark.asyncio
async def test_invalid_keys_release_waiter_and_preserve_borrowed_awaitables(harness):
    batcher = harness.create(max_batch_size=1)
    coroutines = []
    activated = []

    class InvalidAwaitable:
        def __await__(self):
            activated.append(True)
            raise AssertionError("must not activate")

    async def coroutine():
        return 1

    borrowed = asyncio.get_running_loop().create_future()

    def key(item):
        if item == 0:
            raise ValueError("bad key")
        if item == 1:
            return []
        if item == 2:
            created = coroutine()
            coroutines.append(created)
            return created
        if item == 3:
            return InvalidAwaitable()
        if item == 4:
            return borrowed
        return item

    joined = CoalescingBatcher(batcher, key=key, max_waiters=1)
    try:
        for item in range(5):
            with pytest.raises(ValueError if item == 0 else TypeError):
                await joined.process(item)
            assert joined.waiting_callers == joined.in_flight_keys == 0
        assert inspect.getcoroutinestate(coroutines[0]) == inspect.CORO_CLOSED
        assert not borrowed.done() and activated == []
        assert await joined.process(5) == 10
    finally:
        borrowed.cancel()
        await joined.aclose()


def test_wrapper_cannot_move_event_loops():
    batcher = AsyncBatcher.from_callable(lambda batch: batch)
    joined = CoalescingBatcher(batcher, key=lambda item: item)

    async def first():
        assert await joined.process(1) == 1
        await joined.aclose()
        await batcher.stop()

    asyncio.run(first())
    with pytest.raises(RuntimeError, match="same event loop"):
        asyncio.run(joined.aclose())
