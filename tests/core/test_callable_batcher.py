from __future__ import annotations

import asyncio
import threading
from concurrent.futures import ThreadPoolExecutor
from contextvars import ContextVar
from functools import partial

import pytest
from async_batcher import AsyncBatcher

from tests.core_helpers import GatedBatcher, wait_until


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["function", "partial", "object", "method"])
async def test_async_processors_accept_positional_input_and_preserve_results(kind):
    batches = []
    event_loop_thread = threading.get_ident()

    async def multiply(items, /, factor=2):
        assert threading.get_ident() == event_loop_thread
        batches.append(items)
        return tuple(value * factor for value in items)

    class Processor:
        async def __call__(self, items):
            return await multiply(items)

    processor = {
        "function": multiply,
        "partial": partial(multiply, factor=2),
        "object": Processor(),
        "method": Processor().__call__,
    }[kind]
    async with AsyncBatcher.from_callable(processor, max_batch_size=3, max_queue_time=1) as batcher:
        assert await asyncio.wait_for(asyncio.gather(*(batcher.process(i) for i in range(6))), 2) == [
            0,
            2,
            4,
            6,
            8,
            10,
        ]
    assert batches == [[0, 1, 2], [3, 4, 5]]
    assert not await batcher.is_running()


@pytest.mark.asyncio
async def test_sync_processor_uses_owned_executor_and_copied_context():
    context = ContextVar("callable_test", default="missing")
    context.set("caller")
    event_loop_thread = threading.get_ident()
    threads = []

    def multiply(items, /):
        threads.append(threading.get_ident())
        assert context.get() == "caller"
        context.set("worker")
        return [item * 2 for item in items]

    with ThreadPoolExecutor(max_workers=1) as executor:
        async with AsyncBatcher.from_callable(multiply, max_batch_size=1, executor=executor) as batcher:
            assert await batcher.process(3) == 6
        assert context.get() == "caller"
        assert threads[0] != event_loop_thread
        assert executor.submit(lambda: 42).result() == 42


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_none_and_per_item_errors_follow_existing_contract(asynchronous):
    def compute(items):
        return None if items == [0] else [ValueError("bad item"), 4]

    async def async_compute(items):
        return compute(items)

    async with AsyncBatcher.from_callable(async_compute if asynchronous else compute, max_queue_time=0) as b:
        assert await b.process(0) is None
        results = await asyncio.gather(b.process(1), b.process(2), return_exceptions=True)
        assert isinstance(results[0], ValueError)
        assert results[1] == 4


def test_invalid_callable_and_options_fail_at_construction():
    with pytest.raises(TypeError, match="must be callable"):
        AsyncBatcher.from_callable(None)
    with pytest.raises(ValueError, match="max_batch_size"):
        AsyncBatcher.from_callable(lambda items: items, max_batch_size=0)
    with pytest.raises(TypeError, match="Unexpected arguments"):
        AsyncBatcher.from_callable(lambda items: items, typo=True)


@pytest.mark.parametrize("callable_object", [False, True])
def test_async_generators_rejected(callable_object):
    async def generate(items):
        yield items

    class Processor:
        __call__ = staticmethod(generate)

    with pytest.raises(TypeError, match="async generator"):
        AsyncBatcher.from_callable(Processor() if callable_object else generate)


@pytest.mark.asyncio
async def test_regular_function_returning_awaitable_is_awaited_on_the_event_loop():
    event_loop_thread = threading.get_ident()

    async def compute(items):
        assert threading.get_ident() == event_loop_thread
        return items

    class Wrapped:
        def __init__(self, coroutine):
            self.coroutine = coroutine

        def __await__(self):
            return self.coroutine.__await__()

    for factory in (compute, lambda items: Wrapped(compute(items))):
        async with AsyncBatcher.from_callable(
            lambda items, factory=factory: factory(items), max_queue_time=0
        ) as batcher:
            assert await batcher.process(1) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("body_error", [ValueError("body failed"), asyncio.CancelledError()])
async def test_context_exit_drains_accepted_work_before_propagating_body_error(body_error, harness):
    batcher = harness.create(max_batch_size=10, max_queue_time=60)
    with pytest.raises(type(body_error)):
        async with batcher as entered:
            assert entered is batcher
            request = harness.submit(batcher, 4)[0]
            await wait_until(lambda: bool(batcher._pending_futures))
            raise body_error
    assert await request == 8
    assert not await batcher.is_running()
    assert not batcher._running_batches


@pytest.mark.asyncio
async def test_context_rejects_nested_entry_and_can_be_reused(harness):
    batcher = harness.create(max_batch_size=1)
    async with batcher:
        with pytest.raises(RuntimeError, match="already entered"):
            async with batcher:
                pytest.fail("nested context entered")
        assert await batcher.process(2) == 4
    async with batcher:
        assert await batcher.process(3) == 6


@pytest.mark.asyncio
async def test_enter_while_stopping_is_rejected(harness):
    batcher = harness.create(GatedBatcher, max_batch_size=1)
    request = harness.submit(batcher, 3)[0]
    await asyncio.wait_for(batcher.started.get(), 1)
    stopping = harness.spawn(batcher.stop())
    await wait_until(batcher._stop.is_set)
    with pytest.raises(RuntimeError, match="stopping"):
        async with batcher:
            pytest.fail("entered while stopping")
    batcher.release.set()
    await asyncio.wait_for(stopping, 1)
    assert await request == 6


@pytest.mark.asyncio
async def test_context_with_no_submissions_does_not_start_dispatcher(harness):
    batcher = harness.create()
    async with batcher:
        assert not await batcher.is_running()
    assert batcher._current_task is None
