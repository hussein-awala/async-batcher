from __future__ import annotations

import asyncio
from contextvars import Context, ContextVar

import pytest
from async_batcher import AsyncBatcher

from tests.core_helpers import wait_until

value = ContextVar("service_context_test", default="empty")


@pytest.mark.asyncio
@pytest.mark.parametrize("synchronous", [False, True])
@pytest.mark.parametrize("explicit_run", [False, True])
async def test_service_context_is_snapshotted_and_isolated(harness, synchronous, explicit_run):
    service = Context()
    service.run(value.set, "service")

    def process(items):
        inherited = value.get()
        value.set("worker mutation")
        return [inherited for _ in items]

    async def async_process(items):
        return process(items)

    batcher = AsyncBatcher.from_callable(
        process if synchronous else async_process, max_batch_size=1, processor_context=service
    )
    harness.batchers.append(batcher)
    service.run(value.set, "external mutation")
    if explicit_run:
        harness.spawn(batcher.run())
        await wait_until(lambda: batcher._is_running.is_set())
    for label in ("caller A", "caller B"):
        token = value.set(label)
        try:
            assert await batcher.process(1) == "service"
            assert value.get() == label
        finally:
            value.reset(token)
    await batcher.stop()
    assert await batcher.process(2) == "service"
    assert service.get(value) == "external mutation"


@pytest.mark.asyncio
@pytest.mark.parametrize("context", [None, Context()])
async def test_default_context_contract_and_empty_context(harness, context):
    async def process(items):
        return [value.get() for _ in items]

    batcher = AsyncBatcher.from_callable(process, max_batch_size=1, processor_context=context)
    harness.batchers.append(batcher)
    for caller in ("first", "second"):
        token = value.set(caller)
        try:
            assert await batcher.process(1) == ("first" if context is None else "empty")
        finally:
            value.reset(token)


@pytest.mark.asyncio
async def test_service_context_preserves_nested_shutdown_ancestry(harness):
    class Child(AsyncBatcher):
        async def process_batch(self, batch):
            await asyncio.create_task(parent.stop())
            return batch

    class Parent(AsyncBatcher):
        async def process_batch(self, batch):
            return [await child.process(item) for item in batch]

    child = harness.create(Child, max_batch_size=1, processor_context=Context())
    parent = harness.create(Parent, max_batch_size=1, processor_context=Context())
    with pytest.raises(RuntimeError, match="stop.*process_batch"):
        await asyncio.wait_for(parent.process(1), timeout=1)
    assert not parent._stop.is_set()


@pytest.mark.asyncio
async def test_service_context_preserves_thread_shutdown_guard(harness):
    loop = asyncio.get_running_loop()

    class Processor(AsyncBatcher):
        def process_batch(self, items):
            asyncio.run_coroutine_threadsafe(self.stop(), loop).result(timeout=2)
            return items

    batcher = harness.create(Processor, max_batch_size=1, processor_context=Context())
    with pytest.raises(RuntimeError, match="stop.*process_batch"):
        await asyncio.wait_for(batcher.process(1), timeout=3)
    assert not batcher._stop.is_set()


@pytest.mark.parametrize("context", [{}, False, object()])
def test_invalid_processor_context_is_rejected(context):
    with pytest.raises(TypeError, match="processor_context"):
        AsyncBatcher.from_callable(lambda items: items, processor_context=context)
