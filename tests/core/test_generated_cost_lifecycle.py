"""Replayable, gated traces with an independent public-outcome model."""

from __future__ import annotations

import asyncio
import json
import random
from collections import Counter
from dataclasses import asdict, dataclass

import pytest
from async_batcher import AsyncBatcher, QueueFullException

from tests.core_helpers import wait_until


@dataclass(frozen=True)
class Cycle:
    number: int
    costs: tuple[int, int, int, int]
    outcomes: tuple[str, str, str, str]
    cancel: int
    force: bool


@dataclass(frozen=True)
class Item:
    key: tuple[int, int]
    cost: int
    outcome: str


def generate(seed):
    rng = random.Random(seed)
    # The first cycle guarantees three different outcomes in one native batch.
    cycles = [Cycle(0, (4, 3, 0, 2), ("ok", "ok", "error", "cancel"), 0, False)]
    for number in range(1, 7):
        cycles.append(
            Cycle(
                number,
                (4, 3, rng.randrange(6), rng.randrange(6)),
                tuple(rng.choice(("ok", "error", "cancel")) for _ in range(4)),
                rng.randrange(3),
                bool(number % 2),
            )
        )
    return cycles


async def reduce_cycles(cycles, still_fails):
    """Delete whole independent stop/restart cycles, keeping the same failure.

    At most len(cycles) replays are attempted. This intentionally does not model
    or simplify internal scheduling, costs, outcomes, or individual operations.
    """
    reduced = list(cycles)
    for cycle in cycles:
        candidate = [other for other in reduced if other.number != cycle.number]
        if candidate and await still_fails(candidate):
            reduced = candidate
    return reduced


async def replay(cycles, eager=False):
    loop = asyncio.get_running_loop()
    old_factory = loop.get_task_factory()
    initial_tasks = asyncio.all_tasks()
    if eager:
        loop.set_task_factory(asyncio.eager_task_factory)
    tasks = []
    events = []
    executed = []
    evaluated = Counter()
    mutable_costs = {}
    expected = {}
    rejected = 0
    released = asyncio.Event()

    def cost(item):
        evaluated[item.key] += 1
        return mutable_costs[item.key]

    async def processor(batch):
        executed.append(tuple(batch))
        await released.wait()
        return [
            ValueError(item.key)
            if item.outcome == "error"
            else asyncio.CancelledError()
            if item.outcome == "cancel"
            else item.key
            for item in batch
        ]

    batcher = AsyncBatcher.from_callable(
        processor,
        max_batch_size=3,
        max_batch_cost=5,
        item_cost=cost,
        max_queue_size=2,
        max_queue_time=60,
        concurrency=1,
        on_batch=events.append,
        log_exceptions=False,
    )

    def submit(item, **options):
        mutable_costs[item.key] = item.cost
        task = asyncio.create_task(batcher.process(item, **options))
        tasks.append(task)
        return task

    def check_snapshot():
        stats = batcher.stats()
        assert stats.accepted == stats.succeeded + stats.failed + stats.cancelled + stats.pending_items
        assert 0 <= stats.queued_items <= 2
        assert 0 <= stats.active_batches <= 1

    try:
        async with asyncio.timeout(10):
            for cycle in cycles:
                released.clear()
                items = [
                    Item((cycle.number, index), amount, outcome)
                    for index, (amount, outcome) in enumerate(zip(cycle.costs, cycle.outcomes, strict=True))
                ]
                accepted_before = len(expected)
                executed_before = len(executed)
                first = submit(items[0])
                await wait_until(lambda before=accepted_before: batcher.stats().accepted == before + 1)
                # Let the collector take the head without depending on its private queue.
                await wait_until(lambda: batcher.stats().queued_items == 0)
                second = submit(items[1])
                await wait_until(lambda before=executed_before: len(executed) == before + 1)
                assert executed[-1] == (items[0],)
                assert batcher.stats().queued_items == 1  # retained cost-overflow head
                third = submit(items[2])
                await wait_until(lambda: batcher.stats().queued_items == 2)
                accepted_tasks = [first, second, third]
                for item in items[:3]:
                    expected[item.key] = (item, "cancelled" if cycle.force else item.outcome)
                check_snapshot()

                for index, options, error in (
                    (4, {}, QueueFullException),
                    (5, {"wait_for_space": True, "enqueue_timeout": 0}, TimeoutError),
                    (6, {"wait_for_space": True}, ValueError),
                ):
                    probe = Item((cycle.number, index), 6 if index == 6 else 1, "ok")
                    with pytest.raises(error):
                        await submit(probe, **options)
                rejected += 2  # invalid costs are validation failures, not admission refusals

                cancelled_waiter = Item((cycle.number, 7), 1, "ok")
                waiting = submit(cancelled_waiter, wait_for_space=True)
                await wait_until(lambda key=cancelled_waiter.key: evaluated[key] == 1)
                assert not waiting.done()
                assert batcher.stats().accepted == accepted_before + 3
                before_cancel = batcher.stats()
                waiting.cancel()
                await asyncio.gather(waiting, return_exceptions=True)
                after_cancel = batcher.stats()
                for field in (
                    "accepted",
                    "rejected",
                    "succeeded",
                    "failed",
                    "cancelled",
                    "queued_items",
                    "pending_items",
                    "in_flight_items",
                    "active_batches",
                    "batches_dispatched",
                    "batches_completed",
                ):
                    assert getattr(after_cancel, field) == getattr(before_cancel, field), field

                # An accepted caller cancellation and an unaccepted one have distinct totals.
                accepted_tasks[cycle.cancel].cancel()
                await asyncio.gather(accepted_tasks[cycle.cancel], return_exceptions=True)
                expected[items[cycle.cancel].key] = (items[cycle.cancel], "cancelled")
                admitted_waiter = submit(items[3], wait_for_space=True)
                await wait_until(lambda key=items[3].key: evaluated[key] == 1)
                assert not admitted_waiter.done()
                assert batcher.stats().accepted == accepted_before + 3
                # Both the retained head and waiting producer must use their original cost.
                mutable_costs[items[1].key] = mutable_costs[items[3].key] = 99
                check_snapshot()

                if cycle.force:
                    await batcher.stop(force=True)
                    with pytest.raises(RuntimeError, match="stopping"):
                        await admitted_waiter
                    rejected += 1
                else:
                    released.set()
                    await wait_until(lambda before=accepted_before: batcher.stats().accepted == before + 4)
                    expected[items[3].key] = (items[3], items[3].outcome)
                    await batcher.stop()
                    accepted_tasks.append(admitted_waiter)
                results = await asyncio.gather(*accepted_tasks, return_exceptions=True)
                for item, result in zip(items[: len(accepted_tasks)], results, strict=True):
                    outcome = expected[item.key][1]
                    if outcome in {"cancel", "cancelled"}:
                        assert isinstance(result, asyncio.CancelledError), (item, result)
                    elif outcome == "error":
                        assert isinstance(result, ValueError) and result.args == (item.key,), (item, result)
                    else:
                        assert result == item.key, (item, result)

                flattened = [item for batch in executed for item in batch]
                assert len({item.key for item in flattened}) == len(flattened)
                assert all(item.key in expected for item in flattened)
                this_cycle = [item for item in flattened if item.key[0] == cycle.number]
                assert this_cycle == (items[:1] if cycle.force else items)
                assert all(
                    1 <= len(batch) <= 3 and sum(item.cost for item in batch) <= 5 for batch in executed
                )
                assert all(count == 1 for count in evaluated.values())
                stats = batcher.stats()
                assert stats.accepted == len(expected)
                assert stats.rejected == rejected
                assert stats.succeeded == sum(value == "ok" for _, value in expected.values())
                assert stats.failed == sum(value == "error" for _, value in expected.values())
                assert stats.cancelled == sum(
                    value in {"cancel", "cancelled"} for _, value in expected.values()
                )
                assert (
                    stats.queued_items,
                    stats.pending_items,
                    stats.in_flight_items,
                    stats.active_batches,
                ) == (0, 0, 0, 0)
                assert stats.batches_completed == stats.batches_dispatched == len(events) == len(executed)
                for event, batch in zip(events, executed, strict=True):
                    outcomes = [expected[item.key][1] for item in batch]
                    assert event.size == len(batch)
                    assert event.succeeded == outcomes.count("ok")
                    assert event.failed == outcomes.count("error")
                    assert event.cancelled == sum(value in {"cancel", "cancelled"} for value in outcomes)
                assert all(task.done() for task in tasks)
                assert not await batcher.is_running()
                assert not (asyncio.all_tasks() - initial_tasks)
    finally:
        released.set()
        try:
            await asyncio.wait_for(batcher.stop(force=True), timeout=2)
        finally:
            for task in tasks:
                task.cancel()
            try:
                await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), timeout=2)
            finally:
                loop.set_task_factory(old_factory)


@pytest.mark.asyncio
@pytest.mark.parametrize("seed", [3, 17, 47, 101])
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
async def test_generated_cost_admission_and_outcome_traces(seed, eager):
    cycles = generate(seed)
    try:
        await replay(cycles, eager)
    except AssertionError as error:
        # Retain the same assertion location, not an unrelated failure introduced by reduction.
        def location(failure):
            tb = failure.__traceback__
            while tb.tb_next:
                tb = tb.tb_next
            return tb.tb_frame.f_code.co_name, tb.tb_lineno

        target = location(error)

        async def still_fails(candidate):
            try:
                await replay(candidate, eager)
            except AssertionError as failure:
                return location(failure) == target
            except TimeoutError:
                return False
            return False

        reduced = await reduce_cycles(cycles, still_fails)
        error.add_note(
            f"Replay seed={seed}, eager={eager}; cycles=" + json.dumps([asdict(cycle) for cycle in reduced])
        )
        raise
    except TimeoutError as error:
        error.add_note(
            f"Replay seed={seed}, eager={eager}; cycles=" + json.dumps([asdict(cycle) for cycle in cycles])
        )
        raise


@pytest.mark.asyncio
async def test_trace_reducer_keeps_the_failing_cycle_and_is_bounded():
    cycles = generate(3)
    attempts = []

    async def contains_failure(candidate):
        attempts.append(candidate)
        return any(cycle.number == 4 for cycle in candidate)

    assert await reduce_cycles(cycles, contains_failure) == [cycles[4]]
    assert len(attempts) == len(cycles)
