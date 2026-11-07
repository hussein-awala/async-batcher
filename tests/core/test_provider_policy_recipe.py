from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from email.utils import format_datetime

import pytest
from examples.recipes import provider_policy as policy


@pytest.fixture
def clock(monkeypatch):
    now, waits = [0.0], []

    async def advance(seconds):
        waits.append(seconds)
        now[0] += seconds

    monkeypatch.setattr(policy, "monotonic", lambda: now[0])
    monkeypatch.setattr(policy, "sleep", advance)
    monkeypatch.setattr(policy.random, "uniform", lambda _low, high: high)
    return now, waits


@pytest.mark.asyncio
async def test_shared_pacer_charges_every_attempt_and_estimated_work(clock):
    now, waits = clock
    pacer = policy.Pacer(requests_per_second=2, units_per_second=10)
    first = policy.ProviderPolicy(pacer, base_delay=0.1)
    second = policy.ProviderPolicy(pacer, base_delay=0.1)
    starts = []

    async def operation():
        starts.append(now[0])
        return "ok"

    assert await first.run(operation, units=20) == "ok"
    assert await second.run(operation, units=1) == "ok"
    assert await first.run(operation, units=1) == "ok"
    assert starts == [0, 2, 2.5]
    assert waits == [2, 0.5]


@pytest.mark.asyncio
async def test_finite_throttling_retries_honor_retry_after_and_keep_attempt_counts(clock):
    now, _ = clock
    limiter = policy.ProviderPolicy(policy.Pacer(requests_per_second=100), max_attempts=3, base_delay=0.5)
    starts = []

    async def operation():
        starts.append(now[0])
        raise policy.Throttled("2")

    with pytest.raises(policy.Throttled):
        await limiter.run(operation)
    assert starts == [0, 2, 4]
    assert (limiter.attempts, limiter.retry_attempts, limiter.throttled_responses) == (3, 2, 3)


@pytest.mark.asyncio
@pytest.mark.parametrize("header", ["31", "9" * 400, format_datetime(datetime(2100, 1, 1, tzinfo=UTC))])
async def test_server_delay_above_cap_is_not_shortened_or_retried(clock, header):
    limiter = policy.ProviderPolicy(policy.Pacer(requests_per_second=100), max_delay=30)

    async def operation():
        raise policy.Throttled(header)

    with pytest.raises(policy.Throttled):
        await limiter.run(operation)
    assert limiter.attempts == 1
    assert clock[1] == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error", [ValueError("bad input"), ConnectionError("ambiguous response"), TimeoutError("backend")]
)
async def test_unclassified_errors_are_never_retried(clock, error):
    limiter = policy.ProviderPolicy(policy.Pacer(requests_per_second=100))

    async def operation():
        raise error

    with pytest.raises(type(error)) as caught:
        await limiter.run(operation)
    assert caught.value is error
    assert limiter.attempts == 1 and limiter.retry_attempts == 0


@pytest.mark.asyncio
async def test_cancelled_backoff_sends_no_retry_and_releases_gate_for_other_calls(monkeypatch):
    now, sleeping, release = [0.0], asyncio.Event(), asyncio.Event()
    monkeypatch.setattr(policy, "monotonic", lambda: now[0])

    async def held_sleep(seconds):
        sleeping.set()
        await release.wait()
        now[0] += seconds

    monkeypatch.setattr(policy, "sleep", held_sleep)
    limiter = policy.ProviderPolicy(policy.Pacer(requests_per_second=100))

    async def throttled():
        raise policy.Throttled("2")

    task = asyncio.create_task(limiter.run(throttled))
    await asyncio.wait_for(sleeping.wait(), 2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert limiter.attempts == 1 and limiter.retry_attempts == 0
    release.set()

    async def healthy():
        return "ok"

    assert await limiter.run(healthy) == "ok"
    assert now[0] >= 2


@pytest.mark.asyncio
async def test_shared_cooldown_extension_is_rechecked_after_a_wait(monkeypatch):
    now, entered, release = [0.0], asyncio.Event(), asyncio.Event()
    pacer = policy.Pacer(requests_per_second=1)
    monkeypatch.setattr(policy, "monotonic", lambda: now[0])
    waits = []

    async def sleep(seconds):
        waits.append(seconds)
        if len(waits) == 1:
            entered.set()
            await release.wait()
        now[0] += seconds

    monkeypatch.setattr(policy, "sleep", sleep)
    await pacer.acquire()
    waiter = asyncio.create_task(pacer.acquire())
    await asyncio.wait_for(entered.wait(), 2)
    pacer.defer(5)
    release.set()
    await waiter
    assert waits == [1, 4]


@pytest.mark.parametrize("header", [None, "bad", "-1", "1.5", "NaN", "Wed, 21 Oct 2015 07:28:00"])
def test_invalid_or_missing_retry_after_uses_local_backoff(header):
    assert policy.retry_after_seconds(header) is None


def test_retry_after_date_and_integer():
    assert policy.retry_after_seconds(" 12 ") == 12
    assert policy.retry_after_seconds("Wed, 21 Oct 2015 07:28:00 GMT") == 0


@pytest.mark.asyncio
async def test_real_demo_runs_without_external_clients():
    result = await policy.demo()
    assert result["results"] == [i * 2 for i in range(8)]
    assert result["retry_attempts"] == 1
    assert result["throttled_responses"] == 1


@pytest.mark.parametrize(
    "options",
    [
        {"requests_per_second": 0},
        {"requests_per_second": True},
        {"requests_per_second": 1, "units_per_second": float("nan")},
    ],
)
def test_invalid_pacing_limits(options):
    with pytest.raises(ValueError):
        policy.Pacer(**options)


@pytest.mark.parametrize(
    "options",
    [
        {"max_attempts": 0},
        {"max_attempts": True},
        {"base_delay": 0},
        {"max_delay": float("inf")},
        {"base_delay": 2, "max_delay": 1},
    ],
)
def test_invalid_retry_limits(options):
    with pytest.raises(ValueError):
        policy.ProviderPolicy(policy.Pacer(requests_per_second=1), **options)


@pytest.mark.asyncio
async def test_last_throttled_attempt_still_defers_other_shared_calls(clock):
    now, _ = clock
    gate = policy.Pacer(requests_per_second=100)
    limiter = policy.ProviderPolicy(gate, max_attempts=1)

    async def throttled():
        raise policy.Throttled("2")

    with pytest.raises(policy.Throttled):
        await limiter.run(throttled)
    await gate.acquire()
    assert now[0] == 2


@pytest.mark.asyncio
async def test_total_budget_expires_during_backoff_without_another_attempt(monkeypatch):
    async def stalled_sleep(seconds):
        await asyncio.Future()

    monkeypatch.setattr(policy, "sleep", stalled_sleep)
    limiter = policy.ProviderPolicy(policy.Pacer(requests_per_second=100))

    async def throttled():
        raise policy.Throttled("1")

    with pytest.raises(TimeoutError):
        await limiter.run(throttled, total_timeout=0.01)
    assert limiter.attempts == 1 and limiter.retry_attempts == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("units", [-1, True, float("inf")])
async def test_invalid_estimated_work_never_calls_provider(units):
    limiter = policy.ProviderPolicy(policy.Pacer(requests_per_second=100))
    with pytest.raises(ValueError, match="units"):
        await limiter.run(None, units=units)
    assert limiter.attempts == 0
