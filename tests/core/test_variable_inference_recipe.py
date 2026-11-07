from __future__ import annotations

import asyncio
import inspect
from concurrent.futures import ThreadPoolExecutor
from contextvars import ContextVar

import pytest
from examples.recipes.variable_inference import (
    DemoTokenModel,
    TokenRequest,
    collate,
    demo,
    inference_groups,
    unbatch,
)


def request(identifier, tokens, *, tenant="a", revision="v1"):
    return TokenRequest(identifier, tenant, revision, tokens)


def test_collation_snapshots_tokens_and_preserves_zero_tokens_with_attention_masks():
    mutable = [0, 2]
    first = request("first", mutable)
    mutable.append(999)
    second = request("second", (7,))
    batch = collate([first, second])
    assert batch.input_ids == ((0, 2), (7, 0))
    assert batch.attention_mask == ((1, 1), (1, 0))
    results = unbatch([first, second], batch, [[0, 4], [14, -99]])
    assert [result.request_id for result in results] == ["first", "second"]
    assert [result.values for result in results] == [(0, 4), (14,)]


@pytest.mark.parametrize("tokens", [(), [], [True], [-1], [1.5]])
def test_invalid_input_is_rejected_before_a_model_or_group_exists(tokens):
    with pytest.raises(ValueError):
        request("bad", tokens)


@pytest.mark.parametrize(
    "rows", [None, [[1]], [[1, 2], [3]], [[1, 2], [3, float("nan")]], [[1, 2], [3, True]]]
)
def test_complete_model_result_is_validated_including_trimmed_padding(rows):
    requests = [request("a", (1, 2)), request("b", (3,))]
    with pytest.raises(ValueError):
        unbatch(requests, collate(requests), rows)


@pytest.mark.asyncio
async def test_variable_lengths_route_results_and_bound_actual_padding_by_bucket_cost():
    model = DemoTokenModel()
    requests = [
        request(str(index), tuple(range(length))) for index, length in enumerate([1, 3, 4, 2, 5, 6, 8, 7])
    ]
    with ThreadPoolExecutor(max_workers=1) as pool:
        async with inference_groups(
            model,
            executor=pool,
            bucket_limits=(4, 8),
            padded_token_budget=16,
            max_batch_size=4,
            max_queue_time=60,
        ) as groups:
            results = await asyncio.wait_for(asyncio.gather(*(groups.process(item) for item in requests)), 2)
    assert [result.request_id for result in results] == [item.request_id for item in requests]
    assert [result.values for result in results] == [
        tuple(token * 2 for token in item.tokens) for item in requests
    ]
    shapes = model.report()
    assert sum(shape["rows"] * shape["calls"] for shape in shapes) == 8
    assert all(shape["rows"] * shape["padded_width"] <= 16 for shape in shapes)
    assert any(shape["rows"] == 2 and shape["padded_width"] == 6 for shape in shapes)


@pytest.mark.asyncio
async def test_tenants_revisions_and_length_buckets_never_share_a_model_call():
    calls = []

    def model(batch):
        calls.append(batch)
        return [[float(token) for token in row] for row in batch.input_ids]

    requests = [
        request("a", (1,), tenant="a", revision="v1"),
        request("b", (2,), tenant="b", revision="v1"),
        request("c", (3,), tenant="a", revision="v2"),
        request("d", (4, 5, 6, 7, 8), tenant="a", revision="v1"),
    ]
    with ThreadPoolExecutor(max_workers=1) as pool:
        async with inference_groups(
            model, executor=pool, bucket_limits=(4, 8), padded_token_budget=16
        ) as groups:
            results = await asyncio.gather(*(groups.process(item) for item in requests))
            assert groups.group_count == 4
    assert len(calls) == 4
    assert all(len(batch.input_ids) == 1 for batch in calls)
    assert [result.request_id for result in results] == ["a", "b", "c", "d"]


@pytest.mark.asyncio
async def test_oversized_request_fails_without_creating_a_group_or_disturbing_valid_requests():
    model = DemoTokenModel()
    with ThreadPoolExecutor(max_workers=1) as pool:
        async with inference_groups(
            model, executor=pool, bucket_limits=(4,), padded_token_budget=4
        ) as groups:
            with pytest.raises(ValueError, match="largest length bucket"):
                await groups.process(request("large", (1, 2, 3, 4, 5)))
            assert groups.group_count == 0
            result = await groups.process(request("valid", (1, 2)))
    assert result.values == (2, 4)
    assert model.report() == [{"rows": 1, "padded_width": 2, "calls": 1}]


@pytest.mark.asyncio
async def test_invalid_later_output_prevents_any_success_in_its_batch():
    def model(batch):
        return [[1], [float("inf")]]

    with ThreadPoolExecutor(max_workers=1) as pool:
        async with inference_groups(model, executor=pool, max_batch_size=2, max_queue_time=60) as groups:
            outcomes = await asyncio.gather(
                groups.process(request("first", (1,))),
                groups.process(request("second", (2,))),
                return_exceptions=True,
            )
    assert all(isinstance(outcome, ValueError) for outcome in outcomes)


@pytest.mark.asyncio
async def test_accidental_async_result_is_rejected_without_leaking_a_coroutine():
    coroutines = []

    async def async_result():
        return [[1]]

    def model(batch):
        coroutine = async_result()
        coroutines.append(coroutine)
        return coroutine

    with ThreadPoolExecutor(max_workers=1) as pool:
        async with inference_groups(model, executor=pool) as groups:
            with pytest.raises(TypeError, match="synchronous model results"):
                await groups.process(request("one", (1,)))
    assert inspect.getcoroutinestate(coroutines[0]) == inspect.CORO_CLOSED


@pytest.mark.asyncio
async def test_demo_reports_actual_shapes_and_keeps_the_borrowed_pool_open():
    with ThreadPoolExecutor(max_workers=1) as pool:
        report = await demo(pool)
        assert pool.submit(lambda: 7).result(timeout=2) == 7
    assert report["demo_model"] is True
    assert report["completed_requests"] == 32
    assert all(shape["rows"] * shape["padded_width"] <= 256 for shape in report["actual_shapes"])


def test_configuration_and_incompatible_direct_collation_fail_early():
    with ThreadPoolExecutor(max_workers=1) as pool:
        with pytest.raises(ValueError, match="strictly increasing"):
            inference_groups(DemoTokenModel(), executor=pool, bucket_limits=(8, 4))
        with pytest.raises(ValueError, match="largest bucket"):
            inference_groups(DemoTokenModel(), executor=pool, padded_token_budget=128)
    with pytest.raises(ValueError, match="one tenant"):
        collate([request("a", (1,), tenant="a"), request("b", (2,), tenant="b")])


@pytest.mark.asyncio
async def test_recipe_uses_service_context_and_keeps_model_exception_text_out_of_logs(caplog):
    caller_context = ContextVar("inference-request-context", default="service-default")
    observed = []

    def model(batch):
        observed.append(caller_context.get())
        raise ValueError("private-model-error")

    token = caller_context.set("private-tenant-context")
    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            async with inference_groups(model, executor=pool) as groups:
                with pytest.raises(ValueError, match="private-model-error"):
                    await groups.process(request("private-request-id", (1,)))
    finally:
        caller_context.reset(token)
    assert observed == ["service-default"]
    assert "private" not in caplog.text
    assert caplog.records
    assert all(not record.exc_info for record in caplog.records)
