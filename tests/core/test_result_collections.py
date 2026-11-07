from __future__ import annotations

import asyncio
from array import array

import pytest
from async_batcher.batcher import AsyncBatcher


class ResultBatcher(AsyncBatcher):
    def __init__(self, results, **kwargs):
        super().__init__(**kwargs)
        self.results = results

    async def process_batch(self, batch):
        return self.results


async def collect_results(harness, results):
    batcher = harness.create(ResultBatcher, results=results, max_batch_size=2)
    requests = harness.submit(batcher, 1, 2)
    outcomes = await asyncio.wait_for(asyncio.gather(*requests, return_exceptions=True), timeout=1)
    await asyncio.wait_for(batcher.stop(), timeout=1)
    return outcomes


@pytest.mark.asyncio
@pytest.mark.parametrize("results", [{1: 2, 2: 4}, {2, 4}, frozenset({2, 4}), "24", b"24", bytearray(b"24")])
async def test_ambiguous_result_containers_fail_the_entire_batch(harness, results):
    outcomes = await collect_results(harness, results)
    assert all(isinstance(result, TypeError) for result in outcomes)
    assert all("ordered" in str(result) for result in outcomes)


@pytest.mark.asyncio
@pytest.mark.parametrize("values", [[2], [2, 4, 6]])
async def test_inconsistent_result_length_cannot_partially_succeed(harness, values):
    class InconsistentResults:
        def __len__(self):
            return 2

        def __iter__(self):
            return iter(values)

    outcomes = await collect_results(harness, InconsistentResults())
    assert all(isinstance(result, ValueError) for result in outcomes)
    assert outcomes[0] is outcomes[1]


@pytest.mark.asyncio
async def test_result_iterator_failure_cannot_partially_succeed(harness):
    failure = ValueError("result iteration failed")

    class BrokenResults:
        def __len__(self):
            return 2

        def __iter__(self):
            yield 2
            raise failure

    assert await collect_results(harness, BrokenResults()) == [failure, failure]


@pytest.mark.asyncio
@pytest.mark.parametrize("results", [(2, 4), array("i", [2, 4]), range(2, 6, 2)])
async def test_ordered_result_iterables_preserve_request_alignment(harness, results):
    assert await collect_results(harness, results) == [2, 4]


@pytest.mark.asyncio
async def test_oversized_result_iterator_is_not_consumed_without_a_bound(harness):
    consumed = []

    class OversizedResults:
        def __len__(self):
            return 2

        def __iter__(self):
            for value in range(100):
                consumed.append(value)
                yield value

    outcomes = await collect_results(harness, OversizedResults())
    assert all(isinstance(result, ValueError) for result in outcomes)
    assert consumed == [0, 1, 2]
