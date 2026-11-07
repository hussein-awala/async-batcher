from __future__ import annotations

import asyncio
from unittest.mock import create_autospec

import pytest

sdk = pytest.importorskip("pinecone")

from async_batcher.pinecone import (  # noqa: E402
    AsyncPineconeDeleteBatcher,
    AsyncPineconeFetchBatcher,
    AsyncPineconeUpsertBatcher,
)

BATCHERS = [AsyncPineconeUpsertBatcher, AsyncPineconeFetchBatcher, AsyncPineconeDeleteBatcher]


@pytest.fixture
def index():
    return create_autospec(sdk.AsyncIndex, instance=True)


@pytest.mark.parametrize("batcher_type", BATCHERS)
@pytest.mark.parametrize("size", [-1, 0, 1001, True, 1.5])
def test_invalid_batch_sizes_are_rejected(index, batcher_type, size):
    with pytest.raises((TypeError, ValueError)):
        batcher_type(index=index, max_batch_size=size)


@pytest.mark.parametrize("batcher_type", BATCHERS)
def test_only_async_indexes_and_string_namespaces_are_accepted(index, batcher_type):
    with pytest.raises(TypeError, match="AsyncIndex"):
        batcher_type(index=create_autospec(sdk.Index, instance=True))
    with pytest.raises(TypeError, match="namespace"):
        batcher_type(index=index, namespace=None)
    assert batcher_type(index=index, max_batch_size=1000).max_batch_size == 1000


@pytest.mark.parametrize("batcher_type", BATCHERS)
@pytest.mark.asyncio
async def test_empty_and_oversized_direct_batches_do_not_send_requests(index, batcher_type):
    batcher = batcher_type(index=index)
    assert await batcher.process_batch([]) == []
    with pytest.raises(ValueError, match="1000"):
        await batcher.process_batch(["unused"] * 1001)
    index.upsert.assert_not_called()
    index.fetch.assert_not_called()
    index.delete.assert_not_called()


@pytest.mark.asyncio
async def test_upsert_preserves_dense_sparse_and_hybrid_sdk_vectors(index):
    vectors = [
        sdk.Vector(id="dense", values=[0.5, 0.2], metadata={"kind": "dense"}),
        sdk.Vector(id="sparse", sparse_values=sdk.SparseValues(indices=[3], values=[0.7])),
        sdk.Vector(id="hybrid", values=[0.8], sparse_values=sdk.SparseValues(indices=[9], values=[1.0])),
    ]
    index.upsert.return_value = sdk.UpsertResponse(upserted_count=3)
    batcher = AsyncPineconeUpsertBatcher(index=index, namespace="articles")
    assert await batcher.process_batch(vectors) == [None, None, None]
    index.upsert.assert_awaited_once_with(vectors=vectors, namespace="articles")
    assert index.upsert.call_args.kwargs["vectors"][1] is vectors[1]


@pytest.mark.asyncio
async def test_duplicate_upsert_ids_are_rejected_before_any_write(index):
    batcher = AsyncPineconeUpsertBatcher(index=index)
    with pytest.raises(ValueError, match="Duplicate"):
        await batcher.process_batch([sdk.Vector(id="same", values=[1]), sdk.Vector(id="same", values=[2])])
    index.upsert.assert_not_called()


@pytest.mark.asyncio
async def test_upsert_requires_sdk_vectors(index):
    with pytest.raises(TypeError, match="Vector objects"):
        await AsyncPineconeUpsertBatcher(index=index).process_batch([{"id": "one", "values": [1]}])
    index.upsert.assert_not_called()


@pytest.mark.asyncio
async def test_short_upsert_acknowledgement_is_not_reported_as_success(index):
    index.upsert.return_value = sdk.UpsertResponse(upserted_count=1)
    batcher = AsyncPineconeUpsertBatcher(index=index)
    with pytest.raises(RuntimeError, match="1 of 2"):
        await batcher.process_batch([sdk.Vector(id="one", values=[1]), sdk.Vector(id="two", values=[2])])


@pytest.mark.asyncio
async def test_fetch_routes_unordered_results_duplicates_and_missing_ids(index):
    one = sdk.Vector(id="one", values=[1])
    two = sdk.Vector(id="two", sparse_values=sdk.SparseValues(indices=[2], values=[0.5]))
    index.fetch.return_value = sdk.FetchResponse(vectors={"two": two, "one": one}, namespace="records")
    batcher = AsyncPineconeFetchBatcher(index=index, namespace="records")
    assert await batcher.process_batch(["one", "missing", "two", "one"]) == [one, None, two, one]
    index.fetch.assert_awaited_once_with(ids=["one", "missing", "two"], namespace="records")


@pytest.mark.asyncio
async def test_delete_deduplicates_ids_and_returns_only_acknowledgements(index):
    batcher = AsyncPineconeDeleteBatcher(index=index, namespace="records")
    assert await batcher.process_batch(["one", "missing", "one"]) == [None, None, None]
    index.delete.assert_awaited_once_with(ids=["one", "missing"], namespace="records")


@pytest.mark.parametrize("batcher_type", [AsyncPineconeFetchBatcher, AsyncPineconeDeleteBatcher])
@pytest.mark.asyncio
async def test_invalid_id_types_are_rejected_before_deduplication(index, batcher_type):
    with pytest.raises(TypeError, match="IDs must be strings"):
        await batcher_type(index=index).process_batch([True, 1])
    index.fetch.assert_not_called()
    index.delete.assert_not_called()


@pytest.mark.parametrize(
    "batcher_type,method,items",
    [
        (
            AsyncPineconeUpsertBatcher,
            "upsert",
            [sdk.Vector(id="one", values=[1]), sdk.Vector(id="two", values=[2])],
        ),
        (AsyncPineconeFetchBatcher, "fetch", ["one", "two"]),
        (AsyncPineconeDeleteBatcher, "delete", ["one", "two"]),
    ],
)
@pytest.mark.asyncio
async def test_sdk_errors_reach_every_caller_without_closing_the_index(index, batcher_type, method, items):
    error = sdk.ApiError("test rejection", status_code=400)
    getattr(index, method).side_effect = error
    batcher = batcher_type(index=index, max_batch_size=2, max_queue_time=1)
    try:
        results = await asyncio.wait_for(
            asyncio.gather(*(batcher.process(item) for item in items), return_exceptions=True), 5
        )
        assert results == [error, error]
        assert getattr(index, method).await_count == 1
    finally:
        await batcher.stop()
    index.close.assert_not_called()


@pytest.mark.asyncio
async def test_dispatcher_batches_requests_and_keeps_shared_index_owned_by_caller(index):
    one = sdk.Vector(id="one", values=[1])
    index.fetch.return_value = sdk.FetchResponse(vectors={"one": one})
    batcher = AsyncPineconeFetchBatcher(index=index, max_batch_size=3, max_queue_time=1)
    try:
        assert await asyncio.wait_for(
            asyncio.gather(*(batcher.process(item) for item in ["one", "missing", "one"])), 5
        ) == [one, None, one]
    finally:
        await batcher.stop()
    index.fetch.assert_awaited_once_with(ids=["one", "missing"], namespace="")
    index.close.assert_not_called()


@pytest.mark.parametrize("identifier", ["", "a" * 513, "café", "bad\x00id"])
@pytest.mark.parametrize("batcher_type", BATCHERS)
@pytest.mark.asyncio
async def test_invalid_ids_are_rejected_for_every_operation(index, batcher_type, identifier):
    item = sdk.Vector(id=identifier, values=[1]) if batcher_type is AsyncPineconeUpsertBatcher else identifier
    with pytest.raises(ValueError, match="ASCII"):
        await batcher_type(index=index).process_batch([item])
    index.upsert.assert_not_called()
    index.fetch.assert_not_called()
    index.delete.assert_not_called()
