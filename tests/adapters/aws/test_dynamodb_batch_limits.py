from __future__ import annotations

from unittest.mock import MagicMock

import pytest

get_module = pytest.importorskip("async_batcher.aws.dynamodb.get")
write_module = pytest.importorskip("async_batcher.aws.dynamodb.write")


@pytest.mark.parametrize(
    "batcher_class,limit",
    [(get_module.AsyncDynamoDbGetBatcher, 100), (write_module.AsyncDynamoDbWriteBatcher, 25)],
)
@pytest.mark.parametrize("invalid_size", [-1, 1000])
def test_reject_unsafe_batch_sizes(batcher_class, limit, invalid_size):
    with pytest.raises(ValueError, match=f"between 1 and {limit}"):
        batcher_class(max_batch_size=invalid_size, aioboto3_session=MagicMock())


@pytest.mark.parametrize(
    "batcher_class,limit",
    [(get_module.AsyncDynamoDbGetBatcher, 100), (write_module.AsyncDynamoDbWriteBatcher, 25)],
)
def test_deprecated_batch_size_cannot_bypass_limit(batcher_class, limit):
    with pytest.warns(UserWarning, match="batch_size.*deprecated"):
        with pytest.raises(ValueError, match=f"between 1 and {limit}"):
            batcher_class(batch_size=limit + 1, aioboto3_session=MagicMock())


@pytest.mark.parametrize(
    "batcher_class,limit",
    [(get_module.AsyncDynamoDbGetBatcher, 100), (write_module.AsyncDynamoDbWriteBatcher, 25)],
)
def test_service_limit_is_allowed(batcher_class, limit):
    batcher = batcher_class(max_batch_size=limit, aioboto3_session=MagicMock())
    assert batcher.max_batch_size == limit
