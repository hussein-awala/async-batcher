from __future__ import annotations

from unittest.mock import MagicMock

import pytest

get_module = pytest.importorskip("async_batcher.aws.dynamodb.get")
write_module = pytest.importorskip("async_batcher.aws.dynamodb.write")


@pytest.mark.parametrize(
    "batcher_class,expected",
    [(get_module.AsyncDynamoDbGetBatcher, []), (write_module.AsyncDynamoDbWriteBatcher, None)],
)
@pytest.mark.asyncio
async def test_empty_batch_does_not_create_a_service_request(batcher_class, expected):
    session = MagicMock()
    batcher = batcher_class(aioboto3_session=session)
    assert await batcher.process_batch([]) == expected
    session.resource.assert_not_called()
