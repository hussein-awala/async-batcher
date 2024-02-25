from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

from cassandra.cqlengine.query import BatchQuery

from async_batcher.batcher import AsyncBatcher

if TYPE_CHECKING:
    from cassandra.cqlengine.models import Model


@dataclass(kw_only=True)
class WriteOperation:
    operation: Literal["INSERT", "UPDATE", "DELETE"]
    model: Model
    key: dict[str, Any] | None = None
    data: dict[str, Any] | None = None


class AsyncScyllaDbWriteBatcher(AsyncBatcher[WriteOperation, None]):
    """Batcher for ScyllaDB write operations."""

    def process_batch(self, *, batch: list[WriteOperation]) -> list[None | Exception]:
        results = []
        with BatchQuery() as b:
            for op in batch:
                if op.operation == "INSERT":
                    if op.data is None:
                        results.append(ValueError("data must be provided for INSERT operations"))
                    else:
                        op.model.batch(b).create(**op.data)
                        results.append(None)
                elif op.operation == "UPDATE":
                    if op.key is None or op.data is None:
                        results.append(ValueError("key and data must be provided for UPDATE operations"))
                    else:
                        op.model.objects(**op.key).batch(b).update(**op.data)
                        results.append(None)
                elif op.operation == "DELETE":
                    if op.key is None:
                        results.append(ValueError("key must be provided for DELETE operations"))
                    else:
                        op.model.objects(**op.key).batch(b).delete()
                        results.append(None)
        return results
