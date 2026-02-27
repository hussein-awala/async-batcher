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


def _validate_operation(op: WriteOperation) -> ValueError | None:
    """Validate a write operation before executing it.

    Returns None if valid, or a ValueError describing the issue.
    """
    if op.operation == "INSERT" and op.data is None:
        return ValueError("data must be provided for INSERT operations")
    if op.operation == "UPDATE" and (op.key is None or op.data is None):
        return ValueError("key and data must be provided for UPDATE operations")
    if op.operation == "DELETE" and op.key is None:
        return ValueError("key must be provided for DELETE operations")
    return None


class AsyncScyllaDbWriteBatcher(AsyncBatcher[WriteOperation, None]):
    """Batcher for ScyllaDB write operations."""

    def process_batch(self, batch: list[WriteOperation]) -> list[None | Exception]:
        # Validate all operations before executing any
        results: list[None | Exception] = [_validate_operation(op) for op in batch]
        valid_ops = [(i, op) for i, (op, err) in enumerate(zip(batch, results)) if err is None]

        if not valid_ops:
            return results

        with BatchQuery() as b:
            for _i, op in valid_ops:
                if op.operation == "INSERT":
                    op.model.batch(b).create(**op.data)
                elif op.operation == "UPDATE":
                    op.model.objects(**op.key).batch(b).update(**op.data)
                elif op.operation == "DELETE":
                    op.model.objects(**op.key).batch(b).delete()
        return results
