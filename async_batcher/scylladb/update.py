from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

from cassandra.cqlengine.query import BatchQuery, BatchType

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
    if op.operation not in {"INSERT", "UPDATE", "DELETE"}:
        return ValueError(f"Invalid operation: {op.operation}")
    if op.operation == "INSERT" and op.data is None:
        return ValueError("data must be provided for INSERT operations")
    if op.operation == "UPDATE" and (op.key is None or op.data is None):
        return ValueError("key and data must be provided for UPDATE operations")
    if op.operation == "DELETE" and op.key is None:
        return ValueError("key must be provided for DELETE operations")
    return None


class AsyncScyllaDbWriteBatcher(AsyncBatcher[WriteOperation, None]):
    """Batch ScyllaDB writes after validating each operation.

    Invalid operations return an error in their result position and are not
    submitted. Valid operations still execute when other items fail validation.
    Empty batches and batches containing only invalid operations create no query.
    All valid operations must use the same model connection; a mixed-connection
    batch fails before any writes are submitted.
    Counter models use COUNTER batches and cannot be mixed with non-counter models.
    """

    def process_batch(self, batch: list[WriteOperation]) -> list[None | Exception]:
        # Validate all operations before executing any
        results: list[None | Exception] = [_validate_operation(op) for op in batch]
        valid_ops = [op for op, error in zip(batch, results, strict=True) if error is None]

        if not valid_ops:
            return results

        connections = {getattr(op.model, "__connection__", None) for op in valid_ops}
        if len(connections) != 1:
            raise ValueError("All operations in a batch must use the same connection")

        counter_models = {bool(op.model._has_counter) for op in valid_ops}
        if len(counter_models) != 1:
            raise ValueError("A batch cannot mix counter and non-counter models")
        batch_type = BatchType.Counter if counter_models.pop() else None

        with BatchQuery(connection=connections.pop(), batch_type=batch_type) as b:
            for op in valid_ops:
                if op.operation == "INSERT":
                    op.model.batch(b).create(**op.data)
                elif op.operation == "UPDATE":
                    op.model.objects(**op.key).batch(b).update(**op.data)
                elif op.operation == "DELETE":
                    op.model.objects(**op.key).batch(b).delete()
        return results
