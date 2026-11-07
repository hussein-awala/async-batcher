from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal

from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.orm import Mapper

from async_batcher.batcher import AsyncBatcher
from sqlalchemy import Row, insert, inspect, update

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession


class AsyncSqlalchemyWriteBatcher(AsyncBatcher[dict[str, Any], Row[tuple[Any, ...]] | None]):
    """Batch SQLAlchemy inserts or ORM updates.

    Bulk updates require a mapped ORM class and primary-key values in each item.
    Core Table objects are supported for inserts only: their UPDATE statements do
    not automatically add the primary-key filters needed for per-item updates.
    """

    def __init__(
        self,
        model: Any,
        async_engine: AsyncEngine,
        operation: Literal["insert", "update"] = "insert",
        returning: Any = None,
        **kwargs,
    ):
        if operation not in ["insert", "update"]:
            raise ValueError(f"Invalid operation: {operation}")
        if operation == "update" and not isinstance(inspect(model, raiseerr=False), Mapper):
            raise ValueError("Bulk UPDATE requires a mapped ORM class with primary-key values in each item")
        super().__init__(**kwargs)
        self.model = model
        self.async_engine = async_engine
        self.async_session_maker = async_sessionmaker(
            bind=async_engine,
            autocommit=False,
            autoflush=False,
            expire_on_commit=False,
        )
        self.operation = operation
        self.returning = returning
        if returning is None:
            self._returning_columns = []
        elif isinstance(returning, list | tuple):
            self._returning_columns = list(returning)
        else:
            self._returning_columns = [returning]
        if operation == "update" and self._returning_columns:
            raise ValueError("RETURNING is not supported for bulk UPDATE operations")

    async def process_batch(self, batch: list[dict[str, Any]]) -> Sequence[Row[tuple[Any, ...]]] | None:
        if not batch:
            if self.returning is None or (isinstance(self.returning, list | tuple) and not self.returning):
                return None
            return []
        session: AsyncSession
        async with self.async_session_maker() as session:
            if self.operation == "insert":
                statement = insert(self.model)
            elif self.operation == "update":
                statement = update(self.model)
            if self._returning_columns:
                statement = statement.returning(*self._returning_columns, sort_by_parameter_order=True)
            res = await session.execute(
                statement=statement,
                params=batch,
            )
            await session.commit()
            if self._returning_columns:
                return res.all()
