from __future__ import annotations

from asyncio import current_task
from typing import TYPE_CHECKING, Any, Literal

from async_batcher.batcher import AsyncBatcher
from sqlalchemy import Row, insert, update
from sqlalchemy.ext.asyncio import async_scoped_session, async_sessionmaker

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession


class AsyncSqlalchemyWriteBatcher(AsyncBatcher[dict[str, Any], Row[tuple[Any, ...]] | None]):
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
        super().__init__(**kwargs)
        self.model = model
        self.async_engine = async_engine
        self.async_session_maker = async_scoped_session(
            async_sessionmaker(
                bind=async_engine,
                autocommit=False,
                autoflush=False,
                expire_on_commit=False,
            ),
            scopefunc=current_task,
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
