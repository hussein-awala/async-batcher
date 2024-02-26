from __future__ import annotations

from asyncio import current_task
from typing import TYPE_CHECKING, Any, Literal

from async_batcher.batcher import AsyncBatcher
from sqlalchemy import Row, insert, update
from sqlalchemy.ext.asyncio import async_scoped_session, async_sessionmaker

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession


class AsyncSqlalchemyWriteBatcher(AsyncBatcher[dict[str, Any], None]):
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

    async def process_batch(self, batch: list[dict[str, Any]]) -> Sequence[Row[tuple[Any]]] | None:
        session: AsyncSession
        async with self.async_session_maker() as session:
            if self.operation == "insert":
                statement = insert(self.model).returning()
            elif self.operation == "update":
                statement = update(self.model)
            if self.returning:
                if isinstance(self.returning, list):
                    statement = statement.returning(*self.returning)
                else:
                    statement = statement.returning(self.returning)
            res = await session.execute(
                statement=statement,
                params=batch,
            )
            await session.commit()
            if self.returning:
                return res.all()
