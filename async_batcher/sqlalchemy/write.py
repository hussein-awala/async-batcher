from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING, Any, Literal

from async_batcher.batcher import AsyncBatcher
from sqlalchemy import Row, insert, update

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Sequence

    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker


class AsyncSqlalchemyWriteBatcher(AsyncBatcher[dict[str, Any], None]):
    def __init__(
        self,
        model: Any,
        async_session_maker: async_sessionmaker[AsyncSession],
        operation: Literal["insert", "update"] = "insert",
        returning: Any = None,
        **kwargs,
    ):
        if operation not in ["insert", "update"]:
            raise ValueError(f"Invalid operation: {operation}")
        super().__init__(**kwargs)
        self.model = model
        self.async_session_maker = async_session_maker
        self.operation = operation
        self.returning = returning

    @contextlib.asynccontextmanager
    async def create_async_session(self) -> AsyncIterator[AsyncSession]:
        """A contextmanager to create and teardown an async session."""
        async_session: AsyncSession | None = None
        try:
            async with self.async_session_maker() as async_session:
                yield async_session
            await async_session.commit()
        except Exception:
            if async_session:
                await async_session.rollback()
            raise
        finally:
            if async_session:
                await async_session.close()

    async def process_batch(self, batch: list[dict[str, Any]]) -> Sequence[Row[tuple[Any]]] | None:
        session: AsyncSession
        async with self.create_async_session() as session:
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
        if self.returning:
            return res.all()
