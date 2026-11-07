"""Provider-neutral acknowledgement after an atomic SQLite effect and receipt.

The source and acknowledgement callback belong to the application. This recipe
makes no exactly-once claim about a broker, a network, or an external side effect.
"""

from __future__ import annotations

import asyncio
import sqlite3
from contextlib import aclosing, closing
from dataclasses import dataclass
from typing import TYPE_CHECKING

from async_batcher import AsyncBatcher

if TYPE_CHECKING:
    from collections.abc import AsyncIterable, Awaitable, Callable, Iterable
    from pathlib import Path


@dataclass(frozen=True)
class Message:
    """Immutable business identity and payload, retained across redeliveries."""

    identifier: str
    counter: str
    increment: int


@dataclass(frozen=True)
class Delivery:
    """The broker's acknowledgement token may change when a message is replayed."""

    message: Message
    acknowledgement_token: str


@dataclass(frozen=True)
class Receipt:
    identifier: str
    replayed: bool


@dataclass(frozen=True)
class ConsumerReport:
    acknowledged: int
    replayed: int
    failed: int


class ReceiptStore:
    """One SQLite transaction owns both deduplication and the counter update.

    Each call opens its connection on its worker thread. No connection crosses
    threads; the database file belongs to the application and is not deleted.
    """

    def __init__(self, database: Path):
        self.database = database

    def initialize(self) -> None:
        with closing(sqlite3.connect(self.database)) as connection, connection:
            if connection.execute("PRAGMA journal_mode=WAL").fetchone()[0] != "wal":
                raise RuntimeError("The consumer receipt database requires WAL journaling")
            connection.execute("PRAGMA synchronous=FULL")
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS consumer_receipts (
                    identifier TEXT PRIMARY KEY,
                    counter TEXT NOT NULL,
                    increment INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS consumer_counters (
                    name TEXT PRIMARY KEY,
                    total INTEGER NOT NULL CHECK(typeof(total) = 'integer')
                );
                """
            )

    def apply(self, deliveries: list[Delivery]) -> list[Receipt]:
        results = []
        # The context commits on success and rolls back the whole native batch
        # on any failure. A receipt cannot survive without its business effect.
        with closing(sqlite3.connect(self.database)) as connection, connection:
            connection.execute("PRAGMA synchronous=FULL")
            connection.execute("BEGIN IMMEDIATE")
            for delivery in deliveries:
                message = delivery.message
                if (
                    not isinstance(message.identifier, str)
                    or not message.identifier
                    or not isinstance(message.counter, str)
                    or not message.counter
                    or isinstance(message.increment, bool)
                    or not isinstance(message.increment, int)
                    or not -(2**63) <= message.increment < 2**63
                ):
                    raise ValueError(
                        "Message requires stable nonempty identity/counter and a 64-bit increment"
                    )
                existing = connection.execute(
                    "SELECT counter, increment FROM consumer_receipts WHERE identifier = ?",
                    (message.identifier,),
                ).fetchone()
                if existing is not None:
                    if existing != (message.counter, message.increment):
                        raise ValueError("Message identity was reused with a different payload")
                    results.append(Receipt(message.identifier, replayed=True))
                    continue
                connection.execute(
                    "INSERT INTO consumer_receipts VALUES (?, ?, ?)",
                    (message.identifier, message.counter, message.increment),
                )
                connection.execute(
                    "INSERT INTO consumer_counters VALUES (?, ?) "
                    "ON CONFLICT(name) DO UPDATE SET total = total + excluded.total",
                    (message.counter, message.increment),
                )
                results.append(Receipt(message.identifier, replayed=False))
        return results


async def consume(
    source: Iterable[Delivery] | AsyncIterable[Delivery],
    store: ReceiptStore,
    acknowledge: Callable[[Delivery], Awaitable[None]],
    *,
    batch_size: int = 32,
    max_pending: int = 64,
) -> ConsumerReport:
    """Acknowledge only committed outcomes; leave failed deliveries unacknowledged.

    An acknowledgement failure terminates iteration and propagates. Other accepted
    writes may already be committed or finish during drain; their receipts make a
    replay safe for this database effect. The broker's retry/dead-letter policy
    remains outside this helper. Initialize the store before consuming.
    """
    acknowledged = replayed = failed = 0
    async with AsyncBatcher.from_callable(
        store.apply, max_batch_size=batch_size, concurrency=1, log_exceptions=False
    ) as batcher:
        async with aclosing(batcher.map_outcomes(source, max_pending=max_pending)) as outcomes:
            async for outcome in outcomes:
                if outcome.error is not None:
                    failed += 1
                    continue
                receipt = outcome.result
                assert receipt is not None
                await acknowledge(outcome.item)
                acknowledged += 1
                replayed += receipt.replayed
    return ConsumerReport(acknowledged, replayed, failed)


async def demo(database: Path) -> ConsumerReport:
    """An application supplies real broker deliveries and ack calls instead."""
    store = ReceiptStore(database)
    await asyncio.to_thread(store.initialize)
    message = Message("event-1", "page-views", 1)
    deliveries = [Delivery(message, "delivery-a"), Delivery(message, "delivery-b")]

    async def acknowledge(delivery: Delivery) -> None:
        print(f"acknowledged {delivery.acknowledgement_token}")

    return await consume(deliveries, store, acknowledge)
