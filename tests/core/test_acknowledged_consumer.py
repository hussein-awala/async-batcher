from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import subprocess
import sys
from contextlib import closing
from pathlib import Path

import pytest
from examples.recipes.acknowledged_consumer import Delivery, Message, ReceiptStore, consume


def read_state(database):
    with closing(sqlite3.connect(database)) as connection, connection:
        return (
            connection.execute("SELECT * FROM consumer_receipts ORDER BY identifier").fetchall(),
            connection.execute("SELECT * FROM consumer_counters ORDER BY name").fetchall(),
        )


@pytest.mark.asyncio
async def test_acknowledges_each_delivery_after_receipt_and_effect_commit(tmp_path):
    database = tmp_path / "consumer.sqlite"
    store = ReceiptStore(database)
    store.initialize()
    message = Message("id-1", "views", 3)
    deliveries = [Delivery(message, "delivery-1"), Delivery(message, "delivery-2")]
    acknowledged = []

    async def ack(delivery):
        assert read_state(database) == ([("id-1", "views", 3)], [("views", 3)])
        acknowledged.append(delivery.acknowledgement_token)

    report = await consume(deliveries, store, ack, batch_size=2)
    assert (report.acknowledged, report.replayed, report.failed) == (2, 1, 0)
    assert acknowledged == ["delivery-1", "delivery-2"]


@pytest.mark.asyncio
async def test_transaction_failure_rolls_back_both_effects_and_receipts_and_never_acks(tmp_path):
    database = tmp_path / "consumer.sqlite"
    store = ReceiptStore(database)
    store.initialize()
    with closing(sqlite3.connect(database)) as connection, connection:
        connection.executescript(
            "CREATE TRIGGER reject_bad BEFORE INSERT ON consumer_counters "
            "WHEN NEW.name = 'bad' BEGIN SELECT RAISE(ABORT, 'injected failure'); END;"
        )
    deliveries = [Delivery(Message("one", "good", 1), "a"), Delivery(Message("two", "bad", 1), "b")]
    acknowledgements = []

    async def ack(delivery):
        acknowledgements.append(delivery)

    report = await consume(deliveries, store, ack, batch_size=2)
    assert report.failed == 2 and report.acknowledged == 0
    assert not acknowledgements
    assert read_state(database) == ([], [])


@pytest.mark.asyncio
async def test_ack_failure_leaves_committed_effect_replayable_with_new_delivery_token(tmp_path):
    store = ReceiptStore(tmp_path / "consumer.sqlite")
    store.initialize()
    message = Message("stable-id", "views", 1)

    async def lost_ack(delivery):
        raise ConnectionError("acknowledgement connection lost")

    with pytest.raises(ConnectionError):
        await consume([Delivery(message, "first-attempt")], store, lost_ack)
    acknowledged = []

    async def ack(delivery):
        acknowledged.append(delivery.acknowledgement_token)

    report = await consume([Delivery(message, "second-attempt")], store, ack)
    assert report.replayed == report.acknowledged == 1
    assert acknowledged == ["second-attempt"]
    assert read_state(store.database)[1] == [("views", 1)]


@pytest.mark.asyncio
async def test_same_identity_different_payload_is_not_acknowledged(tmp_path):
    store = ReceiptStore(tmp_path / "consumer.sqlite")
    store.initialize()
    store.apply([Delivery(Message("stable-id", "views", 2), "old")])
    acknowledgements = []

    async def ack(delivery):
        acknowledgements.append(delivery)

    report = await consume([Delivery(Message("stable-id", "views", 99), "new")], store, ack)
    assert report.failed == 1 and not acknowledgements
    assert read_state(store.database)[1] == [("views", 2)]


@pytest.mark.asyncio
async def test_held_ack_bounds_consumption_and_cancellation_closes_source(tmp_path):
    store = ReceiptStore(tmp_path / "consumer.sqlite")
    store.initialize()
    consumed, closed = [], []
    started = asyncio.Event()

    def source():
        try:
            for index in range(100):
                consumed.append(index)
                yield Delivery(Message(str(index), "views", 1), str(index))
        finally:
            closed.append(True)

    async def ack(delivery):
        started.set()
        await asyncio.Event().wait()

    task = asyncio.create_task(consume(source(), store, ack, batch_size=2, max_pending=3))
    try:
        await asyncio.wait_for(started.wait(), 2)
        # One yielded outcome can be held by acknowledgement outside map's window.
        for _ in range(10):
            await asyncio.sleep(0)
        assert len(consumed) <= 4
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 2)
    assert closed == [True]
    assert len(read_state(store.database)[0]) <= 4


def test_real_process_crash_after_commit_before_ack_replays_without_reapplying_effect(tmp_path):
    worker = tmp_path / "consumer_worker.py"
    worker.write_text(
        """import asyncio, json, os, sys
from pathlib import Path
from examples.recipes.acknowledged_consumer import Delivery, Message, ReceiptStore, consume

async def main():
    store = ReceiptStore(Path(sys.argv[1]))
    store.initialize()
    attempt = sys.argv[3]
    deliveries = [Delivery(Message(str(i), "views", 1), attempt + "-" + str(i)) for i in range(2)]
    async def ack(delivery):
        if attempt == "crash":
            os._exit(17)
        with open(sys.argv[2], "a", encoding="utf-8") as log:
            log.write(delivery.acknowledgement_token + "\\n")
            log.flush()
            os.fsync(log.fileno())
    report = await consume(deliveries, store, ack, batch_size=2, max_pending=2)
    print(json.dumps([report.acknowledged, report.replayed, report.failed]))
asyncio.run(main())
""",
        encoding="utf-8",
    )
    database, ack_log = tmp_path / "durable.sqlite", tmp_path / "acknowledged.txt"
    env = {**os.environ, "PYTHONPATH": str(Path(__file__).resolve().parents[2])}
    command = [sys.executable, str(worker), str(database), str(ack_log)]
    crashed = subprocess.run([*command, "crash"], cwd=tmp_path, env=env, capture_output=True, timeout=10)
    assert crashed.returncode == 17, crashed.stderr.decode()
    assert not ack_log.exists()
    assert read_state(database)[1] == [("views", 2)]
    replayed = subprocess.run(
        [*command, "replay"], cwd=tmp_path, env=env, capture_output=True, text=True, check=True, timeout=10
    )
    assert json.loads(replayed.stdout) == [2, 2, 0]
    assert ack_log.read_text(encoding="utf-8").splitlines() == ["replay-0", "replay-1"]
    assert read_state(database)[1] == [("views", 2)]


@pytest.mark.asyncio
async def test_counter_overflow_rolls_back_new_receipt_without_acknowledging(tmp_path):
    store = ReceiptStore(tmp_path / "consumer.sqlite")
    store.initialize()
    store.apply([Delivery(Message("first", "counter", 2**63 - 1), "old")])
    acknowledgements = []

    async def ack(delivery):
        acknowledgements.append(delivery)

    report = await consume([Delivery(Message("overflow", "counter", 1), "new")], store, ack)
    assert report.failed == 1 and not acknowledgements
    assert read_state(store.database) == ([("first", "counter", 2**63 - 1)], [("counter", 2**63 - 1)])


@pytest.mark.asyncio
async def test_async_source_failure_after_committed_effect_closes_source_and_allows_replay(tmp_path):
    store = ReceiptStore(tmp_path / "consumer.sqlite")
    store.initialize()
    message = Message("stable", "views", 1)
    acknowledged = asyncio.Event()
    closed = []

    async def source():
        try:
            yield Delivery(message, "first")
            await acknowledged.wait()
            raise RuntimeError("source connection failed")
        finally:
            closed.append(True)

    async def ack(delivery):
        assert read_state(store.database)[1] == [("views", 1)]
        acknowledged.set()

    with pytest.raises(RuntimeError, match="source connection failed"):
        await asyncio.wait_for(consume(source(), store, ack, batch_size=1), 2)
    assert closed == [True]
    report = await consume([Delivery(message, "replayed")], store, ack)
    assert report.replayed == report.acknowledged == 1
    assert read_state(store.database)[1] == [("views", 1)]
