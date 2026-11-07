from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path

import pytest
from examples.recipes.durable_embeddings import Identity, Spool, deliver_pending, stage_file
from examples.recipes.embedding_vectors import SQLiteVectorSink
from examples.recipes.resumable_sqlite import Source


def source_file(tmp_path, rows=None):
    path = tmp_path / "documents.jsonl"
    path.write_text(
        "".join(
            json.dumps({"id": identifier, "text": text}) + "\n"
            for identifier, text in (rows or [("a", "first"), ("b", "second")])
        )
    )
    return path


def identity():
    return Identity(
        "test", "chosen-model", "revision-1", "normalize-v2", "search_document", 2, "tenant/index"
    )


class Model:
    def __init__(self):
        self.texts = []

    async def embed(self, texts):
        self.texts.extend(texts)
        return [[float(len(text)), 1.0] for text in texts]


@pytest.mark.asyncio
async def test_restart_reuses_saved_embeddings_and_delivered_rows_without_repeat_calls(tmp_path):
    path = source_file(tmp_path)
    spool = Spool(tmp_path / "spool.sqlite", "job", identity())
    sink = SQLiteVectorSink(tmp_path / "sink.sqlite")
    sink.initialize()
    model = Model()
    with ThreadPoolExecutor(max_workers=2) as pool:
        assert (await stage_file(path, spool, model, executor=pool))["pending_vectors"] == 2
        result = await deliver_pending(spool, sink, executor=pool)
        assert result == {"persisted_embeddings": 2, "acknowledged_vectors": 2, "pending_vectors": 0}
        await stage_file(path, spool, model, executor=pool, batch_size=1)
        await deliver_pending(spool, sink, executor=pool)
    assert model.texts == ["first", "second"]


@pytest.mark.asyncio
async def test_ambiguous_sink_ack_replays_identical_vectors_without_embedding_again(tmp_path):
    path = source_file(tmp_path)
    spool = Spool(tmp_path / "spool.sqlite", "job", identity())
    native = SQLiteVectorSink(tmp_path / "sink.sqlite")
    native.initialize()
    calls = []

    class LostAck:
        async def upsert(self, records):
            calls.append([(record.id, list(record.values)) for record in records])
            await native.upsert(records)
            raise ConnectionError("Response lost after remote commit")

    class RecordingSink:
        async def upsert(self, records):
            calls.append([(record.id, list(record.values)) for record in records])
            return await native.upsert(records)

    model = Model()
    with ThreadPoolExecutor(max_workers=2) as pool:
        await stage_file(path, spool, model, executor=pool)
        with pytest.raises(ConnectionError):
            await deliver_pending(spool, LostAck(), executor=pool)
        assert spool.counts()["pending_vectors"] == 2
        await stage_file(path, spool, model, executor=pool)
        await deliver_pending(spool, RecordingSink(), executor=pool)
    assert calls[0] == calls[1]
    assert model.texts == ["first", "second"]
    assert spool.counts()["pending_vectors"] == 0


@pytest.mark.asyncio
async def test_real_crash_after_durable_save_does_not_reembed_saved_prefix(tmp_path):
    path = source_file(tmp_path)
    database = tmp_path / "spool.sqlite"
    script = """
import asyncio, os, sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from examples.recipes.durable_embeddings import Identity, Spool, stage_file
class Model:
    async def embed(self, texts):
        return [[len(text), 1] for text in texts]
identity = Identity(
    "test", "chosen-model", "revision-1", "normalize-v2", "search_document", 2, "tenant/index"
)
spool = Spool(Path(sys.argv[2]), "job", identity)
original = spool.save
def crash(records, vectors):
    original(records, vectors)
    os._exit(23)
spool.save = crash
with ThreadPoolExecutor(max_workers=2) as pool:
    asyncio.run(stage_file(Path(sys.argv[1]), spool, Model(), executor=pool, batch_size=1, max_pending=1))
"""
    result = await asyncio.to_thread(
        subprocess.run,
        [sys.executable, "-B", "-c", script, str(path), str(database)],
        env=os.environ | {"PYTHONPATH": str(Path(__file__).resolve().parents[2])},
        capture_output=True,
        timeout=15,
        check=False,
    )
    assert result.returncode == 23, result.stderr.decode()
    spool = Spool(database, "job", identity())
    assert spool.counts()["persisted_embeddings"] == 1
    model = Model()
    with ThreadPoolExecutor(max_workers=2) as pool:
        await stage_file(path, spool, model, executor=pool)
    assert model.texts == ["second"]
    assert spool.counts()["persisted_embeddings"] == 2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value",
    [
        ("revision", "v2"),
        ("dimension", 3),
        ("destination", "other"),
        ("preprocessing", "different"),
        ("input_type", "query"),
    ],
)
async def test_configuration_changes_are_rejected_before_any_model_call(tmp_path, field, value):
    path = source_file(tmp_path)
    database = tmp_path / "spool.sqlite"
    Spool(database, "job", identity()).initialize(Source.inspect(path))
    model = Model()
    changed = Spool(database, "job", replace(identity(), **{field: value}))
    with ThreadPoolExecutor(max_workers=2) as pool:
        with pytest.raises(ValueError, match="identity or source"):
            await stage_file(path, changed, model, executor=pool)
    assert model.texts == []


@pytest.mark.asyncio
async def test_changed_contents_and_duplicate_ids_are_rejected(tmp_path):
    path = source_file(tmp_path)
    spool = Spool(tmp_path / "spool.sqlite", "job", identity())
    spool.initialize(Source.inspect(path))
    path.write_text('{"id":"x","text":"changed"}\n')
    with ThreadPoolExecutor(max_workers=2) as pool:
        with pytest.raises(ValueError, match="identity or source"):
            await stage_file(path, spool, Model(), executor=pool)
        path = source_file(tmp_path, [("duplicate", "one"), ("duplicate", "two")])
        fresh = Spool(tmp_path / "spool.sqlite", "duplicates", identity())
        model = Model()
        with pytest.raises(ValueError, match="unique"):
            await stage_file(path, fresh, model, executor=pool)
        assert model.texts == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "rows",
    [[[1, 2]], [[1, 2], [float("nan"), 2]], [[1, 2], [True, 2]], [[1, 2], [1]], [[1, 2], [10**400, 2]]],
)
async def test_malformed_model_response_persists_no_valid_prefix(tmp_path, rows):
    path = source_file(tmp_path)
    spool = Spool(tmp_path / "spool.sqlite", "job", identity())

    class BadModel:
        async def embed(self, texts):
            return rows

    with ThreadPoolExecutor(max_workers=2) as pool:
        with pytest.raises(ValueError):
            await stage_file(path, spool, BadModel(), executor=pool)
    assert spool.counts()["persisted_embeddings"] == 0


@pytest.mark.asyncio
async def test_bad_sink_ack_does_not_mark_rows_delivered(tmp_path):
    path = source_file(tmp_path)
    spool = Spool(tmp_path / "spool.sqlite", "job", identity())

    class BadSink:
        async def upsert(self, records):
            return [records[-1].id]

    with ThreadPoolExecutor(max_workers=2) as pool:
        await stage_file(path, spool, Model(), executor=pool)
        with pytest.raises(ValueError, match="acknowledge"):
            await deliver_pending(spool, BadSink(), executor=pool)
    assert spool.counts()["pending_vectors"] == 2


@pytest.mark.asyncio
async def test_cancelled_delivery_is_bounded_and_drains_accepted_work(tmp_path):
    path = source_file(tmp_path, [(str(i), str(i)) for i in range(30)])
    spool = Spool(tmp_path / "spool.sqlite", "job", identity())
    started, release = asyncio.Event(), asyncio.Event()
    calls = []

    class Sink:
        async def upsert(self, records):
            calls.extend(record.id for record in records)
            started.set()
            await release.wait()
            return [record.id for record in records]

    with ThreadPoolExecutor(max_workers=2) as pool:
        await stage_file(path, spool, Model(), executor=pool)
        task = asyncio.create_task(deliver_pending(spool, Sink(), executor=pool, batch_size=1, max_pending=2))
        try:
            await asyncio.wait_for(started.wait(), 3)
            task.cancel()
            release.set()
            with pytest.raises(asyncio.CancelledError):
                await task
        finally:
            release.set()
    assert 1 <= len(calls) <= 2
    assert spool.counts()["acknowledged_vectors"] == len(calls)


def test_cli_is_runnable_and_empty_source_never_calls_a_model(tmp_path, monkeypatch, capsys):
    from examples.recipes.durable_embeddings import main

    path = tmp_path / "empty.jsonl"
    path.write_text("")
    monkeypatch.setattr(
        "sys.argv",
        [
            "durable_embeddings",
            "--input",
            str(path),
            "--spool",
            str(tmp_path / "spool.sqlite"),
            "--sink",
            str(tmp_path / "sink.sqlite"),
            "--job-id",
            "empty",
        ],
    )
    main()
    assert json.loads(capsys.readouterr().out) == {
        "persisted_embeddings": 0,
        "acknowledged_vectors": 0,
        "pending_vectors": 0,
    }


@pytest.mark.asyncio
async def test_cancelled_staging_drains_and_preserves_the_committed_embedding(tmp_path):
    path = source_file(tmp_path)
    spool = Spool(tmp_path / "spool.sqlite", "job", identity())
    entered, release = asyncio.Event(), asyncio.Event()

    class HeldModel(Model):
        async def embed(self, texts):
            entered.set()
            await release.wait()
            return await super().embed(texts)

    model = HeldModel()
    with ThreadPoolExecutor(max_workers=2) as pool:
        task = asyncio.create_task(stage_file(path, spool, model, executor=pool, batch_size=1, max_pending=1))
        try:
            await asyncio.wait_for(entered.wait(), 3)
            task.cancel()
            release.set()
            with pytest.raises(asyncio.CancelledError):
                await task
        finally:
            release.set()
        fresh = Model()
        await stage_file(path, spool, fresh, executor=pool)
    assert model.texts == ["first"]
    assert fresh.texts == ["second"]


@pytest.mark.asyncio
async def test_delivery_refuses_changed_identity_without_calling_sink(tmp_path):
    path = source_file(tmp_path)
    spool = Spool(tmp_path / "spool.sqlite", "job", identity())
    with ThreadPoolExecutor(max_workers=2) as pool:
        await stage_file(path, spool, Model(), executor=pool)
        changed = Spool(spool.database, "job", replace(identity(), destination="other"))
        with pytest.raises(ValueError, match="matching job identity"):
            await deliver_pending(changed, object(), executor=pool)


@pytest.mark.parametrize("change", [{"model": ""}, {"revision": " "}, {"dimension": True}, {"dimension": 0}])
def test_identity_requires_explicit_configuration(tmp_path, change):
    with pytest.raises(ValueError):
        Spool(tmp_path / "spool.sqlite", "job", replace(identity(), **change))


@pytest.mark.asyncio
@pytest.mark.parametrize("options", [{"batch_size": -1}, {"max_pending": 0}, {"max_pending": True}])
async def test_invalid_bounds_fail_before_input_or_clients_are_used(tmp_path, options):
    with ThreadPoolExecutor(max_workers=1) as pool:
        with pytest.raises(ValueError, match="positive integer"):
            await stage_file(tmp_path / "absent", object(), object(), executor=pool, **options)
        with pytest.raises(ValueError, match="positive integer"):
            await deliver_pending(object(), object(), executor=pool, **options)
