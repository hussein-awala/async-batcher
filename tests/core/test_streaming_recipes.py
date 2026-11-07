from __future__ import annotations

import asyncio
import json
import math
import sqlite3
from contextlib import closing

import pytest
from examples.recipes import embedding_vectors, sqlite_stream
from examples.recipes.embedding_vectors import DemoEmbeddingModel, SQLiteVectorSink, embed_and_store
from examples.recipes.sqlite_stream import SQLiteWriter, write_stream
from examples.recipes.support import Document, read_jsonl

from tests.core_helpers import wait_until


def read_rows(database, table):
    assert table in {"documents", "vectors"}
    with closing(sqlite3.connect(database)) as connection:
        return dict(connection.execute(f"SELECT * FROM {table}"))


def assert_measurements(report, count):
    assert report["stats"]["accepted"] == report["stats"]["succeeded"] == count
    assert report["stats"]["pending_items"] == report["stats"]["active_batches"] == 0
    assert sum(int(size) * batches for size, batches in report["batch_size_histogram"].items()) == count
    assert sum(report["batch_size_histogram"].values()) == report["stats"]["batches_completed"]


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_stream_commits_real_sqlite_rows_and_reports_measured_batches(tmp_path, asynchronous):
    documents = [Document(str(index), f"text {index}") for index in range(17)]

    async def source():
        for document in documents:
            yield document

    database = tmp_path / "records.sqlite"
    report = await write_stream(
        source() if asynchronous else documents, database, max_pending=5, batch_size=4
    )
    assert read_rows(database, "documents") == {document.id: document.text for document in documents}
    assert_measurements(report, 17)
    assert report["completed_items"] == 17
    assert report["elapsed_seconds"] > 0
    assert math.isfinite(report["items_per_second"])
    assert max(report["batch_size_histogram"]) <= 4


@pytest.mark.asyncio
async def test_stream_preserves_duplicate_id_order_and_cost_budget(tmp_path):
    database = tmp_path / "records.sqlite"
    report = await write_stream(
        [Document("a", "old!"), Document("a", "new!"), Document("b", "")],
        database,
        max_batch_cost=6,
    )
    assert read_rows(database, "documents") == {"a": "new!", "b": ""}
    assert_measurements(report, 3)
    assert report["batch_size_histogram"] == {1: 1, 2: 1}


@pytest.mark.asyncio
async def test_sqlite_batch_rolls_back_without_undoing_earlier_batches(tmp_path):
    database = tmp_path / "records.sqlite"
    writer = SQLiteWriter(database)
    await asyncio.to_thread(writer.initialize)
    await asyncio.to_thread(writer.write, [Document("existing", "kept")])
    with pytest.raises(sqlite3.IntegrityError):
        await asyncio.to_thread(writer.write, [Document("valid", "rolled back"), Document("", "invalid")])
    assert read_rows(database, "documents") == {"existing": "kept"}
    assert await asyncio.to_thread(writer.write, [Document("after", "usable")]) == ["after"]


@pytest.mark.asyncio
async def test_jsonl_reader_uses_real_file_offsets_and_closes_chunks(tmp_path):
    path = tmp_path / "rows.jsonl"
    documents = [{"id": str(index), "text": "été 🌎"} for index in range(7)]
    path.write_text("".join(json.dumps(document, ensure_ascii=False) + "\n" for document in documents))
    source = read_jsonl(path, chunk_size=2)
    assert [document async for document in source] == [Document(**document) for document in documents]
    path.unlink()
    assert not path.exists()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "line, message",
    [(b"x" * 20, "exceeds"), (b"{}\n", "exactly id and text"), (b'{"id":1,"text":"x"}\n', "must be strings")],
)
async def test_jsonl_reader_rejects_oversized_or_malformed_records(tmp_path, line, message):
    path = tmp_path / "rows.jsonl"
    path.write_bytes(line)
    with pytest.raises(ValueError, match=message):
        await anext(read_jsonl(path, max_line_bytes=10 if message == "exceeds" else 64))


@pytest.mark.asyncio
async def test_cancelled_import_closes_async_source_and_retains_already_committed_rows(tmp_path):
    database = tmp_path / "records.sqlite"
    source_closed = asyncio.Event()
    original_tasks = asyncio.all_tasks()

    async def source():
        try:
            yield Document("one", "committed")
            await asyncio.Event().wait()
        finally:
            source_closed.set()

    task = asyncio.create_task(write_stream(source(), database, batch_size=1, max_pending=2))
    try:
        async with asyncio.timeout(2):
            while True:
                if database.exists():
                    try:
                        if await asyncio.to_thread(read_rows, database, "documents"):
                            break
                    except sqlite3.OperationalError:
                        pass  # Initialization and the first transaction are still in flight.
                await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert source_closed.is_set()
        assert read_rows(database, "documents") == {"one": "committed"}
        assert not (asyncio.all_tasks() - original_tasks)
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_demo_pipeline_persists_vectors_and_reports_both_stages(tmp_path):
    database = tmp_path / "vectors.sqlite"
    report = await embedding_vectors.demo(
        [Document("a", "one two 3"), Document("b", "four")], database, batch_size=2, max_pending=3
    )
    assert {key: json.loads(value) for key, value in read_rows(database, "vectors").items()} == {
        "a": [9.0, 3.0, 1.0],
        "b": [4.0, 1.0, 0.0],
    }
    assert "NOT semantic embeddings" in report["workload"]
    assert_measurements(report["embedding"], 2)
    assert_measurements(report["storage"], 2)


@pytest.mark.asyncio
async def test_pipeline_backpressure_bounds_consumed_documents_while_sink_is_blocked():
    consumed = []
    stored = []
    release = asyncio.Event()
    source_closed = asyncio.Event()

    async def source():
        try:
            for index in range(20):
                consumed.append(index)
                yield Document(str(index), "example")
        finally:
            source_closed.set()

    class Sink:
        async def upsert(self, records):
            await release.wait()
            stored.extend(record.id for record in records)
            return [record.id for record in records]

    task = asyncio.create_task(
        embed_and_store(source(), DemoEmbeddingModel(), Sink(), max_pending=2, batch_size=1)
    )
    try:
        await wait_until(lambda: len(consumed) == 4)
        await asyncio.sleep(0)
        assert consumed == [0, 1, 2, 3]  # Two pending items per stage, including completed unyielded results.
        assert stored == []
        release.set()
        report = await asyncio.wait_for(task, 2)
        assert stored == [str(index) for index in range(20)]
        assert source_closed.is_set()
        assert_measurements(report["embedding"], 20)
        assert_measurements(report["storage"], 20)
    finally:
        release.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_pipeline_cancellation_closes_both_maps_and_drains_accepted_sink_work():
    original_tasks = asyncio.all_tasks()
    started = asyncio.Event()
    release = asyncio.Event()
    source_closed = asyncio.Event()
    stored = []

    async def source():
        try:
            for index in range(20):
                yield Document(str(index), "example")
        finally:
            source_closed.set()

    class Sink:
        async def upsert(self, records):
            started.set()
            await release.wait()
            stored.extend(record.id for record in records)
            return [record.id for record in records]

    task = asyncio.create_task(
        embed_and_store(source(), DemoEmbeddingModel(), Sink(), max_pending=2, batch_size=1)
    )
    try:
        await asyncio.wait_for(started.wait(), 2)
        task.cancel()
        await asyncio.wait_for(source_closed.wait(), 2)
        assert not task.done()  # Context exit still owns the accepted sink operation.
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 2)
        assert stored and stored[0] == "0"
        assert not (asyncio.all_tasks() - original_tasks)
    finally:
        release.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("vectors", [[], [[float("nan")]], [[]]])
async def test_invalid_embedding_results_never_reach_storage(tmp_path, vectors):
    class InvalidModel:
        async def embed(self, texts):
            return vectors

    database = tmp_path / "vectors.sqlite"
    sink = SQLiteVectorSink(database)
    await asyncio.to_thread(sink.initialize)
    with pytest.raises(ValueError):
        await embed_and_store([Document("a", "text")], InvalidModel(), sink, batch_size=1)
    assert read_rows(database, "vectors") == {}


@pytest.mark.asyncio
async def test_empty_sources_have_no_batches_and_leave_database_usable(tmp_path):
    records = await write_stream([], tmp_path / "records.sqlite")
    vectors = await embedding_vectors.demo([], tmp_path / "vectors.sqlite")
    assert_measurements(records, 0)
    assert_measurements(vectors["embedding"], 0)
    assert_measurements(vectors["storage"], 0)


@pytest.mark.parametrize("module, table", [(sqlite_stream, "documents"), (embedding_vectors, "vectors")])
def test_recipe_commands_emit_real_measurements(tmp_path, monkeypatch, capsys, module, table):
    database = tmp_path / "demo.sqlite"
    monkeypatch.setattr("sys.argv", ["recipe", "--database", str(database), "--rows", "5"])
    module.main()
    report = json.loads(capsys.readouterr().out)
    assert report["completed_items"] == 5
    assert len(read_rows(database, table)) == 5
    assert report["elapsed_seconds"] > 0
