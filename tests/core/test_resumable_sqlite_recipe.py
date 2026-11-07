from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from pathlib import Path
from types import SimpleNamespace

import pytest
from examples.recipes.resumable_sqlite import (
    CheckpointConflict,
    CheckpointWriter,
    Record,
    Source,
    SourceChanged,
    import_jsonl,
    read_chunk,
)
from examples.recipes.support import Document


def make_source(tmp_path, documents, *, newline=True):
    path = tmp_path / "source.jsonl"
    content = "\n".join(json.dumps(document, ensure_ascii=False) for document in documents)
    path.write_text(content + ("\n" if newline and documents else ""), encoding="utf-8")
    return path


def saved(database):
    with closing(sqlite3.connect(database)) as connection:
        checkpoint = connection.execute("SELECT next_offset, processed_rows FROM import_jobs").fetchone()
        documents = dict(connection.execute("SELECT id, text FROM import_documents"))
    return checkpoint, documents


@pytest.mark.asyncio
async def test_resume_preserves_utf8_offsets_duplicate_order_and_last_line(tmp_path):
    path = make_source(
        tmp_path,
        [{"id": "a", "text": "été 🌎"}, {"id": "b", "text": "second"}, {"id": "a", "text": "last"}],
        newline=False,
    )
    source = Source.inspect(path)
    database = tmp_path / "import.sqlite"
    writer = CheckpointWriter(database, "job", source)
    assert writer.initialize() == (0, 0)
    first = read_chunk(source, 0, chunk_size=1)
    assert first[0].end == len(path.read_bytes().splitlines(keepends=True)[0])
    assert writer.write(first) == ["a"]
    with ThreadPoolExecutor(max_workers=2) as executor:
        report = await import_jsonl(path, database, "job", executor=executor, batch_size=2)
        again = await import_jsonl(path, database, "job", executor=executor)
        assert executor.submit(lambda: "still caller-owned").result() == "still caller-owned"
    assert saved(database) == ((source.size, 3), {"a": "last", "b": "second"})
    assert report["previously_processed"] == 1
    assert report["completed_this_run"] == report["stats"]["succeeded"] == 2
    assert sum(size * count for size, count in report["batch_size_histogram"].items()) == 2
    assert again["completed_this_run"] == 0
    assert again["processed_rows"] == 3


@pytest.mark.asyncio
async def test_real_process_crash_after_commit_resumes_from_atomic_checkpoint(tmp_path):
    path = make_source(tmp_path, [{"id": str(i), "text": f"row {i}"} for i in range(5)])
    database = tmp_path / "import.sqlite"
    # Exit in the worker after the database commit, before AsyncBatcher can deliver
    # that batch's results. This is a real process crash, not a raised exception.
    script = """
import asyncio, os, sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from examples.recipes.resumable_sqlite import CheckpointWriter, import_jsonl
original = CheckpointWriter.write
def crash_after_commit(self, records):
    original(self, records)
    os._exit(23)
CheckpointWriter.write = crash_after_commit
with ThreadPoolExecutor(max_workers=2) as executor:
    asyncio.run(import_jsonl(Path(sys.argv[1]), Path(sys.argv[2]), 'job',
                            executor=executor, batch_size=2, max_pending=2))
"""
    environment = os.environ | {"PYTHONPATH": str(Path(__file__).resolve().parents[2])}
    crashed = await asyncio.to_thread(
        subprocess.run,
        [sys.executable, "-c", script, str(path), str(database)],
        env=environment,
        capture_output=True,
        timeout=15,
        check=False,
    )
    assert crashed.returncode == 23, crashed.stderr.decode()
    committed = saved(database)[0][1]
    assert 1 <= committed <= 2
    with ThreadPoolExecutor(max_workers=2) as executor:
        report = await import_jsonl(path, database, "job", executor=executor, batch_size=2)
    assert report["previously_processed"] == committed
    assert report["completed_this_run"] == 5 - committed
    assert saved(database) == ((path.stat().st_size, 5), {str(i): f"row {i}" for i in range(5)})


def test_later_row_failure_rolls_back_rows_and_checkpoint_together(tmp_path):
    path = make_source(tmp_path, [{"id": str(i), "text": str(i)} for i in range(3)])
    source = Source.inspect(path)
    database = tmp_path / "import.sqlite"
    writer = CheckpointWriter(database, "job", source)
    writer.initialize()
    records = read_chunk(source, 0)
    writer.write(records[:1])
    before = saved(database)
    with closing(sqlite3.connect(database)) as connection, connection:
        connection.execute(
            "CREATE TRIGGER fail_later BEFORE INSERT ON import_documents WHEN NEW.id = '2' "
            "BEGIN SELECT RAISE(ABORT, 'simulated row constraint'); END"
        )
    with pytest.raises(sqlite3.IntegrityError, match="simulated row"):
        writer.write(records[1:])
    assert saved(database) == before
    with closing(sqlite3.connect(database)) as connection, connection:
        connection.execute("DROP TRIGGER fail_later")
    writer.write(records[1:])
    assert saved(database) == ((source.size, 3), {"0": "0", "1": "1", "2": "2"})
    with closing(writer._connect()) as connection:
        assert connection.execute("PRAGMA journal_mode").fetchone() == ("wal",)
        assert connection.execute("PRAGMA synchronous").fetchone() == (2,)


def test_changed_source_is_rejected_even_when_size_is_unchanged(tmp_path):
    path = make_source(tmp_path, [{"id": "a", "text": "one"}])
    source = Source.inspect(path)
    database = tmp_path / "import.sqlite"
    writer = CheckpointWriter(database, "job", source)
    writer.initialize()
    records = read_chunk(source, 0)
    path.write_bytes(path.read_bytes().replace(b"one", b"two"))
    with pytest.raises(SourceChanged):
        writer.write(records)
    with pytest.raises(SourceChanged):
        read_chunk(source, 0)
    with pytest.raises(SourceChanged, match="different source contents"):
        CheckpointWriter(database, "job", Source.inspect(path)).initialize()
    assert saved(database) == ((0, 0), {})


def test_copied_identical_source_can_resume_and_stale_writer_cannot_reapply(tmp_path):
    path = make_source(tmp_path, [{"id": "a", "text": "one"}, {"id": "b", "text": "two"}])
    database = tmp_path / "import.sqlite"
    source = Source.inspect(path)
    writer = CheckpointWriter(database, "job", source)
    writer.initialize()
    records = read_chunk(source, 0)
    writer.write(records[:1])
    before = saved(database)
    with pytest.raises(CheckpointConflict):
        writer.write(records)
    with pytest.raises(CheckpointConflict):
        writer.write([Record(records[1].start + 1, records[1].end, records[1].document)])
    assert saved(database) == before
    copied = tmp_path / "copy.jsonl"
    copied.write_bytes(path.read_bytes())
    replacement = CheckpointWriter(database, "job", Source.inspect(copied))
    assert replacement.initialize() == (records[0].end, 1)
    replacement.write(read_chunk(replacement.source, records[0].end))
    assert saved(database)[0] == (source.size, 2)


@pytest.mark.asyncio
async def test_empty_source_and_bounded_reader(tmp_path):
    path = make_source(tmp_path, [])
    with ThreadPoolExecutor(max_workers=1) as executor:
        report = await import_jsonl(path, tmp_path / "import.sqlite", "empty", executor=executor)
    assert report["next_offset"] == report["processed_rows"] == report["completed_this_run"] == 0
    path = make_source(tmp_path, [{"id": str(i), "text": "x"} for i in range(70)])
    source = Source.inspect(path)
    chunk = read_chunk(source, 0)
    assert len(chunk) == 32
    assert chunk[-1].end < source.size


@pytest.mark.parametrize(
    "line, message",
    [
        (b"\n", "Invalid"),
        (b"\xff\n", "Invalid"),
        (b"{}\n", "exactly"),
        (b'{"id":"","text":"x"}\n', "nonempty"),
        (b'{"id":"x","text":1}\n', "string text"),
        (b"x" * 65, "line limit"),
    ],
)
def test_invalid_or_oversized_line_does_not_advance_checkpoint(tmp_path, line, message):
    path = tmp_path / "source.jsonl"
    path.write_bytes(line)
    source = Source.inspect(path)
    database = tmp_path / "import.sqlite"
    writer = CheckpointWriter(database, "job", source)
    writer.initialize()
    with pytest.raises(ValueError, match=message):
        read_chunk(source, 0, max_line_bytes=64)
    assert saved(database) == ((0, 0), {})


def test_invalid_checkpoint_batch_never_writes_a_prefix(tmp_path):
    path = make_source(tmp_path, [{"id": "a", "text": "one"}, {"id": "b", "text": "two"}])
    source = Source.inspect(path)
    database = tmp_path / "import.sqlite"
    writer = CheckpointWriter(database, "job", source)
    writer.initialize()
    records = read_chunk(source, 0)
    with pytest.raises(CheckpointConflict):
        writer.write([records[0], Record(source.size, source.size + 1, Document("bad", "gap"))])
    assert saved(database) == ((0, 0), {})
    assert writer.write([]) == []


def test_path_stat_timestamp_difference_does_not_reject_unchanged_source(tmp_path, monkeypatch):
    path = make_source(tmp_path, [{"id": "a", "text": "unchanged"}])
    original_stat = Path.stat

    def differing_path_stat(self, *args, **kwargs):
        result = original_stat(self, *args, **kwargs)
        if self != path:
            return result
        # Some Windows stat/fstat implementations report different ctime values
        # for the same file. The source guard must compare like-for-like queries.
        return SimpleNamespace(
            st_dev=result.st_dev,
            st_ino=result.st_ino,
            st_size=result.st_size,
            st_mtime_ns=result.st_mtime_ns,
            st_ctime_ns=result.st_ctime_ns + 1,
        )

    monkeypatch.setattr(Path, "stat", differing_path_stat)
    source = Source.inspect(path)
    source.check()
    records = read_chunk(source, 0)
    writer = CheckpointWriter(tmp_path / "import.sqlite", "job", source)
    assert writer.initialize() == (0, 0)
    assert writer.write(records) == ["a"]
    assert saved(writer.database) == ((source.size, 1), {"a": "unchanged"})


def test_replacement_with_identical_bytes_and_mtime_still_rejected(tmp_path):
    path = make_source(tmp_path, [{"id": "a", "text": "original"}])
    source = Source.inspect(path)
    replacement = tmp_path / "replacement.jsonl"
    replacement.write_bytes(path.read_bytes())
    original_stat = path.stat()
    os.utime(replacement, ns=(original_stat.st_atime_ns, original_stat.st_mtime_ns))
    replacement.replace(path)
    with pytest.raises(SourceChanged):
        source.check()
    with pytest.raises(SourceChanged):
        read_chunk(source, 0)


@pytest.mark.asyncio
async def test_cancelled_caller_drains_accepted_write_then_resume_uses_checkpoint(tmp_path, monkeypatch):
    path = make_source(tmp_path, [{"id": str(i), "text": "x"} for i in range(3)])
    database = tmp_path / "import.sqlite"
    entered = asyncio.Event()
    release = threading.Event()
    loop = asyncio.get_running_loop()
    original = CheckpointWriter.write

    def blocked(self, records):
        loop.call_soon_threadsafe(entered.set)
        assert release.wait(3), "test must release the worker"
        return original(self, records)

    monkeypatch.setattr(CheckpointWriter, "write", blocked)
    before = asyncio.all_tasks()
    with ThreadPoolExecutor(max_workers=2) as executor:
        task = asyncio.create_task(
            import_jsonl(path, database, "job", executor=executor, batch_size=1, max_pending=1)
        )
        try:
            await asyncio.wait_for(entered.wait(), 2)
            task.cancel()
            await asyncio.sleep(0)
            assert not task.done()
            release.set()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(task, 2)
            assert saved(database)[0][1] == 1
            report = await import_jsonl(path, database, "job", executor=executor)
            assert report["previously_processed"] == 1
            assert report["processed_rows"] == 3
        finally:
            release.set()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
    assert asyncio.all_tasks() == before
