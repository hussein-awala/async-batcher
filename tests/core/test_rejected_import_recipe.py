from __future__ import annotations

import asyncio
import json
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing

import pytest
from examples.recipes.rejected_import import ImportStore, import_file, read_rows
from examples.recipes.resumable_sqlite import Source, SourceChanged


def source_file(tmp_path, content):
    path = tmp_path / "input.jsonl"
    path.write_bytes(content)
    return path


def saved(database):
    with closing(sqlite3.connect(database)) as connection:
        return (
            connection.execute(
                "SELECT start, end, rejection FROM rejected_import_rows ORDER BY start"
            ).fetchall(),
            connection.execute("SELECT id, text FROM rejected_import_documents ORDER BY id").fetchall(),
        )


@pytest.mark.asyncio
async def test_mixed_import_commits_good_rows_and_metadata_only_rejections(tmp_path):
    first = '{"id":"same","text":"été 🌎"}\r\n'.encode()
    bad = b'{"private":"DO-NOT-STORE"}\n'
    last = b'{"id":"same","text":"last"}'
    path = source_file(tmp_path, first + bad + last)
    database = tmp_path / "rows.sqlite"
    with ThreadPoolExecutor(max_workers=2) as pool:
        report = await import_file(path, database, "run", executor=pool, batch_size=1, max_pending=2)
    rows, documents = saved(database)
    assert report == {"run_id": "run", "accepted_records": 2, "rejected_records": 1}
    assert documents == [("same", "last")]
    assert rows == [
        (0, len(first), None),
        (len(first), len(first + bad), "invalid_document_fields"),
        (len(first + bad), len(first + bad + last), None),
    ]
    with closing(sqlite3.connect(database)) as connection:
        dump = "\n".join(connection.iterdump())
    assert "DO-NOT-STORE" not in dump


@pytest.mark.parametrize(
    "bad,reason",
    [
        (b"\xff\n", "invalid_utf8_or_json"),
        (b"\n", "invalid_utf8_or_json"),
        (b'{"id":"","text":"x"}\n', "invalid_document_fields"),
        (b"x" * 10000 + b"\n", "line_too_large"),
    ],
)
def test_bad_line_is_one_rejection_and_next_record_boundary_is_preserved(tmp_path, bad, reason):
    good = b'{"id":"ok","text":"x"}\n'
    path = source_file(tmp_path, bad + good)
    source = Source.inspect(path)
    first = read_rows(source, 0, chunk_size=1, max_line_bytes=32)
    assert len(first) == 1 and first[0].rejection == reason and first[0].end == len(bad)
    following = read_rows(source, first[0].end, max_line_bytes=32)
    assert following[0].document.id == "ok"
    assert following[0].start == len(bad)


@pytest.mark.asyncio
async def test_source_is_immutable_empty_runs_are_valid_and_run_ids_are_not_reused(tmp_path):
    path = source_file(tmp_path, b"")
    database = tmp_path / "rows.sqlite"
    with ThreadPoolExecutor(max_workers=2) as pool:
        assert (await import_file(path, database, "empty", executor=pool))["accepted_records"] == 0
        with pytest.raises(sqlite3.IntegrityError):
            await import_file(path, database, "empty", executor=pool)
    source = Source.inspect(path)
    path.write_bytes(b"changed")
    with pytest.raises(SourceChanged):
        read_rows(source, 0)


def test_database_failure_rolls_back_a_whole_batch_instead_of_quarantining_it(tmp_path):
    path = source_file(tmp_path, b'{"id":"one","text":"ok"}\n{}\n')
    source = Source.inspect(path)
    store = ImportStore(tmp_path / "rows.sqlite", "run", source)
    store.initialize()
    rows = read_rows(source, 0)
    with closing(store.connect()) as connection, connection:
        connection.execute(
            "CREATE TRIGGER reject_audit BEFORE INSERT ON rejected_import_rows "
            "WHEN NEW.rejection IS NOT NULL BEGIN SELECT RAISE(ABORT, 'disk policy'); END"
        )
    with pytest.raises(sqlite3.IntegrityError):
        store.write(rows)
    assert saved(store.database) == ([], [])


@pytest.mark.asyncio
async def test_cancellation_bounds_read_ahead_and_drains_accepted_writes(tmp_path, monkeypatch):
    path = source_file(
        tmp_path, b"".join(json.dumps({"id": str(i), "text": "x"}).encode() + b"\n" for i in range(100))
    )
    database = tmp_path / "rows.sqlite"
    entered, release = threading.Event(), threading.Event()
    original = ImportStore.write
    calls = []

    def blocked(self, rows):
        calls.append(len(rows))
        entered.set()
        assert release.wait(5)
        return original(self, rows)

    monkeypatch.setattr(ImportStore, "write", blocked)
    with ThreadPoolExecutor(max_workers=2) as pool:
        task = asyncio.create_task(
            import_file(path, database, "run", executor=pool, batch_size=1, max_pending=2)
        )
        try:
            assert await asyncio.to_thread(entered.wait, 5)
            task.cancel()
            release.set()
            with pytest.raises(asyncio.CancelledError):
                await task
        finally:
            release.set()
    rows, _ = saved(database)
    assert 1 <= len(rows) <= 2
    assert len(calls) == len(rows)


@pytest.mark.asyncio
@pytest.mark.parametrize("options", [{"max_pending": 0}, {"batch_size": -1}, {"batch_size": True}])
async def test_invalid_limits_do_not_read_source_or_create_database(tmp_path, options):
    database = tmp_path / "absent.sqlite"
    with ThreadPoolExecutor(max_workers=1) as pool:
        with pytest.raises(ValueError, match="positive integer"):
            await import_file(tmp_path / "absent.jsonl", database, "run", executor=pool, **options)
    assert not database.exists()


def test_cli_reports_real_accepted_and_rejected_counts(tmp_path, monkeypatch, capsys):
    from examples.recipes.rejected_import import main

    path = source_file(tmp_path, b'{"id":"ok","text":"x"}\n{}\n')
    database = tmp_path / "cli.sqlite"
    monkeypatch.setattr(
        "sys.argv", ["rejected_import", "--input", str(path), "--database", str(database), "--run-id", "cli"]
    )
    main()
    assert json.loads(capsys.readouterr().out) == {
        "run_id": "cli",
        "accepted_records": 1,
        "rejected_records": 1,
    }
    assert saved(database)[1] == [("ok", "x")]
