from __future__ import annotations

import asyncio
import json
import math
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import AsyncMock, call, patch

from benchmarks.sqlite_open_loop import (
    Config,
    SQLiteWriter,
    arrivals,
    experiment,
    summarize,
    trial,
    wait_until_due,
)


class ScheduleTests(unittest.TestCase):
    def test_arrivals_keep_absolute_offsets_and_bursts(self):
        config = Config(duration=0.5, sparse_rate=10, burst_rate=10, burst_size=3)
        self.assertEqual(list(arrivals(config, "sparse")), [(0, 0), (1, 0.1), (2, 0.2), (3, 0.3), (4, 0.4)])
        self.assertEqual(list(arrivals(config, "burst")), [(0, 0), (1, 0), (2, 0), (3, 0.3), (4, 0.3)])

    def test_invalid_or_unbounded_settings_fail(self):
        for values in (
            {"duration": 0},
            {"sparse_rate": math.nan},
            {"queue_time": -1},
            {"outstanding": 0},
            {"warmup": 129},
            {"keys": 0},
            {"hot_fraction": 1.1},
            {"repetitions": 0},
            {"saturated_rate": 1_000_000},
            {"drain_timeout": math.inf},
        ):
            with self.subTest(values=values), self.assertRaises(ValueError):
                replace(Config(), **values).validate()

    def test_summary_includes_scheduled_delay_rejection_and_drain(self):
        records = [
            {
                "key": 0,
                "scheduled": 0,
                "launched": 0.1,
                "started": 0.2,
                "completed": 0.3,
                "status": "completed",
            },
            {
                "key": 1,
                "scheduled": 0.5,
                "launched": 0.6,
                "started": 0.8,
                "completed": 1.1,
                "status": "completed",
            },
            {"key": 0, "scheduled": 0.7, "launched": 0.9, "status": "rejected"},
        ]
        summary = summarize(records, 1, 1.1, [{"size": 2}], 2, [])
        self.assertEqual((summary["offered"], summary["completed"], summary["rejected"]), (3, 2, 1))
        self.assertAlmostEqual(summary["latency_ms"]["p50"], 450)
        self.assertAlmostEqual(summary["queue_ms"]["p50"], 150)
        self.assertAlmostEqual(summary["drain_seconds"], 0.1)
        self.assertEqual(summary["completed_within_arrival_window"], 1)
        self.assertEqual(summary["per_key"]["0"]["rejected"], 1)
        self.assertEqual(summary["event_loop_lag_ms"]["samples"], 0)
        self.assertIsNone(summary["event_loop_lag_ms"]["p99"])
        with self.assertRaisesRegex(RuntimeError, "did not settle"):
            summarize([{"status": "pending"}], 1, 1, [], 1, [])


class DeadlineTests(unittest.IsolatedAsyncioTestCase):
    async def test_early_timer_wakeup_cannot_emit_an_early_arrival(self):
        with patch("benchmarks.sqlite_open_loop.time.perf_counter", side_effect=[0.0, 0.4, 1.0]):
            with patch("benchmarks.sqlite_open_loop.asyncio.sleep", new_callable=AsyncMock) as sleep:
                await wait_until_due(1.0)
        self.assertEqual(sleep.await_args_list, [call(1.0), call(0.6)])

    async def test_late_arrival_keeps_original_deadline_without_added_delay(self):
        with patch("benchmarks.sqlite_open_loop.time.perf_counter", return_value=2.0):
            with patch("benchmarks.sqlite_open_loop.asyncio.sleep", new_callable=AsyncMock) as sleep:
                await wait_until_due(1.0)
        sleep.assert_not_awaited()


class SQLiteTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.config = Config(
            duration=0.02,
            repetitions=1,
            outstanding=8,
            warmup=4,
            batch_size=4,
            sparse_rate=100,
            burst_rate=600,
            saturated_rate=600,
            grouped=True,
        )
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.database = Path(self.directory.name) / "rows.sqlite"

    async def test_modes_share_bounded_admission_and_verify_real_database_contents(self):
        before = asyncio.all_tasks()
        for mode in ("direct", "application", "library", "grouped"):
            with self.subTest(mode=mode):
                result = await trial(self.config, mode, "burst", self.database)
                summary = result["summary"]
                # All twelve arrivals share one timestamp, before any transaction
                # can complete. The common limit accepts eight in every mode.
                self.assertEqual((summary["offered"], summary["completed"], summary["rejected"]), (12, 8, 4))
                self.assertEqual(summary["peak_outstanding"], 8)
                self.assertGreaterEqual(summary["elapsed_seconds"], self.config.duration)
                self.assertEqual(result["persisted_rows_verified"], 8)
                self.assertEqual(sum(call["size"] for call in result["transactions"]), 8)
                self.assertEqual(result["sqlite_settings"]["journal_mode"], "wal")
                self.assertEqual(result["sqlite_settings"]["synchronous"], 2)
                self.assertLessEqual(summary["batch_sizes"]["max"], 1 if mode == "direct" else 4)
                self.assertEqual(sum(row["completed"] for row in summary["per_key"].values()), 8)
                for row in result["requests"]:
                    if row["status"] == "completed":
                        self.assertLessEqual(row["scheduled"], row["launched"])
                        self.assertLessEqual(row["launched"], row["started"])
                        self.assertLessEqual(row["started"], row["completed"])
                json.dumps(result, allow_nan=False)
                self.database.unlink()
                self.assertEqual(asyncio.all_tasks(), before)

    async def test_warmup_failure_closes_worker_connection_and_tasks(self):
        before = asyncio.all_tasks()
        for mode in ("direct", "application", "library", "grouped"):
            with self.subTest(mode=mode):
                with patch.object(SQLiteWriter, "write", side_effect=RuntimeError("write failed")):
                    with self.assertRaisesRegex(RuntimeError, "write failed"):
                        await trial(self.config, mode, "burst", self.database)
                self.database.unlink()
                self.assertEqual(asyncio.all_tasks(), before)

    async def test_cancelled_trial_cleans_up_pending_warmup(self):
        before = asyncio.all_tasks()
        started = asyncio.Event()

        async def blocked(_writer, _rows):
            started.set()
            await asyncio.Event().wait()

        with patch.object(SQLiteWriter, "process", blocked):
            task = asyncio.create_task(trial(self.config, "application", "burst", self.database))
            await started.wait()
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
        self.database.unlink()
        self.assertEqual(asyncio.all_tasks(), before)

    async def test_misrouted_results_fail_and_release_measured_tasks(self):
        original = SQLiteWriter.write
        before = asyncio.all_tasks()

        def misroute(writer, rows):
            results = original(writer, rows)
            if rows[0].identifier >= 0:
                return [(identifier + 1, started, finished) for identifier, started, finished in results]
            return results

        for mode in ("direct", "application", "library", "grouped"):
            with self.subTest(mode=mode), patch.object(SQLiteWriter, "write", misroute):
                with self.assertRaisesRegex(RuntimeError, "wrong request ID"):
                    await trial(self.config, mode, "burst", self.database)
            self.database.unlink()
            self.assertEqual(asyncio.all_tasks(), before)

    async def test_persisted_row_mismatch_fails_instead_of_reporting_performance(self):
        with patch.object(SQLiteWriter, "contents", return_value=[]):
            with self.assertRaisesRegex(RuntimeError, "persisted SQLite rows"):
                await trial(self.config, "library", "burst", self.database)

    async def test_report_contains_all_profiles_and_a_replayable_order(self):
        config = replace(self.config, duration=0.005, sparse_rate=200, burst_rate=200, saturated_rate=200)
        report = await experiment(config, self.directory.name)
        self.assertEqual(len(report["cases"]), 12)
        self.assertEqual(len(report["trial_order"]), 12)
        self.assertEqual({entry["case"] for entry in report["trial_order"]}, set(report["cases"]))
        self.assertTrue(report["environment"]["sqlite_version"])
        self.assertEqual(report["config"]["outstanding"], 8)
        self.assertTrue(all(len(trials) == 1 for trials in report["cases"].values()))
        self.assertEqual(list(Path(self.directory.name).iterdir()), [])
        json.dumps(report, allow_nan=False)


if __name__ == "__main__":
    unittest.main()
