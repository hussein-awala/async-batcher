from __future__ import annotations

import asyncio
import json
import math
import tempfile
import tracemalloc
import unittest
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path
from unittest.mock import Mock, patch

from benchmarks.core import Case, Config, Operation, experiment, main, percentiles, sample_lag, trial


class MetricTests(unittest.TestCase):
    def test_percentiles_interpolate_and_do_not_hide_missing_samples(self):
        result = percentiles([4, 1, 3, 2])
        self.assertEqual(result["p50"], 2.5)
        self.assertAlmostEqual(result["p95"], 3.85)
        self.assertAlmostEqual(result["p99"], 3.97)
        self.assertEqual(result["max"], 4)
        self.assertEqual(percentiles([]), {"samples": 0, "p50": None, "p95": None, "p99": None, "max": None})

    def test_invalid_or_unbounded_settings_are_rejected(self):
        for values in (
            {"requests": 0},
            {"warmup": 0},
            {"outstanding": 0},
            {"repetitions": 0},
            {"batch_sizes": (0,)},
            {"concurrencies": (1, 1)},
            {"workloads": ()},
            {"fixed_delay": math.inf},
            {"queue_time": math.nan},
            {"timeout": 0},
            {"fixed_delay": 31},
        ):
            with self.subTest(values=values), self.assertRaises(ValueError):
                replace(Config(), **values).validate()


class WorkloadTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.config = Config(
            requests=12,
            warmup=8,
            repetitions=1,
            outstanding=4,
            batch_sizes=(4,),
            concurrencies=(2,),
            fixed_delay=0.001,
            per_item_delay=0,
            queue_time=0.001,
            lag_interval=0.001,
        )

    async def test_direct_and_batched_workloads_complete_identical_requests(self):
        for workload in ("async", "thread"):
            with ThreadPoolExecutor(max_workers=2) as executor:
                direct = await trial(self.config, Case(workload, 2, None), 12, executor)
                batch = await trial(self.config, Case(workload, 2, 4), 12, executor)
            for result in (direct, batch):
                self.assertEqual(result["requests"], 12)
                self.assertEqual(result["latency_ms"]["samples"], 12)
                self.assertEqual(result["request_worker_tasks"], 4)
                self.assertGreater(result["elapsed_seconds"], 0)
                self.assertGreater(result["requests_per_second"], 0)
                self.assertLessEqual(result["latency_ms"]["p50"], result["latency_ms"]["p99"])
                for metric in (result["latency_ms"], result["event_loop_lag_ms"]):
                    self.assertTrue(all(value is None or math.isfinite(value) for value in metric.values()))
            self.assertEqual(direct["operation_calls"], 12)
            self.assertEqual(direct["batch_items"]["max"], 1)
            self.assertEqual(batch["operation_calls"], 3)
            self.assertEqual(batch["batch_items"]["max"], 4)

    async def test_partial_batches_respect_outstanding_limit(self):
        config = replace(self.config, outstanding=2, batch_sizes=(8,))
        result = await trial(config, Case("async", 1, 8), 3)
        self.assertEqual(result["request_worker_tasks"], 2)
        self.assertEqual(result["batch_items"]["max"], 2)
        self.assertEqual(result["operation_calls"], 2)

    async def test_lag_measures_delay_beyond_scheduled_interval(self):
        clock = Mock()
        clock.time.side_effect = [0.0, 0.008, 0.008, 0.016, 0.016]
        sleeps = 0

        async def sleep(interval):
            nonlocal sleeps
            self.assertEqual(interval, 0.005)
            sleeps += 1
            if sleeps == 3:
                raise asyncio.CancelledError

        samples = []
        ready = asyncio.Event()
        with patch("benchmarks.core.asyncio.get_running_loop", return_value=clock):
            with patch("benchmarks.core.asyncio.sleep", side_effect=sleep):
                with self.assertRaises(asyncio.CancelledError):
                    await sample_lag(0.005, samples, ready)
        self.assertTrue(ready.is_set())
        self.assertEqual(len(samples), 2)
        for sample in samples:
            self.assertAlmostEqual(sample, 3.0)

    async def test_failure_does_not_leave_worker_or_sampler_tasks(self):
        before = set(asyncio.all_tasks())
        with patch.object(Operation, "asynchronous", side_effect=RuntimeError("operation failed")):
            with self.assertRaisesRegex(RuntimeError, "operation failed"):
                await trial(self.config, Case("async", 2, None), 12)
        self.assertEqual(set(asyncio.all_tasks()), before)

    async def test_failed_memory_pass_stops_tracing_and_batcher_tasks(self):
        before = set(asyncio.all_tasks())
        with patch.object(Operation, "asynchronous", side_effect=RuntimeError("operation failed")):
            with self.assertLogs("async_batcher.batcher", level="ERROR"):
                with self.assertRaisesRegex(RuntimeError, "operation failed"):
                    await trial(self.config, Case("async", 2, 4), 12, memory=True)
        self.assertFalse(tracemalloc.is_tracing())
        self.assertEqual(set(asyncio.all_tasks()), before)

    async def test_report_separates_timing_warmup_and_memory(self):
        config = replace(self.config, workloads=("async",))
        report = await experiment(config)
        self.assertEqual(len(report["cases"]), 2)
        for result in report["cases"].values():
            self.assertEqual(len(result["timing_trials"]), 1)
            self.assertIsNone(result["timing_trials"][0]["peak_traced_bytes"])
            self.assertGreater(result["memory_trial"]["peak_traced_bytes"], 0)
            self.assertEqual(result["memory_trial"]["requests"], 12)
        json.dumps(report, allow_nan=False)


class CommandTests(unittest.TestCase):
    def test_cli_writes_complete_report(self):
        with tempfile.TemporaryDirectory() as temp, patch("builtins.print"):
            output = Path(temp) / "report.json"
            main(
                [
                    "--requests",
                    "4",
                    "--warmup",
                    "4",
                    "--repetitions",
                    "1",
                    "--outstanding",
                    "4",
                    "--batch-sizes",
                    "4",
                    "--concurrencies",
                    "1",
                    "--workloads",
                    "async",
                    "--output",
                    str(output),
                ]
            )
            report = json.loads(output.read_text())
        self.assertEqual(report["schema_version"], 1)
        self.assertEqual(len(report["trial_order"]), 4)
        self.assertIn("python", report["environment"])


if __name__ == "__main__":
    unittest.main()
