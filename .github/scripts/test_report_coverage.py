from __future__ import annotations

import importlib.util
import itertools
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from check_adapter import MODULES
from ci_plan import plan
from report_coverage import ADAPTER_PATHS, artifact_files, planned_artifacts, source_scope

SCRIPT = Path(__file__).with_name("report_coverage.py").resolve()


def matrices_env(result):
    return {
        name: json.dumps(result[key])
        for name, key in (
            ("CORE_MATRIX", "core"),
            ("ADAPTER_MATRIX", "adapters"),
            ("SERVICE_MATRIX", "services"),
        )
    }


class CoveragePlanTests(unittest.TestCase):
    def test_mapping_covers_every_installable_adapter(self):
        self.assertEqual(ADAPTER_PATHS.keys(), MODULES.keys())

    def test_full_plan_requires_every_interpreter_platform_and_service(self):
        result = plan("workflow_dispatch", set())
        names, suites = planned_artifacts(result["core"], result["adapters"], result["services"])
        self.assertEqual(
            len(names), sum(len(result[key]["include"]) for key in ("core", "adapters", "services"))
        )
        self.assertEqual(suites, ADAPTER_PATHS.keys())
        self.assertIn("coverage-data-core-macos-15-intel-py3.15", names)
        self.assertIn("coverage-data-adapter-kafka-py3.11", names)
        self.assertIn("coverage-data-adapter-kafka-py3.15", names)
        self.assertIn("coverage-data-service-scylladb", names)

    def test_scoped_plan_keeps_selected_adapter_and_service_artifacts(self):
        result = plan("pull_request", {"kafka"})
        names, suites = planned_artifacts(result["core"], result["adapters"], result["services"])
        self.assertEqual(suites, {"kafka"})
        self.assertEqual(
            set(names),
            {
                "coverage-data-core-ubuntu-24.04-py3.11",
                "coverage-data-core-ubuntu-24.04-py3.15",
                "coverage-data-adapter-kafka-py3.12",
                "coverage-data-service-kafka",
            },
        )

    def test_invalid_plan_cannot_silently_remove_required_collection(self):
        empty = {"include": []}
        core = {"include": [{"os": "ubuntu-24.04", "python": "3.11"}]}
        with self.assertRaisesRegex(ValueError, "core gate"):
            planned_artifacts(empty, empty, empty)
        with self.assertRaisesRegex(ValueError, "Duplicate"):
            planned_artifacts({"include": core["include"] * 2}, empty, empty)
        with self.assertRaisesRegex(ValueError, "Unknown"):
            planned_artifacts(core, {"include": [{"suite": "new-adapter", "python": "3.11"}]}, empty)

    def test_each_missing_or_empty_artifact_is_rejected_even_if_peers_are_present(self):
        result = plan("workflow_dispatch", set())
        names, _ = planned_artifacts(result["core"], result["adapters"], result["services"])
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name in names:
                (root / name).mkdir()
                (root / name / ".coverage.runner").write_bytes(b"coverage data")
            self.assertEqual(len(artifact_files(root, names)), len(names))
            for name in names:
                with self.subTest(name=name):
                    path = root / name / ".coverage.runner"
                    path.unlink()
                    with self.assertRaisesRegex(ValueError, name):
                        artifact_files(root, names)
                    path.touch()
                    with self.assertRaisesRegex(ValueError, name):
                        artifact_files(root, names)
                    path.write_bytes(b"coverage data")

    def test_scopes_include_unexecuted_files_and_shared_ml_helpers_without_unselected_adapters(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            paths = ["async_batcher/__init__.py", "async_batcher/batcher.py", "async_batcher/ml/shared.py"]
            paths += [f"async_batcher/{path}/unexecuted.py" for path in ADAPTER_PATHS.values()]
            for name in paths:
                path = root / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.touch()
            core, label = source_scope(root, set())
            self.assertEqual(core, paths[:2])
            self.assertIn("core only", label)
            keras, label = source_scope(root, {"keras"})
            self.assertEqual(
                set(keras), {*core, "async_batcher/ml/shared.py", "async_batcher/ml/keras/unexecuted.py"}
            )
            self.assertIn("Scoped", label)
            self.assertNotIn("async_batcher/ml/sklearn/unexecuted.py", keras)
            full, label = source_scope(root, set(ADAPTER_PATHS))
            self.assertEqual(set(full), set(paths))
            self.assertIn("Full package", label)
            # Check every combination, not only one-adapter PRs.
            for count in range(len(ADAPTER_PATHS) + 1):
                for selection in itertools.combinations(ADAPTER_PATHS, count):
                    selected, _ = source_scope(root, set(selection))
                    for suite, path in ADAPTER_PATHS.items():
                        self.assertEqual(
                            f"async_batcher/{path}/unexecuted.py" in selected, suite in selection
                        )


@unittest.skipUnless(importlib.util.find_spec("coverage"), "Real report checks require the test group")
class CoverageReportTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        package = self.root / "async_batcher"
        package.mkdir()
        (package / "__init__.py").touch()
        (package / "batcher.py").write_text(
            "def choose(value):\n    if value:\n        return 1\n    return 2\n"
        )
        (package / "unexecuted_adapter").mkdir()
        (package / "unexecuted_adapter" / "extra.py").write_text("missing = 1\n")
        (self.root / "exercise.py").write_text(
            "import sys\nfrom async_batcher.batcher import choose\nchoose(sys.argv[1] == 'yes')\n"
        )
        (self.root / "pyproject.toml").write_text(
            '[tool.coverage.run]\nbranch=true\nrelative_files=true\nsource=["async_batcher"]\n'
        )
        self.plan = plan("pull_request", set())
        self.names, _ = planned_artifacts(self.plan["core"], self.plan["adapters"], self.plan["services"])
        self.env = os.environ | matrices_env(self.plan)
        # Parent collection settings must not redirect this isolated fixture's data.
        self.env.pop("COVERAGE_FILE", None)
        self.env.pop("PYTHONPATH", None)
        self.env["GITHUB_STEP_SUMMARY"] = str(self.root / "step-summary.md")

    def collect(self, choices=("yes", "no")):
        for name, choice in zip(self.names, choices, strict=True):
            target = self.root / "coverage-data" / name
            target.mkdir(parents=True)
            result = subprocess.run(
                [sys.executable, "-m", "coverage", "run", "--parallel-mode", "exercise.py", choice],
                cwd=self.root,
                env=self.env | {"COVERAGE_FILE": str(target / ".coverage")},
                text=True,
                capture_output=True,
            )
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def report(self, *args):
        return subprocess.run(
            [sys.executable, str(SCRIPT), *args], cwd=self.root, env=self.env, text=True, capture_output=True
        )

    def test_real_combine_requires_both_branch_runs_and_writes_all_reports(self):
        self.collect()
        result = self.report()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        output = self.root / "coverage-report"
        report = json.loads((output / "coverage.json").read_text())
        self.assertEqual(report["totals"]["percent_covered"], 100)
        self.assertEqual(set(report["files"]), {"async_batcher/__init__.py", "async_batcher/batcher.py"})
        self.assertTrue((output / "html" / "index.html").is_file())
        self.assertTrue((output / "coverage.xml").is_file())
        self.assertIn("Scoped coverage: core only", (output / "summary.md").read_text())
        self.assertEqual((output / "summary.md").read_text(), (self.root / "step-summary.md").read_text())
        # Parallel input files remain available for diagnosing a failed collection.
        self.assertEqual(len(artifact_files(self.root / "coverage-data", self.names)), 2)

    def test_below_threshold_reports_failure_without_hiding_reports_or_reusing_old_data(self):
        self.collect()
        self.assertEqual(self.report().returncode, 0)
        # Replace both complete inputs with a run of only the true branch.
        first, second = artifact_files(self.root / "coverage-data", self.names)
        second.write_bytes(first.read_bytes())
        result = self.report()
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        report = json.loads((self.root / "coverage-report" / "coverage.json").read_text())
        self.assertLess(report["totals"]["percent_covered"], 95)
        self.assertTrue((self.root / "coverage-report" / "html" / "index.html").exists())

    def test_unmeasured_source_file_is_included_in_the_denominator(self):
        self.collect()
        # This file is absent even from coverage's recorded list of source files.
        (self.root / "async_batcher" / "untouched.py").write_text("first = 1\nsecond = 2\n")
        result = self.report("--fail-under", "0")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        report = json.loads((self.root / "coverage-report" / "coverage.json").read_text())
        untouched = report["files"]["async_batcher/untouched.py"]
        self.assertEqual(untouched["executed_lines"], [])
        self.assertEqual(untouched["missing_lines"], [1, 2])
        self.assertLess(report["totals"]["percent_covered"], 100)

    def test_line_only_artifact_cannot_lower_the_branch_denominator(self):
        from coverage import CoverageData

        self.collect()
        path = artifact_files(self.root / "coverage-data", self.names)[0]
        data = CoverageData(basename=str(path))
        data.erase()
        data.add_lines({"async_batcher/batcher.py": [1, 2, 3]})
        data.write()
        result = self.report()
        self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
        self.assertIn("no executed branch coverage", result.stdout)

    def test_corrupt_artifact_cannot_be_ignored_in_favor_of_valid_peer(self):
        self.collect()
        artifact_files(self.root / "coverage-data", self.names)[0].write_bytes(b"corrupt")
        result = self.report()
        self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
        self.assertIn("Coverage incomplete", (self.root / "coverage-report" / "summary.md").read_text())
        self.assertFalse((self.root / "coverage-report" / "coverage.json").exists())


if __name__ == "__main__":
    unittest.main()
