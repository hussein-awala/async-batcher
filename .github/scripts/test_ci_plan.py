from __future__ import annotations

import importlib.util
import unittest
from fnmatch import fnmatchcase
from pathlib import Path

from check_adapter import MODULES
from ci_plan import plan


class PlanTests(unittest.TestCase):
    def test_docs_pull_request_keeps_core_gate_and_skips_adapters(self):
        result = plan("pull_request", set())
        self.assertEqual(len(result["core"]["include"]), 2)
        self.assertEqual(result["adapters"]["include"], [])
        self.assertFalse(result["has_adapters"])
        self.assertFalse(result["has_services"])
        self.assertFalse(result["has_examples"])
        self.assertEqual(
            {row["os"] for row in result["core"]["include"]},
            {"ubuntu-24.04"},
        )

    def test_core_changes_keep_macos_architectures_and_windows_endpoints(self):
        result = plan("pull_request", {"core"})
        self.assertTrue(result["has_examples"])
        self.assertEqual(
            result["core"]["include"],
            [
                {"os": "ubuntu-24.04", "python": "3.11"},
                {"os": "ubuntu-24.04", "python": "3.15"},
                {"os": "macos-15", "python": "3.15"},
                {"os": "macos-15-intel", "python": "3.15"},
                {"os": "windows-2025", "python": "3.11"},
                {"os": "windows-2025", "python": "3.15"},
            ],
        )

    def test_adapter_changes_keep_linux_core_gate_without_macos_queue(self):
        for suite in MODULES:
            with self.subTest(suite=suite):
                result = plan("pull_request", {suite})
                self.assertEqual(
                    result["core"]["include"],
                    [
                        {"os": "ubuntu-24.04", "python": "3.11"},
                        {"os": "ubuntu-24.04", "python": "3.15"},
                    ],
                )
                self.assertEqual([row["suite"] for row in result["adapters"]["include"]], [suite])
                self.assertFalse(result["has_examples"])

    def test_examples_and_shared_changes_select_http_tests_without_adapter_extras(self):
        for changed in ({"examples"}, {"shared"}):
            with self.subTest(changed=changed):
                result = plan("pull_request", changed)
                self.assertTrue(result["has_examples"])
                self.assertFalse(result["has_adapters"])
                self.assertFalse(result["has_services"])

    def test_scoped_change_only_selects_related_adapter_and_service(self):
        result = plan("pull_request", {"aws"})
        self.assertEqual([row["suite"] for row in result["adapters"]["include"]], ["aws"])
        self.assertEqual(result["adapters"]["include"][0]["python"], "3.12")
        self.assertEqual([row["suite"] for row in result["services"]["include"]], ["aws"])

    def test_infrastructure_schedule_and_manual_runs_cover_complete_matrix(self):
        for event, changes in (
            ("pull_request", {"infrastructure"}),
            ("schedule", set()),
            ("workflow_dispatch", set()),
        ):
            with self.subTest(event=event):
                result = plan(event, changes)
                self.assertTrue(result["has_examples"])
                self.assertEqual(
                    {row["python"] for row in result["core"]["include"]},
                    {"3.11", "3.12", "3.13", "3.14", "3.15"},
                )
                self.assertEqual(
                    {row["os"] for row in result["core"]["include"]},
                    {"ubuntu-24.04", "macos-15", "macos-15-intel", "windows-2025"},
                )
                self.assertEqual(
                    {(row["suite"], row["python"]) for row in result["adapters"]["include"]},
                    {(suite, version) for suite in MODULES for version in ("3.11", "3.15")},
                )
                self.assertEqual({row["python"] for row in result["adapters"]["include"]}, {"3.11", "3.15"})
                self.assertEqual(
                    {row["suite"] for row in result["services"]["include"]},
                    {"aws", "sqlalchemy", "scylladb", "mongodb", "kafka"},
                )

    def test_kafka_change_selects_native_tests_and_live_broker(self):
        result = plan("pull_request", {"kafka"})
        self.assertEqual([row["suite"] for row in result["adapters"]["include"]], ["kafka"])
        self.assertEqual(
            result["services"]["include"],
            [{"suite": "kafka", "extra": "kafka", "groups": "test", "compose": "kafka"}],
        )

    def test_adapter_groups_only_include_their_test_requirements(self):
        result = plan("schedule", set())
        for row in [*result["adapters"]["include"], *result["services"]["include"]]:
            suite = row.get("extra", row["suite"])
            expected = {"test"}
            if suite == "sqlalchemy":
                expected.add("adapter-test")
            elif suite == "keras":
                expected.add("ml-test")
            if "extra" not in row and suite in {"aws", "kafka"}:
                expected.add("typecheck")
            with self.subTest(suite=suite):
                self.assertEqual(set(row["groups"].split()), expected)

    @unittest.skipUnless(importlib.util.find_spec("yaml"), "Filter parsing uses lint's pre-commit dependency")
    def test_adapter_typing_fixtures_select_their_installed_sdk_gate(self):
        import yaml

        root = Path(__file__).resolve().parents[2]
        filters = yaml.safe_load((root / ".github/filters.yml").read_text())

        def paths(entries):
            for entry in entries:
                if isinstance(entry, list):  # YAML aliases expand to nested lists.
                    yield from paths(entry)
                else:
                    self.assertIsInstance(entry, str)
                    yield entry

        for suite in ("aws", "kafka"):
            fixtures = sorted((root / "tests/typing").glob(f"{suite}_*.py"))
            self.assertTrue(fixtures)
            for fixture in fixtures:
                with self.subTest(path=fixture.name):
                    relative = fixture.relative_to(root).as_posix()
                    changed = {
                        name
                        for name, entries in filters.items()
                        if any(fnmatchcase(relative, pattern) for pattern in paths(entries))
                    }
                    self.assertEqual(changed, {suite})
                    result = plan("pull_request", changed)
                    self.assertEqual([row["suite"] for row in result["adapters"]["include"]], [suite])
                    self.assertIn("typecheck", result["adapters"]["include"][0]["groups"].split())

    def test_main_expands_core_without_unrelated_service_jobs(self):
        result = plan("push", {"keras"})
        self.assertTrue(result["has_examples"])
        self.assertEqual(len(result["core"]["include"]), 9)
        self.assertEqual([row["suite"] for row in result["adapters"]["include"]], ["keras"])
        self.assertFalse(result["has_services"])


if __name__ == "__main__":
    unittest.main()
