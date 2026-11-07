from __future__ import annotations

import unittest

from ci_plan import plan


class PlanTests(unittest.TestCase):
    def test_docs_pull_request_keeps_core_gate_and_skips_adapters(self):
        result = plan("pull_request", set())
        self.assertEqual(len(result["core"]["include"]), 2)
        self.assertEqual(result["adapters"]["include"], [])
        self.assertFalse(result["has_adapters"])
        self.assertFalse(result["has_services"])
        self.assertEqual(
            {row["os"] for row in result["core"]["include"]},
            {"ubuntu-24.04"},
        )

    def test_core_changes_keep_both_macos_architectures(self):
        result = plan("pull_request", {"core"})
        self.assertEqual(
            result["core"]["include"],
            [
                {"os": "ubuntu-24.04", "python": "3.11"},
                {"os": "ubuntu-24.04", "python": "3.15"},
                {"os": "macos-15", "python": "3.15"},
                {"os": "macos-15-intel", "python": "3.15"},
            ],
        )

    def test_adapter_changes_keep_linux_core_gate_without_macos_queue(self):
        for suite in ("aws", "sqlalchemy", "scylla", "keras", "sklearn", "pinecone"):
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
                self.assertEqual(
                    {row["python"] for row in result["core"]["include"]},
                    {"3.11", "3.12", "3.13", "3.14", "3.15"},
                )
                self.assertEqual(
                    {row["os"] for row in result["core"]["include"]},
                    {"ubuntu-24.04", "macos-15", "macos-15-intel"},
                )
                self.assertEqual(len(result["adapters"]["include"]), 12)
                self.assertEqual({row["python"] for row in result["adapters"]["include"]}, {"3.11", "3.15"})
                self.assertEqual(
                    {row["suite"] for row in result["services"]["include"]},
                    {"aws", "sqlalchemy", "scylladb"},
                )

    def test_main_expands_core_without_unrelated_service_jobs(self):
        result = plan("push", {"keras"})
        self.assertEqual(len(result["core"]["include"]), 7)
        self.assertEqual([row["suite"] for row in result["adapters"]["include"]], ["keras"])
        self.assertFalse(result["has_services"])


if __name__ == "__main__":
    unittest.main()
