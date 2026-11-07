from __future__ import annotations

import json
import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import patch
from urllib.error import HTTPError

from audit_lock import (
    CONTROL,
    AuditError,
    audit,
    load_exceptions,
    load_packages,
    main,
    query_packages,
    request_json,
)


class LockAuditTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.lock = self.root / "uv.lock"
        self.project = self.root / "pyproject.toml"
        self.project.write_text('[project]\nname="async-batcher"\nversion="0.0.1"\n')
        self.lock.write_text(
            "version=1\nrevision=5\n"
            '[[package]]\nname="async-batcher"\nversion="0.0.1"\nsource={editable="."}\n'
            '[package.optional-dependencies]\nml=[{name="example-package", version="2.0"}]\n'
            '[package.dev-dependencies]\ntest=[{name="example-package", version="1.0"}]\n'
            '[[package]]\nname="Example_Package"\nversion="1.0"\n'
            'source={registry="https://pypi.org/simple"}\n'
            "resolution-markers=[\"python_full_version < '3.12'\"]\n"
            '[[package]]\nname="example-package"\nversion="2.0"\n'
            'source={registry="https://pypi.org/simple"}\n'
            "resolution-markers=[\"python_full_version >= '3.12'\"]\n"
        )
        self.exceptions = self.root / "exceptions.json"
        self.exceptions.write_text('{"exceptions": []}')

    def fake_osv(self, path, payload=None):
        if path == "/querybatch":
            return {
                "results": [
                    {"vulns": [{"id": "CONTROL"}]}
                    if (query["package"]["name"], query["version"]) == CONTROL
                    else {"vulns": [{"id": "GHSA-example"}]}
                    for query in payload["queries"]
                ]
            }
        self.assertEqual(path, "/vulns/GHSA-example")
        return {"id": "GHSA-example", "affected": [{"package": {"name": "example-package"}}]}

    def write_exception(self, **overrides):
        entry = {
            "name": "EXAMPLE_package",
            "version": "1.0",
            "id": "GHSA-example",
            "reason": "Temporary compatibility investigation; tracked in issue 123",
            "expires": "2026-12-01",
            **overrides,
        }
        self.exceptions.write_text(json.dumps({"exceptions": [entry]}))

    def test_all_conditional_versions_in_extras_and_dev_groups_are_audited(self):
        packages = load_packages(self.lock)
        self.assertEqual(set(packages), {("example-package", "1.0"), ("example-package", "2.0")})
        self.assertEqual(
            packages[("example-package", "1.0")]["lock_entries"][0]["resolution_markers"],
            ["python_full_version < '3.12'"],
        )
        report = audit(self.lock, self.exceptions, self.fake_osv)
        self.assertEqual(report["packages_audited"], 2)
        self.assertEqual(report["blocking_findings"], 2)
        self.assertEqual(report["status"], "fail")

    def test_same_version_in_multiple_forks_is_queried_once_with_all_lock_entries(self):
        self.lock.write_text(
            self.lock.read_text() + '[[package]]\nname="example.package"\nversion="1.0"\n'
            'source={registry="https://pypi.org/simple/"}\n'
            "resolution-markers=[\"sys_platform == 'win32'\"]\n"
        )
        packages = load_packages(self.lock)
        self.assertEqual(len(packages), 2)
        entries = packages[("example-package", "1.0")]["lock_entries"]
        self.assertEqual(len(entries), 2)
        self.assertEqual(entries[1]["resolution_markers"], ["sys_platform == 'win32'"])
        queries = []

        def fetch(path, payload=None):
            if path == "/querybatch":
                queries.extend((q["package"]["name"], q["version"]) for q in payload["queries"])
            return self.fake_osv(path, payload)

        audit(self.lock, self.exceptions, fetch)
        self.assertEqual(queries, [CONTROL, ("example-package", "1.0"), ("example-package", "2.0")])

    def test_verified_editable_and_virtual_root_are_not_queried_as_public_packages(self):
        contents = self.lock.read_text()
        for source in ('editable="."', 'virtual="."'):
            with self.subTest(source=source):
                self.lock.write_text(contents.replace('editable="."', source))
                self.assertNotIn(("async-batcher", "0.0.1"), load_packages(self.lock))

    def test_virtual_root_without_a_version_is_allowed(self):
        self.lock.write_text(
            self.lock.read_text().replace('version="0.0.1"\nsource={editable="."}', 'source={virtual="."}')
        )
        self.assertEqual(len(load_packages(self.lock)), 2)

    def test_only_the_verified_root_can_be_excluded(self):
        for contents in ('[project]\nname="different-project"', '[project]\nversion="0.0.1"'):
            with self.subTest(contents=contents):
                self.project.write_text(contents)
                with self.assertRaisesRegex(AuditError, "does not match"):
                    load_packages(self.lock)
        self.project.unlink()
        with self.assertRaisesRegex(AuditError, "Cannot verify"):
            load_packages(self.lock)

    def test_duplicate_root_entries_fail_closed(self):
        self.lock.write_text(
            self.lock.read_text() + '[[package]]\nname="async-batcher"\nsource={virtual="."}\n'
        )
        with self.assertRaisesRegex(AuditError, "duplicate local project"):
            load_packages(self.lock)

    def test_non_pypi_sources_fail_instead_of_querying_same_named_public_packages(self):
        contents = self.lock.read_text()
        for source in (
            '{git="https://example.com/repository?rev=main#abcdef"}',
            '{path="../example.whl"}',
            '{directory="../example"}',
            '{editable="../example"}',
            '{virtual="../example"}',
            '{url="https://example.com/example.whl"}',
            '{registry="https://private.example/simple"}',
            '{registry="http://pypi.org/simple"}',
            '{registry="https://pypi.org/simple", git="https://example.com"}',
            '{editable=".", registry="https://pypi.org/simple"}',
            "{registry=true}",
            "{}",
            '"https://pypi.org/simple"',
        ):
            with self.subTest(source=source):
                self.lock.write_text(
                    contents + f'[[package]]\nname="unsupported"\nversion="1.0"\nsource={source}\n'
                )
                with self.assertRaisesRegex(AuditError, "non-PyPI"):
                    load_packages(self.lock)

    def test_registry_packages_need_an_exact_version(self):
        contents = self.lock.read_text()
        for version in ('""', '"*"', '"1.*"', '">=1"', '"==1.0"', '"latest"', '"1.0 "', "1", "true"):
            with self.subTest(version=version):
                self.lock.write_text(
                    contents.replace('version="1.0"\nsource=', f"version={version}\nsource=")
                )
                with self.assertRaisesRegex(AuditError, "exact version"):
                    load_packages(self.lock)

    def test_normalized_pep440_version_forms_are_supported(self):
        contents = self.lock.read_text()
        for version in ("1", "1.0rc1", "1.0a2", "1.0b3", "1.0.post1", "1!2.0.post1.dev2+local.1"):
            with self.subTest(version=version):
                self.lock.write_text(
                    contents.replace('version="1.0"\nsource=', f'version="{version}"\nsource=')
                )
                self.assertIn(("example-package", version), load_packages(self.lock))

    def test_unsupported_lock_schema_fails_closed(self):
        contents = self.lock.read_text()
        for version in ('"1"', "2", "0", "true", "1.0"):
            with self.subTest(version=version):
                self.lock.write_text(contents.replace("version=1\n", f"version={version}\n", 1))
                with self.assertRaisesRegex(AuditError, "schema version"):
                    load_packages(self.lock)

    def test_empty_or_malformed_lockfile_is_not_a_clean_result(self):
        for contents in (
            '[metadata]\nlock-version="2.1"',
            "version=1\npackage=[]",
            'version=1\npackage=["example"]',
            'version=1\n[[package]]\nname="example"\nsource={registry="https://pypi.org/simple"}',
            'version=1\n[[package]]\nname="example"\nversion="1.0"',
            'version=1\n[[package]]\nname="invalid name"\nversion="1.0"',
            'version=1\n[[package]]\nname="async-batcher"\nsource={editable="."}',
        ):
            with self.subTest(contents=contents):
                self.lock.write_text(contents)
                with self.assertRaises(AuditError):
                    load_packages(self.lock)

    def test_pagination_tracks_only_packages_that_have_more_results(self):
        calls = []

        def fetch(path, payload):
            calls.append(payload)
            if len(calls) == 1:
                return {"results": [{"vulns": [{"id": "FIRST"}], "next_page_token": "next"}, {}]}
            return {"results": [{"vulns": [{"id": "SECOND"}]}]}

        result = query_packages([("one", "1"), ("two", "2")], fetch)
        self.assertEqual(result, {("one", "1"): {"FIRST", "SECOND"}, ("two", "2"): set()})
        self.assertEqual(len(calls[1]["queries"]), 1)
        self.assertEqual(calls[1]["queries"][0]["page_token"], "next")
        self.assertEqual(calls[1]["queries"][0]["package"]["name"], "one")

    def test_large_lock_is_batched_without_losing_packages(self):
        queries = []

        def fetch(path, payload):
            queries.extend(payload["queries"])
            self.assertLessEqual(len(payload["queries"]), 100)
            return {"results": [{} for _ in payload["queries"]]}

        keys = [(f"package-{i}", "1") for i in range(205)]
        self.assertEqual(set(query_packages(keys, fetch)), set(keys))
        self.assertEqual(len(queries), 205)

    def test_incomplete_and_invalid_responses_fail_closed(self):
        for response in (
            {},
            {"results": []},
            {"results": [None]},
            {"results": [{"error": "failed"}]},
            {"results": [{"vulns": None}]},
            {"results": [{"vulns": [{}]}]},
            {"results": [{"next_page_token": 42}]},
        ):
            with self.subTest(response=response), self.assertRaises(AuditError):
                query_packages([("example", "1")], lambda *args, response=response: response)

    def test_repeated_pagination_token_fails_closed(self):
        with self.assertRaisesRegex(AuditError, "pagination"):
            query_packages([("example", "1")], lambda *args: {"results": [{"next_page_token": "same"}]})

    def test_empty_positive_control_fails_even_if_lock_would_look_clean(self):
        with self.assertRaisesRegex(AuditError, "positive control"):
            audit(self.lock, self.exceptions, lambda *args: {"results": [{}]})

    def test_completed_clean_audit_passes(self):
        def fetch(path, payload):
            if payload["queries"][0]["package"]["name"] == CONTROL[0]:
                return {"results": [{"vulns": [{"id": "CONTROL"}]}]}
            return {"results": [{} for _ in payload["queries"]]}

        report = audit(self.lock, self.exceptions, fetch)
        self.assertEqual(report["status"], "pass")
        self.assertEqual(report["packages_audited"], 2)
        self.assertEqual(report["findings"], [])

    def test_exception_is_exact_version_specific_and_remains_visible(self):
        self.write_exception()
        report = audit(self.lock, self.exceptions, self.fake_osv, date(2026, 11, 7))
        self.assertEqual(report["accepted_findings"], 1)
        self.assertEqual(report["blocking_findings"], 1)
        self.assertTrue(next(f for f in report["findings"] if f["version"] == "1.0")["accepted"])
        self.assertEqual(report["status"], "fail")

    def test_expiring_exception_does_not_suppress_findings_on_expiry_date(self):
        self.write_exception(expires="2026-11-07")
        report = audit(self.lock, self.exceptions, self.fake_osv, date(2026, 11, 7))
        self.assertEqual(report["accepted_findings"], 0)
        self.assertEqual(report["blocking_findings"], 2)

    def test_missing_reason_and_invalid_expiry_are_rejected(self):
        for fields in ({"reason": ""}, {"expires": "never"}):
            with self.subTest(fields=fields):
                self.write_exception(**fields)
                with self.assertRaises(AuditError):
                    load_exceptions(self.exceptions)

    def test_duplicate_exceptions_are_rejected(self):
        self.write_exception()
        data = json.loads(self.exceptions.read_text())
        data["exceptions"] *= 2
        self.exceptions.write_text(json.dumps(data))
        with self.assertRaisesRegex(AuditError, "Duplicate"):
            load_exceptions(self.exceptions)

    def test_withdrawn_advisories_are_not_reported_as_current_findings(self):
        def fetch(path, payload=None):
            if path.startswith("/vulns/"):
                return {"id": "GHSA-example", "withdrawn": "2026-10-01T00:00:00Z"}
            return self.fake_osv(path, payload)

        self.assertEqual(audit(self.lock, self.exceptions, fetch)["findings"], [])

    def test_invalid_withdrawal_does_not_suppress_a_vulnerability(self):
        def fetch(path, payload=None):
            if path.startswith("/vulns/"):
                return {"id": "GHSA-example", "withdrawn": True}
            return self.fake_osv(path, payload)

        with self.assertRaisesRegex(AuditError, "invalid withdrawal"):
            audit(self.lock, self.exceptions, fetch)

    def test_mismatched_advisory_record_fails_closed(self):
        def fetch(path, payload=None):
            return {"id": "wrong"} if path.startswith("/vulns/") else self.fake_osv(path, payload)

        with self.assertRaisesRegex(AuditError, "wrong advisory"):
            audit(self.lock, self.exceptions, fetch)

    def test_http_retry_exhaustion_is_an_error(self):
        error = HTTPError("https://api.osv.dev", 503, "unavailable", {}, None)
        self.addCleanup(error.close)
        with patch("audit_lock.urlopen", side_effect=error) as request, patch("audit_lock.time.sleep"):
            with self.assertRaisesRegex(AuditError, "HTTP 503"):
                request_json("/querybatch", {"queries": []})
        self.assertEqual(request.call_count, 3)

    def test_cli_writes_error_report_and_returns_nonzero_for_incomplete_audit(self):
        output = self.root / "report.json"
        with (
            patch("audit_lock.audit", side_effect=AuditError("incomplete response")),
            patch("builtins.print"),
        ):
            self.assertEqual(main(["--output", str(output)]), 2)
        self.assertEqual(json.loads(output.read_text())["status"], "error")

    def test_cli_returns_failure_for_unaccepted_findings(self):
        output = self.root / "report.json"
        with (
            patch("audit_lock.audit", return_value={"status": "fail", "blocking_findings": 1}),
            patch("builtins.print"),
        ):
            self.assertEqual(main(["--output", str(output)]), 1)

    def test_cli_defaults_to_uv_lock(self):
        output = self.root / "report.json"
        with (
            patch("audit_lock.audit", return_value={"status": "pass", "blocking_findings": 0}) as run_audit,
            patch("builtins.print"),
        ):
            self.assertEqual(main(["--output", str(output)]), 0)
        run_audit.assert_called_once_with(Path("uv.lock"), Path(".github/lock-audit-exceptions.json"))


if __name__ == "__main__":
    unittest.main()
