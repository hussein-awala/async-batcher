from __future__ import annotations

import json
import subprocess
import tempfile
import unittest
from collections import Counter
from pathlib import Path

from check_public_typing import diagnostics, expected_errors, verify_fixture


class PublicTypingValidationTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.fixture = Path(temporary.name) / "consumer.py"
        self.fixture.write_text("bad()  # error: arg-type\n")

    def result(self, entries=(), *, code=1, stderr=""):
        return subprocess.CompletedProcess(
            [], code, "\n".join(json.dumps(entry) for entry in entries), stderr
        )

    def error(self, **changes):
        return {"file": str(self.fixture), "line": 1, "severity": "error", "code": "arg-type"} | changes

    def test_positive_and_exact_negative_diagnostics(self):
        verify_fixture(self.result([self.error()]), self.fixture, negative=True)
        self.fixture.write_text("good()\n")
        verify_fixture(self.result(code=0), self.fixture, negative=False)

    def test_duplicate_error_codes_and_relative_filenames_are_retained(self):
        self.fixture.write_text("bad()  # error: arg-type, arg-type\n")
        result = self.result([self.error(file="consumer.py"), self.error()])
        verify_fixture(result, self.fixture, negative=True)
        self.assertEqual(expected_errors(self.fixture.read_text()), Counter({(1, "arg-type"): 2}))

    def test_missing_extra_and_wrong_diagnostics_fail(self):
        for errors in ([], [self.error(line=2)], [self.error(code="misc")], [self.error(), self.error()]):
            with self.subTest(errors=errors), self.assertRaises(ValueError):
                verify_fixture(self.result(errors, code=1 if errors else 0), self.fixture, negative=True)

    def test_expected_error_markers_cannot_make_positive_or_empty_negative_pass(self):
        with self.assertRaises(ValueError):
            verify_fixture(self.result([self.error()]), self.fixture, negative=False)
        self.fixture.write_text("good()\n")
        with self.assertRaises(ValueError):
            verify_fixture(self.result(code=0), self.fixture, negative=True)

    def test_malformed_marker_is_rejected(self):
        for source in ("bad() # error:", "bad() # error: arg-type,", "bad() # error: nonsense!"):
            with self.subTest(source=source), self.assertRaises(ValueError):
                expected_errors(source)

    def test_checker_crash_stderr_or_inconsistent_exit_cannot_pass(self):
        for result in (
            self.result(code=2),
            self.result(code=0, stderr="checker warning"),
            self.result(code=1),
            self.result([self.error()], code=0),
            subprocess.CompletedProcess([], 0, "not JSON", ""),
        ):
            with self.subTest(result=result), self.assertRaises(ValueError):
                diagnostics(result, self.fixture)

    def test_malformed_or_foreign_diagnostics_are_not_counted(self):
        for entry in (
            [],
            self.error(file="other.py"),
            self.error(file=None),
            self.error(line=True),
            self.error(line=0),
            self.error(code=None),
            self.error(severity="warning"),
        ):
            with self.subTest(entry=entry), self.assertRaises(ValueError):
                diagnostics(self.result([entry]), self.fixture)

    def test_blank_lines_and_notes_do_not_invent_errors(self):
        result = self.result([self.error(severity="note")], code=0)
        result.stdout += "\n\n"
        self.assertEqual(diagnostics(result, self.fixture), Counter())


if __name__ == "__main__":
    unittest.main()
