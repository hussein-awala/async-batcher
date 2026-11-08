"""Build the global release history with the real locked documentation toolchain."""

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


class ReleaseHistoryBuildTests(unittest.TestCase):
    def test_global_history_build_has_valid_links_and_leaves_sources_unchanged(self):
        sources = {path: path.read_bytes() for path in (ROOT / "release-notes").glob("*.md")}
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "site"
            environment = dict(os.environ, PYTHONDONTWRITEBYTECODE="1", DOCS_SOURCE_SHA="a" * 40)
            subprocess.run(
                [
                    sys.executable,
                    str(ROOT / ".github/scripts/release_notes.py"),
                    "build",
                    "--output-dir",
                    str(output),
                ],
                env=environment,
                check=True,
                capture_output=True,
                text=True,
            )
            subprocess.run(
                [
                    sys.executable,
                    str(ROOT / ".github/scripts/check_docs.py"),
                    str(output),
                    "--base-path",
                    "/async-batcher/release-notes/",
                ],
                env=environment,
                check=True,
                capture_output=True,
                text=True,
            )
            html = (output / "index.html").read_text(encoding="utf-8")
            self.assertIn('<meta name="documentation-channel" content="project">', html)
            self.assertIn('<meta name="documentation-version" content="">', html)
            self.assertIn("Release notes for all versions.", html)
            self.assertIn("<strong>In preparation</strong>", html)
            self.assertIn("Published 2025-02-17", html)
            self.assertIn('href="https://github.com/hussein-awala/async-batcher/pull/35"', html)
            self.assertIn('href="https://hussein.awala.fr/async-batcher/"', html)
            self.assertNotIn('data-md-component="version"', html)
            self.assertFalse((output / "versions.json").exists())
        self.assertEqual(sources, {path: path.read_bytes() for path in (ROOT / "release-notes").glob("*.md")})
        self.assertFalse((ROOT / "release-notes/index.md").exists())


if __name__ == "__main__":
    unittest.main()
