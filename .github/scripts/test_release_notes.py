from __future__ import annotations

import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from release_notes import (
    ROOT,
    Release,
    demote_headings,
    load_release,
    load_releases,
    release_preview_url,
    render_index,
    version_key,
)


class ReleaseNotesTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)

    def fixture(self, version="0.3.0", *, body="## Features\n\nA change.\n", **metadata):
        fields = {
            "version": f'"{version}"',
            "title": f'"Async Batcher {version}"',
            "date": "null",
            "status": '"planned"',
            "github_url": f'"https://github.com/hussein-awala/async-batcher/releases/tag/{version}"',
        }
        fields.update(metadata)
        path = self.root / f"{version}.md"
        path.write_text(
            "---\n" + "\n".join(f"{key}: {value}" for key, value in fields.items()) + "\n---\n\n" + body,
            encoding="utf-8",
        )
        return path

    def test_metadata_and_body_are_loaded_without_modification(self):
        body = "\n## Features\n\n```python\n# Code heading\n```\n\nTrailing spaces  "
        release = load_release(self.fixture(body=body))
        self.assertEqual(release.body, body)
        self.assertEqual(release.version, "0.3.0")
        self.assertIsNone(release.date)

    def test_invalid_metadata_is_rejected(self):
        cases = [
            {"status": '"draft"'},
            {"status": '"published"'},
            {"date": '"2026-02-30"'},
            {"date": '"2026-1-01"'},
            {"date": "2026-11-08"},
            {"github_url": '"https://github.com/other/repo/releases/tag/0.3.0"'},
            {"title": '""'},
            {"unknown": "true"},
        ]
        for metadata in cases:
            with self.subTest(metadata=metadata), self.assertRaises(ValueError):
                load_release(self.fixture(**metadata))
        with self.assertRaisesRegex(ValueError, "empty"):
            load_release(self.fixture(body="\n"))
        path = self.fixture()
        renamed = path.with_name("0.3.1.md")
        path.rename(renamed)
        with self.assertRaisesRegex(ValueError, "filename"):
            load_release(renamed)
        for invalid in ("../0.3.0", "v0.3.0", "00.3.0", "0.3", "0.3.0+local"):
            with self.subTest(version=invalid), self.assertRaises(ValueError):
                version_key(invalid)

    def test_numeric_version_order_including_prereleases(self):
        for version in ("0.3.0rc1", "0.9.0", "0.3.0", "0.10.0", "0.3.0b2", "0.3.0a1", "0.3.0rc10"):
            self.fixture(version)
        self.assertEqual(
            [release.version for release in load_releases(self.root)],
            ["0.10.0", "0.9.0", "0.3.0", "0.3.0rc10", "0.3.0rc1", "0.3.0b2", "0.3.0a1"],
        )

    def test_demotes_headings_but_preserves_both_kinds_of_fences(self):
        body = (
            "# One\n## Two\n````python\n# Inside\n```\n## Still inside\n````\n~~~\n# Inside\n~~~\n### Three\n"
        )
        self.assertEqual(
            demote_headings(body),
            "## One\n### Two\n````python\n# Inside\n```\n## Still inside\n"
            "````\n~~~\n# Inside\n~~~\n#### Three\n",
        )

    def test_render_distinguishes_planned_and_published_releases(self):
        planned = load_release(self.fixture())
        published = Release(
            "0.2.2", "Async Batcher 0.2.2", "2025-02-17", "published", "https://example.invalid", "## Fixes\n"
        )
        rendered = render_index([planned, published])
        self.assertIn("**In preparation**", rendered)
        self.assertIn("Published 2025-02-17", rendered)
        self.assertIn("[GitHub release](https://example.invalid)", rendered)
        self.assertIn("### Fixes", rendered)
        self.assertEqual(planned.body, "## Features\n\nA change.\n")

    def test_planned_links_keep_paths_queries_and_fragments(self):
        cases = {
            "https://hussein.awala.fr/async-batcher/0.3.0/": "https://hussein.awala.fr/async-batcher/develop/",
            "https://hussein.awala.fr/async-batcher/0.3.0/migration/?view=full#shutdown": "https://hussein.awala.fr/async-batcher/develop/migration/?view=full#shutdown",
            "/async-batcher/0.3.0": "/async-batcher/develop",
            "//hussein.awala.fr/async-batcher/0.3.0/": "//hussein.awala.fr/async-batcher/develop/",
        }
        for source, expected in cases.items():
            with self.subTest(url=source):
                self.assertEqual(release_preview_url(source, {"0.3.0"}), expected)
                self.assertEqual(release_preview_url(source, set()), source)

    def test_preview_links_leave_published_external_and_other_targets_unchanged(self):
        for url in (
            "https://hussein.awala.fr/async-batcher/0.2.2/migration/",
            "https://hussein.awala.fr/async-batcher/0.3.01/",
            "https://hussein.awala.fr/another-project/0.3.0/",
            "https://other.example/async-batcher/0.3.0/",
            "https://github.com/hussein-awala/async-batcher/releases/tag/0.3.0",
            "https://github.com/hussein-awala/async-batcher/compare/0.2.1...0.2.2",
            "https://github.com/hussein-awala/async-batcher/compare/0.2.2...0.3.01",
            "https://github.com/another/repository/compare/0.2.2...0.3.0",
            "#async-batcher-030",
            "mailto:maintainer@example.invalid",
        ):
            with self.subTest(url=url):
                self.assertEqual(release_preview_url(url, {"0.3.0"}), url)

    def test_planned_comparison_targets_use_main_only_for_this_repository(self):
        url = "https://github.com/hussein-awala/async-batcher/compare/0.2.2...0.3.0?expand=1#files_bucket"
        expected = "https://github.com/hussein-awala/async-batcher/compare/0.2.2...main?expand=1#files_bucket"
        self.assertEqual(release_preview_url(url, {"0.3.0"}), expected)
        self.assertEqual(release_preview_url(url, set()), url)

    def test_cli_extract_preserves_each_real_body_exactly(self):
        for source in (ROOT / "release-notes").glob("*.md"):
            with self.subTest(version=source.stem):
                output = self.root / "body.md"
                subprocess.run(
                    [
                        sys.executable,
                        str(ROOT / ".github/scripts/release_notes.py"),
                        "extract",
                        source.stem,
                        "--output",
                        str(output),
                    ],
                    check=True,
                )
                expected = source.read_bytes().split(b"\n---\n\n", 1)[1]
                self.assertEqual(output.read_bytes(), expected)

    def test_missing_front_matter_and_empty_directory_are_rejected(self):
        with self.assertRaisesRegex(ValueError, "no release notes"):
            load_releases(self.root)
        path = self.root / "0.3.0.md"
        path.write_text("# Missing metadata\n", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "front matter"):
            load_release(path)


if __name__ == "__main__":
    unittest.main()
