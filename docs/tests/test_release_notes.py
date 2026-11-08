"""Build the global release history with the real locked documentation toolchain."""

from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import markdown
from mkdocs.commands.build import build
from mkdocs.config import load_config

ROOT = Path(__file__).resolve().parents[2]


SPEC = importlib.util.spec_from_file_location(
    "release_notes_helper", ROOT / ".github/scripts/release_notes.py"
)
assert SPEC is not None and SPEC.loader is not None
RELEASE_NOTES = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = RELEASE_NOTES
SPEC.loader.exec_module(RELEASE_NOTES)


class ReleaseHistoryBuildTests(unittest.TestCase):
    def test_preview_links_rewrite_markdown_links_but_preserve_code_and_canonical_body(self):
        source = """
[Documentation](https://hussein.awala.fr/async-batcher/0.3.0/) and [migration][guide].

[guide]: https://hussein.awala.fr/async-batcher/0.3.0/migration/#changes

<https://hussein.awala.fr/async-batcher/0.3.0/api/>

[Published](https://hussein.awala.fr/async-batcher/0.2.2/)

[Full changelog](https://github.com/hussein-awala/async-batcher/compare/0.2.2...0.3.0)

`[code](https://hussein.awala.fr/async-batcher/0.3.0/)`

```markdown
[example](https://hussein.awala.fr/async-batcher/0.3.0/)
```
"""
        release = RELEASE_NOTES.Release("0.3.0", "Async Batcher 0.3.0", None, "planned", "unused", source)
        rendered = markdown.markdown(
            RELEASE_NOTES.render_index([release]),
            extensions=["fenced_code", RELEASE_NOTES.preview_links_extension({"0.3.0"})],
        )
        self.assertIn('href="https://hussein.awala.fr/async-batcher/develop/"', rendered)
        self.assertIn('href="https://hussein.awala.fr/async-batcher/develop/migration/#changes"', rendered)
        self.assertIn('href="https://hussein.awala.fr/async-batcher/develop/api/"', rendered)
        self.assertIn('href="https://hussein.awala.fr/async-batcher/0.2.2/"', rendered)
        self.assertIn('href="https://github.com/hussein-awala/async-batcher/compare/0.2.2...main"', rendered)
        self.assertEqual(rendered.count("(development preview)"), 4)
        self.assertIn("<code>[code](https://hussein.awala.fr/async-batcher/0.3.0/)</code>", rendered)
        self.assertIn("[example](https://hussein.awala.fr/async-batcher/0.3.0/)", rendered)
        self.assertEqual(release.body, source)
        published = markdown.markdown(
            source, extensions=["fenced_code", RELEASE_NOTES.preview_links_extension(set())]
        )
        self.assertIn('href="https://hussein.awala.fr/async-batcher/0.3.0/"', published)
        self.assertIn(
            'href="https://github.com/hussein-awala/async-batcher/compare/0.2.2...0.3.0"', published
        )
        self.assertNotIn("(development preview)", published)

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
            self.assertIn("Published 2025-02-17", html)
            self.assertIn('href="https://github.com/hussein-awala/async-batcher/pull/35"', html)
            self.assertIn('href="https://hussein.awala.fr/async-batcher/"', html)
            self.assertNotIn('data-md-component="version"', html)
            self.assertFalse((output / "versions.json").exists())
        self.assertEqual(sources, {path: path.read_bytes() for path in (ROOT / "release-notes").glob("*.md")})
        self.assertFalse((ROOT / "release-notes/index.md").exists())

    def test_planned_links_work_in_complete_develop_archive_before_release(self):
        # Keep this archive fixture independent of actual publication dates and
        # future history: only development docs and one planned release exist.
        planned = RELEASE_NOTES.Release(
            "0.3.0",
            "Async Batcher 0.3.0",
            None,
            "planned",
            "https://github.com/hussein-awala/async-batcher/releases/tag/0.3.0",
            "[Documentation](https://hussein.awala.fr/async-batcher/0.3.0/)\n\n"
            "[Migration](https://hussein.awala.fr/async-batcher/0.3.0/migration/)\n\n"
            "[Full changelog](https://github.com/hussein-awala/async-batcher/compare/0.2.2...0.3.0)\n",
        )
        with tempfile.TemporaryDirectory() as temporary:
            archive = Path(temporary) / "pages"
            environment = dict(
                os.environ,
                PYTHONDONTWRITEBYTECODE="1",
                DOCS_SOURCE_SHA="a" * 40,
                DOCS_CHANNEL="develop",
                DOCS_VERSION="develop",
            )
            with patch.dict(os.environ, environment):
                config = load_config(
                    config_file=str(ROOT / "mkdocs.yml"),
                    site_url="https://hussein.awala.fr/async-batcher/develop/",
                    site_dir=str(archive / "develop"),
                )
                build(config)
            with (
                patch.dict(os.environ, environment),
                patch.object(RELEASE_NOTES, "load_releases", return_value=[planned]),
            ):
                RELEASE_NOTES.build(archive / "release-notes")
            history = (archive / "release-notes/index.html").read_text(encoding="utf-8")
            self.assertIn("this release has not been published", history)
            self.assertEqual(history.count("(development preview)"), 3)
            (archive / "index.html").write_text('<a href="develop/">Documentation</a>', encoding="utf-8")
            (archive / "versions.json").write_text(
                json.dumps([{"version": "develop", "aliases": []}]), encoding="utf-8"
            )
            self.assertFalse((archive / "0.3.0").exists())
            checked = subprocess.run(
                [sys.executable, str(ROOT / ".github/scripts/check_docs.py"), str(archive)],
                env=environment,
                capture_output=True,
                text=True,
            )
            self.assertEqual(checked.returncode, 0, checked.stdout + checked.stderr)


if __name__ == "__main__":
    unittest.main()
