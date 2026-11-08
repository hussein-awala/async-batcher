from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from check_docs import check_site


class DocumentationChecks(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.write("index.html", "<!doctype html><h1 id='home'>Home</h1>")

    def write(self, name, text):
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
        return path

    def test_shared_release_notes_are_external_only_for_version_previews(self):
        self.write(
            "index.html", '<a href="https://hussein.awala.fr/async-batcher/release-notes/">Release notes</a>'
        )
        self.assertEqual(check_site(self.root, version_preview=True), [])
        self.assertEqual(len(check_site(self.root)), 1)
        self.write("release-notes/index.html", "<h1>History</h1>")
        self.assertEqual(check_site(self.root), [])
        self.write("versions.json", "[]")
        with self.assertRaisesRegex(ValueError, "complete version archive"):
            check_site(self.root, version_preview=True)

    def test_preview_does_not_skip_broken_release_notes_subpages_or_relative_links(self):
        self.write(
            "index.html",
            '<a href="release-notes/">Relative</a>'
            '<a href="/async-batcher/release-notes/missing/">Missing</a>',
        )
        self.assertEqual(len(check_site(self.root, version_preview=True)), 2)

    def test_missing_homepage(self):
        (self.root / "index.html").unlink()
        self.assertEqual(check_site(self.root), ["index.html: missing documentation homepage"])

    def test_links_assets_queries_encoded_names_and_empty_fragments(self):
        self.write("guide/a b.html", '<a name="café"></a><a href="../#home">Home</a>')
        self.write("app.js", "console.log('ready')")
        self.write("style.css", "body {}")
        self.write("small.png", "image")
        self.write("large.png", "image")
        self.write(
            "index.html",
            """<!doctype html><h1 id="home">Home</h1>
            <a href="guide/a%20b.html?q=one&amp;r=two#caf%C3%A9">Guide</a><a href="#">Top</a>
            <script src="app.js"></script><link href="style.css" rel="stylesheet"/>
            <img srcset="small.png 1x, large.png 2x"/><source srcset="small.png,large.png">
            <img srcset="data:image/png;base64,aabb 1x, large.png 2x">
            """,
        )
        self.assertEqual(check_site(self.root), [])

    def test_missing_file_and_anchor_report_once_per_page(self):
        self.write("index.html", '<a href="missing/">One</a><img src="missing/"/><a href="#nope">Two</a>')
        errors = check_site(self.root)
        self.assertEqual(len(errors), 2)
        self.assertIn("'missing/': target does not exist", errors[0])
        self.assertIn("'#nope': target anchor does not exist", errors[1])

    def test_srcset_reports_missing_candidates(self):
        self.write("ok.png", "image")
        self.write("index.html", '<img srcset="ok.png 1x, missing.png 2x, other.png 3x">')
        errors = check_site(self.root)
        self.assertEqual(len(errors), 2)
        self.assertIn("missing.png", errors[0])
        self.assertIn("other.png", errors[1])

    def test_external_schemes_and_other_sites_are_ignored(self):
        self.write(
            "index.html",
            """<a href="https://github.com/missing">External</a><a href="mailto:test@example.com">Mail</a>
            <script src="//cdn.example.com/script.js"></script><img src="data:image/png;base64,aabb">
            <a href="https://hussein.awala.fr/spark-on-k8s/">Other project</a><a href="/elsewhere/">Other</a>
            """,
        )
        self.assertEqual(check_site(self.root), [])

    def test_absolute_own_site_and_root_base_paths_are_checked(self):
        self.write("develop/index.html", '<h1 id="start">Start</h1>')
        self.write(
            "index.html",
            """<a href="/async-batcher/develop/#start">Develop</a>
            <a href="https://hussein.awala.fr/async-batcher/develop/#start">Absolute</a>
            <a href="//hussein.awala.fr/async-batcher/develop/#start">Protocol-relative</a>
            <a href="/async-batcher">Root</a>
            <a href="https://hussein.awala.fr/async-batcher/missing/">Missing</a>""",
        )
        errors = check_site(self.root)
        self.assertEqual(len(errors), 1)
        self.assertIn("missing/", errors[0])

    def test_individual_version_mount_and_custom_origin(self):
        self.write(
            "index.html",
            '<link rel="canonical" href="https://docs.example.com/async-batcher/develop/">'
            '<a href="/async-batcher/latest/">Sibling channel</a>',
        )
        self.assertEqual(check_site(self.root, "/async-batcher/develop/", "https://docs.example.com"), [])
        with self.assertRaisesRegex(ValueError, "hostname"):
            check_site(self.root, site_url="no-hostname")

    def test_traversal_and_invalid_url_are_rejected(self):
        self.write(
            "index.html",
            '<a href="%2e%2e/escape.html">Escape</a><a href="/async-batcher/%2e%2e/escape.html">Escape</a>'
            '<a href="http://[invalid">Invalid</a>',
        )
        errors = check_site(self.root)
        self.assertEqual(len(errors), 3)
        self.assertIn("leaves the documentation build", errors[0])
        self.assertIn("leaves the documentation build", errors[1])
        self.assertIn("invalid URL", errors[2])

    def test_symlink_escape_is_rejected(self):
        with tempfile.TemporaryDirectory() as outside:
            external = Path(outside) / "external.html"
            external.write_text("<h1>Outside</h1>", encoding="utf-8")
            try:
                (self.root / "escape.html").symlink_to(external)
            except (OSError, NotImplementedError):
                self.skipTest("Symlinks are unavailable")
            self.write("index.html", '<a href="escape.html">Escape</a>')
            self.assertTrue(all("leaves the documentation build" in error for error in check_site(self.root)))

    def test_redirects_and_version_manifest_include_aliases(self):
        self.write("index.html", '<meta http-equiv="refresh" content="1; URL=latest/">')
        self.write("latest/index.html", '<meta http-equiv="refresh" content="0; url=\'../0.3.0/\'">')
        self.write("0.3.0/index.html", "<h1>Release</h1>")
        self.write("versions.json", json.dumps([{"version": "0.3.0", "aliases": ["latest"]}]))
        self.assertEqual(check_site(self.root), [])
        (self.root / "0.3.0/index.html").unlink()
        errors = check_site(self.root)
        self.assertEqual(len(errors), 2)
        self.assertIn("target does not exist", errors[0])
        self.assertIn("missing version or alias homepage", errors[1])

    def test_invalid_version_manifests_fail_closed(self):
        for manifest in (
            "not json",
            "{}",
            "[]",
            '[{"version": "../outside"}]',
            '[{"version": "develop", "aliases": "latest"}]',
            '[{"version": "develop", "aliases": ["develop"]}]',
            '[{"version": "versions.json"}]',
            '[{"version": "develop"}, {"version": "develop"}]',
        ):
            with self.subTest(manifest=manifest):
                self.write("versions.json", manifest)
                errors = check_site(self.root)
                self.assertEqual(len(errors), 1)
                self.assertTrue(errors[0].startswith("versions.json:"))

    def test_cli_reports_failure_and_success(self):
        script = Path(__file__).with_name("check_docs.py")
        self.write("index.html", '<a href="bad/">Broken</a>')
        result = subprocess.run([sys.executable, str(script), str(self.root)], capture_output=True, text=True)
        self.assertEqual(result.returncode, 1)
        self.assertIn("1 broken references", result.stdout)
        self.write("bad/index.html", "<h1>Fixed</h1>")
        result = subprocess.run([sys.executable, str(script), str(self.root)], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0)
        self.assertIn("passed", result.stdout)


if __name__ == "__main__":
    unittest.main()
