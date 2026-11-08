"""Exercise the real locked mike CLI without contacting or publishing to GitHub."""

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

SPEC = importlib.util.spec_from_file_location(
    "publication_helper", Path(__file__).parents[2] / ".github" / "scripts" / "publish_docs.py"
)
assert SPEC is not None and SPEC.loader is not None
PUBLICATION = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(PUBLICATION)


class RealPublicationTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.repo = self.root / "repo"
        self.repo.mkdir()
        previous = Path.cwd()
        os.chdir(self.repo)
        self.addCleanup(os.chdir, previous)
        environment = patch.dict(
            os.environ,
            {
                "PATH": str(Path(sys.executable).parent) + os.pathsep + os.environ.get("PATH", ""),
                "GIT_CONFIG_NOSYSTEM": "1",
                "GIT_CONFIG_GLOBAL": os.devnull,
            },
        )
        environment.start()
        self.addCleanup(environment.stop)
        self.git("init", "--quiet", "--initial-branch=main")
        self.git("config", "user.name", "Documentation fixture")
        self.git("config", "user.email", "docs-fixture@example.invalid")
        # These disposable local objects are test data and are never published.
        self.git("config", "commit.gpgsign", "false")
        self.git("config", "tag.gpgsign", "false")
        (self.repo / "docs").mkdir()
        (self.repo / "mkdocs.yml").write_text(
            """site_name: Publication fixture
site_url: https://example.invalid/async-batcher/
strict: true
theme:
  name: mkdocs
plugins:
  - search
  - mike:
      alias_type: redirect
nav:
  - Home: index.md
""",
            encoding="utf-8",
        )
        self.commit_page("Development fixture")
        empty_tree = self.git("hash-object", "-t", "tree", "--stdin", input=b"")
        bootstrap = self.git("commit-tree", empty_tree, "-m", "Empty local archive fixture")
        self.git("update-ref", "refs/heads/gh-pages", bootstrap)
        self.git("update-ref", PUBLICATION.REMOTE_REF, bootstrap)
        self.sequence = 0

    def git(self, *arguments, input=None):
        result = subprocess.run(["git", *arguments], input=input, capture_output=True, check=True)
        return result.stdout.decode().strip()

    def commit_page(self, text):
        (self.repo / "docs" / "index.md").write_text(
            f"# {text}\n\nA real generated page.\n", encoding="utf-8"
        )
        self.git("add", "docs/index.md", "mkdocs.yml")
        self.git("commit", "--quiet", "-m", text)
        sha = self.git("rev-parse", "HEAD")
        self.git("update-ref", "refs/remotes/origin/main", sha)
        return sha

    def prepare(self, ref, *, release_notes=False):
        self.sequence += 1
        output = self.root / f"pages-{self.sequence}"
        state = self.root / f"state-{self.sequence}.json"
        with patch.object(PUBLICATION, "graphql", side_effect=AssertionError("Preparation must be offline")):
            result = PUBLICATION.prepare(
                ref, self.git("rev-parse", "HEAD"), output, state, release_notes=release_notes
            )
        PUBLICATION.verify_output(result["tree"], output)
        self.assertEqual(json.loads(state.read_text()), result)
        # Simulate a subsequent fresh fetch using only local staging objects.
        # No test calls publish(), pushes a branch, or uses an external remote.
        # The remote commit must carry the complete prepared tree, including
        # the global subtree overlaid after mike created its staging commits.
        archived = self.git(
            "commit-tree", result["tree"], "-p", result["expected_head"], "-m", "Local archive fixture"
        )
        self.git("update-ref", "refs/heads/gh-pages", archived)
        self.git("update-ref", PUBLICATION.REMOTE_REF, archived)
        return result, output

    @staticmethod
    def snapshot(directory):
        return {
            path.relative_to(directory).as_posix(): path.read_bytes()
            for path in directory.rglob("*")
            if path.is_file()
        }

    def test_real_build_preserves_versions_and_moves_latest_only_for_highest_stable(self):
        develop, first = self.prepare("refs/heads/main")
        self.assertIn("Development fixture", (first / "develop" / "index.html").read_text())
        self.assertIn('href="develop/"', (first / "index.html").read_text())
        development_archive = self.snapshot(first / "develop")
        versions = json.loads((first / "versions.json").read_text())
        self.assertEqual(
            versions[0]["properties"], {"source_sha": develop["source_sha"], "channel": "develop"}
        )

        release_sha = self.commit_page("Stable fixture")
        self.git("tag", "0.3.0")
        release, stable = self.prepare("refs/tags/0.3.0")
        self.assertTrue(release["latest"])
        self.assertIn("Stable fixture", (stable / "0.3.0" / "index.html").read_text())
        self.assertIn('href="latest/"', (stable / "index.html").read_text())
        self.assertIn('href="../0.3.0/"', (stable / "latest" / "index.html").read_text())
        stable_archive = self.snapshot(stable / "0.3.0")

        self.commit_page("Prerelease fixture")
        self.git("tag", "0.4.0rc1")
        prerelease, candidate = self.prepare("refs/tags/0.4.0rc1")
        self.assertFalse(prerelease["latest"])
        self.assertIn("Prerelease fixture", (candidate / "0.4.0rc1" / "index.html").read_text())
        self.assertEqual(self.snapshot(candidate / "0.3.0"), stable_archive)

        self.commit_page("Maintenance fixture")
        self.git("tag", "0.2.3")
        backport, final = self.prepare("refs/tags/0.2.3")
        self.assertFalse(backport["latest"])
        self.assertIn("Maintenance fixture", (final / "0.2.3" / "index.html").read_text())
        self.assertEqual(self.snapshot(final / "develop"), development_archive)
        self.assertEqual(self.snapshot(final / "0.3.0"), stable_archive)
        self.assertIn('href="latest/"', (final / "index.html").read_text())
        self.assertIn('href="../0.3.0/"', (final / "latest" / "index.html").read_text())
        versions = {entry["version"]: entry for entry in json.loads((final / "versions.json").read_text())}
        self.assertEqual(set(versions), {"develop", "0.3.0", "0.4.0rc1", "0.2.3"})
        self.assertEqual(versions["0.3.0"]["aliases"], ["latest"])
        self.assertEqual(versions["0.3.0"]["properties"]["source_sha"], release_sha)
        self.assertEqual(versions["0.4.0rc1"]["properties"]["channel"], "prerelease")
        self.assertEqual(versions["0.2.3"]["aliases"], [])

    def test_main_updates_global_notes_but_old_tag_cannot_restore_older_notes(self):
        scripts = self.repo / ".github" / "scripts"
        scripts.mkdir(parents=True)
        # Stand-in for the independent Material builder: the actual subprocess
        # interface, source metadata, Git overlay and mike builds remain real.
        (scripts / "release_notes.py").write_text(
            "import argparse, os\n"
            "from pathlib import Path\n"
            "p = argparse.ArgumentParser()\n"
            "p.add_argument('command', choices=['build'])\n"
            "p.add_argument('--output-dir', type=Path, required=True)\n"
            "a = p.parse_args()\n"
            "a.output_dir.mkdir(parents=True)\n"
            "content = Path('notes.txt').read_text() + os.environ['DOCS_SOURCE_SHA']\n"
            "(a.output_dir / 'index.html').write_text(content)\n",
            encoding="utf-8",
        )
        notes = self.repo / "notes.txt"
        notes.write_text("Original notes: ", encoding="utf-8")
        self.git("add", ".github/scripts/release_notes.py", "notes.txt")
        original_sha = self.commit_page("First notes source")
        self.git("tag", "0.3.0")
        _, initial = self.prepare("refs/heads/main", release_notes=True)
        self.assertEqual(
            (initial / "release-notes" / "index.html").read_text(), "Original notes: " + original_sha
        )
        _, stable = self.prepare("refs/tags/0.3.0")
        stable_archive = self.snapshot(stable / "0.3.0")
        notes.write_text("Updated global notes: ", encoding="utf-8")
        self.git("add", "notes.txt")
        latest_sha = self.commit_page("New main notes")
        _, updated = self.prepare("refs/heads/main", release_notes=True)
        self.assertEqual(self.snapshot(updated / "0.3.0"), stable_archive)
        expected_notes = self.snapshot(updated / "release-notes")
        self.assertEqual(expected_notes["index.html"].decode(), "Updated global notes: " + latest_sha)
        self.git("checkout", "--quiet", "--detach", original_sha)
        _, old_release = self.prepare("refs/tags/0.3.0")
        self.assertEqual(self.snapshot(old_release / "release-notes"), expected_notes)
        self.assertEqual(self.snapshot(old_release / "develop"), self.snapshot(updated / "develop"))
        self.assertNotIn(
            "release-notes",
            {entry["version"] for entry in json.loads((old_release / "versions.json").read_text())},
        )


if __name__ == "__main__":
    unittest.main()
