from __future__ import annotations

import importlib.util
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

import markdown

SPEC = importlib.util.spec_from_file_location("docs_hooks", Path(__file__).parents[1] / "hooks.py")
assert SPEC is not None and SPEC.loader is not None
HOOKS = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(HOOKS)


class RepositoryLinkTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.docs = self.root / "docs"
        self.docs.mkdir()
        (self.root / "examples").mkdir()
        (self.root / "examples" / "a file.py").write_text("print(1)")
        (self.root / "README.md").write_text("# Readme")
        (self.root / "image.png").write_bytes(b"image")
        self.options = {
            "page": self.docs / "guide.md",
            "docs_dir": self.docs,
            "root": self.root,
            "repo_url": "https://github.com/hussein-awala/async-batcher",
            "ref": "a" * 40,
        }

    def test_repository_files_keep_query_anchor_and_quoted_path(self):
        self.assertEqual(
            HOOKS.repository_url("../examples/a%20file.py?plain=1#L1", **self.options),
            "https://github.com/hussein-awala/async-batcher/blob/"
            + "a" * 40
            + "/examples/a%20file.py?plain=1#L1",
        )

    def test_local_and_external_links_are_left_for_mkdocs(self):
        for url in (
            "other.md#heading",
            "missing.md",
            "#heading",
            "?query=1",
            "/absolute/path",
            "https://example.com/../README.md",
            "//example.com/page",
            "mailto:maintainer@example.com",
        ):
            with self.subTest(url=url):
                self.assertEqual(HOOKS.repository_url(url, **self.options), url)

    def test_nested_page_to_repository_directory(self):
        self.options["page"] = self.docs / "nested" / "page.md"
        self.assertEqual(
            HOOKS.repository_url("../../examples/", **self.options),
            "https://github.com/hussein-awala/async-batcher/tree/" + "a" * 40 + "/examples",
        )
        self.assertEqual(HOOKS.repository_url("../guide.md", **self.options), "../guide.md")

    def test_images_use_raw_files(self):
        self.assertEqual(
            HOOKS.repository_url("../image.png", image=True, **self.options),
            "https://raw.githubusercontent.com/hussein-awala/async-batcher/" + "a" * 40 + "/image.png",
        )

    def test_invalid_repository_targets_fail(self):
        for url in ("../missing.py", "../../outside.md"):
            with self.subTest(url=url), self.assertRaisesRegex(ValueError, "missing repository target"):
                HOOKS.repository_url(url, **self.options)

    def test_markdown_links_and_references_change_but_code_does_not(self):
        source = """
[readme](../README.md#readme) and [reference][ref]

[ref]: ../README.md

`[code](../README.md)`

```text
[example](../README.md)
```
"""
        rendered = markdown.markdown(
            source, extensions=["fenced_code", HOOKS.RepositoryLinksExtension(**self.options)]
        )
        expected = "https://github.com/hussein-awala/async-batcher/blob/" + "a" * 40 + "/README.md"
        self.assertIn(f'href="{expected}#readme"', rendered)
        self.assertIn(f'href="{expected}"', rendered)
        self.assertIn("<code>[code](../README.md)</code>", rendered)
        self.assertIn("[example](../README.md)", rendered)

    def test_page_hook_pins_home_install_and_replaces_per_page_context(self):
        config = SimpleNamespace(
            config_file_path=str(self.root / "mkdocs.yml"),
            docs_dir=str(self.docs),
            extra={"documentation": {"source_sha": "a" * 40}},
            repo_url=self.options["repo_url"],
            markdown_extensions=["fenced_code"],
        )
        page = SimpleNamespace(file=SimpleNamespace(src_uri="index.md"))
        source = "git checkout --detach main\n"
        self.assertEqual(
            HOOKS.on_page_markdown(source, page=page, config=config),
            "git checkout --detach " + "a" * 40 + "\n",
        )
        page.file.src_uri = "nested/guide.md"
        self.assertEqual(HOOKS.on_page_markdown(source, page=page, config=config), source)
        extensions = [x for x in config.markdown_extensions if isinstance(x, HOOKS.RepositoryLinksExtension)]
        self.assertEqual(len(extensions), 1)
        rendered = markdown.markdown("[readme](../../README.md)", extensions=config.markdown_extensions)
        self.assertIn("/blob/" + "a" * 40 + "/README.md", rendered)
        config.extra["documentation"]["source_sha"] = "main; bad-command"
        with self.assertRaisesRegex(ValueError, "full lowercase commit SHA"):
            HOOKS.on_page_markdown(source, page=page, config=config)


if __name__ == "__main__":
    unittest.main()
