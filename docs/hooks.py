"""Keep repository-relative Markdown links usable on GitHub and the docs site."""

from __future__ import annotations

import re
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import quote, unquote, urlsplit, urlunsplit

from markdown.extensions import Extension
from markdown.treeprocessors import Treeprocessor

if TYPE_CHECKING:
    from xml.etree.ElementTree import Element

    from markdown import Markdown
    from mkdocs.config.defaults import MkDocsConfig
    from mkdocs.structure.pages import Page


def repository_url(
    url: str, *, page: Path, docs_dir: Path, root: Path, repo_url: str, ref: str, image: bool = False
) -> str:
    """Rewrite only existing repository targets outside the documentation tree."""
    parts = urlsplit(url)
    if parts.scheme or parts.netloc or not parts.path or parts.path.startswith(("/", "\\")):
        return url
    target = (page.parent / unquote(parts.path)).resolve()
    if target.is_relative_to(docs_dir):
        return url
    if not target.is_relative_to(root) or not target.exists():
        raise ValueError(f"{page.relative_to(root)} links to a missing repository target: {url}")
    path = quote(target.relative_to(root).as_posix(), safe="/")
    revision = quote(ref, safe="")
    if image:
        # GitHub's blob view is HTML; image sources must fetch the raw file.
        base = repo_url.replace("https://github.com/", "https://raw.githubusercontent.com/")
        destination = f"{base.rstrip('/')}/{revision}/{path}"
    else:
        kind = "tree" if target.is_dir() else "blob"
        destination = f"{repo_url.rstrip('/')}/{kind}/{revision}/{path}"
    parsed = urlsplit(destination)
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, parts.query, parts.fragment))


class RepositoryLinks(Treeprocessor):
    def __init__(self, md: Markdown, *, page: Path, docs_dir: Path, root: Path, repo_url: str, ref: str):
        super().__init__(md)
        self.options = {"page": page, "docs_dir": docs_dir, "root": root, "repo_url": repo_url, "ref": ref}

    def run(self, root: Element) -> None:
        for element in root.iter():
            attribute = {"a": "href", "img": "src"}.get(element.tag)
            if attribute is not None and (url := element.get(attribute)) is not None:
                element.set(attribute, repository_url(url, **self.options, image=element.tag == "img"))


class RepositoryLinksExtension(Extension):
    def __init__(self, **options):
        self.options = options
        super().__init__()

    def extendMarkdown(self, md: Markdown) -> None:
        # Inline Markdown has become links, but MkDocs' relative-path validation
        # (priority 0) has not run. Fenced and inline code remain untouched.
        md.treeprocessors.register(RepositoryLinks(md, **self.options), "repository_links", 1)


def on_page_markdown(markdown: str, *, page: Page, config: MkDocsConfig, **kwargs) -> str:
    root = Path(config.config_file_path).resolve().parent
    docs_dir = Path(config.docs_dir).resolve()
    metadata = config.extra["documentation"]
    source_sha = metadata["source_sha"]
    if source_sha and not re.fullmatch(r"[0-9a-f]{40}", source_sha):
        raise ValueError("DOCS_SOURCE_SHA must be a full lowercase commit SHA")
    if source_sha and page.file.src_uri == "index.md":
        markdown = markdown.replace("git checkout --detach main\n", f"git checkout --detach {source_sha}\n")
    extension = RepositoryLinksExtension(
        page=(docs_dir / page.file.src_uri).resolve(),
        docs_dir=docs_dir,
        root=root,
        repo_url=config.repo_url,
        ref=metadata["source_sha"] or "main",
    )
    config.markdown_extensions = [
        item for item in config.markdown_extensions if not isinstance(item, RepositoryLinksExtension)
    ] + [extension]
    return markdown
