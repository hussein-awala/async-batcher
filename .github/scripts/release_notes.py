"""Build the shared release history and extract canonical GitHub release bodies."""

from __future__ import annotations

import argparse
import datetime
import re
import tempfile
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit
from xml.etree.ElementTree import SubElement

ROOT = Path(__file__).resolve().parents[2]
RELEASE_URL = "https://github.com/hussein-awala/async-batcher/releases/tag/"
VERSION = re.compile(r"(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:(a|b|rc)(0|[1-9]\d*))?")


@dataclass(frozen=True)
class Release:
    version: str
    title: str
    date: str | None
    status: str
    github_url: str
    body: str


def version_key(version: str) -> tuple[int, int, int, int, int]:
    match = VERSION.fullmatch(version)
    if match is None:
        raise ValueError(f"Invalid release version: {version!r}")
    major, minor, patch, stage, number = match.groups()
    return int(major), int(minor), int(patch), {"a": 0, "b": 1, "rc": 2, None: 3}[stage], int(number or 0)


def load_release(path: Path) -> Release:
    import yaml

    text = path.read_bytes().decode("utf-8")
    if not text.startswith("---\n") or "\n---\n\n" not in text[4:]:
        raise ValueError(f"{path}: expected YAML front matter followed by one blank line")
    header, body = text[4:].split("\n---\n\n", 1)
    metadata = yaml.safe_load(header)
    fields = {"version", "title", "date", "status", "github_url"}
    if not isinstance(metadata, dict) or set(metadata) != fields:
        raise ValueError(f"{path}: expected metadata fields {sorted(fields)}")
    version = metadata["version"]
    if not isinstance(version, str):
        raise ValueError(f"{path}: version must be a string")
    version_key(version)
    if path.stem != version:
        raise ValueError(f"{path}: version must match the filename")
    if not isinstance(metadata["title"], str) or not metadata["title"].strip():
        raise ValueError(f"{path}: title must be a nonempty string")
    if metadata["status"] not in ("planned", "published"):
        raise ValueError(f"{path}: status must be planned or published")
    date = metadata["date"]
    if date is not None:
        if not isinstance(date, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", date):
            raise ValueError(f"{path}: date must be a quoted ISO date or null")
        datetime.date.fromisoformat(date)
    if metadata["status"] == "published" and date is None:
        raise ValueError(f"{path}: published releases require a date")
    if metadata["github_url"] != RELEASE_URL + version:
        raise ValueError(f"{path}: github_url must point to this repository's matching release tag")
    if not body.strip():
        raise ValueError(f"{path}: release body must not be empty")
    return Release(**metadata, body=body)


def load_releases(directory: Path) -> list[Release]:
    releases = [load_release(path) for path in directory.glob("*.md")]
    if not releases:
        raise ValueError(f"{directory}: no release notes found")
    return sorted(releases, key=lambda release: version_key(release.version), reverse=True)


def demote_headings(body: str) -> str:
    """Nest ATX headings below each release without changing fenced code examples."""
    result = []
    fence = None
    for line in body.splitlines(keepends=True):
        marker = re.match(r"^ {0,3}(`{3,}|~{3,})(.*)$", line.rstrip("\r\n"))
        if fence:
            if marker and marker[1][0] == fence[0] and len(marker[1]) >= len(fence) and not marker[2].strip():
                fence = None
        elif marker and (marker[1][0] != "`" or "`" not in marker[2]):
            fence = marker[1]
        else:
            line = re.sub(r"^( {0,3})(#{1,5})(?=\s|$)", r"\1#\2", line)
        result.append(line)
    return "".join(result)


def release_preview_url(url: str, planned_versions: set[str]) -> str:
    """Route planned documentation and repository comparisons to development previews."""
    parts = urlsplit(url)
    if parts.scheme not in ("", "http", "https") or (parts.scheme and not parts.netloc):
        return url
    docs_prefix = "/async-batcher/"
    compare_prefix = "/hussein-awala/async-batcher/compare/"
    if parts.netloc in ("", "hussein.awala.fr") and parts.path.startswith(docs_prefix):
        version, separator, page = parts.path.removeprefix(docs_prefix).partition("/")
        if version not in planned_versions:
            return url
        path = docs_prefix + "develop" + separator + page
    elif parts.netloc == "github.com" and parts.path.startswith(compare_prefix):
        base, separator, target = parts.path.removeprefix(compare_prefix).rpartition("...")
        if not base or not separator or target not in planned_versions:
            return url
        path = compare_prefix + base + "...main"
    else:
        return url
    return urlunsplit((parts.scheme, parts.netloc, path, parts.query, parts.fragment))


def preview_links_extension(planned_versions: set[str]):
    # Extraction and metadata checks also run in environments without Markdown.
    from markdown.extensions import Extension
    from markdown.treeprocessors import Treeprocessor

    class PreviewLinks(Treeprocessor):
        def run(self, root):
            for link in root.iter("a"):
                url = link.get("href", "")
                preview = release_preview_url(url, planned_versions)
                if preview != url:
                    link.set("href", preview)
                    SubElement(link, "span").text = " (development preview)"

    class PreviewLinksExtension(Extension):
        def extendMarkdown(self, md):
            # Rewrite parsed inline, reference and automatic links. Code stays
            # untouched, and MkDocs validates the resulting destinations next.
            md.treeprocessors.register(PreviewLinks(md), "planned_release_links", 1)

    return PreviewLinksExtension()


def render_index(releases: list[Release]) -> str:
    sections = [
        "# Release notes\n\n"
        "Published GitHub release notes and upcoming drafts live on this permanent page, "
        "independently of the "
        "documentation version selected elsewhere on this site.\n\n"
        "Releases marked **In preparation** are drafts. Their installation commands and "
        "release links become available when the release is published.\n"
    ]
    for release in releases:
        heading = f"\n## {release.title}\n\n"
        if release.status == "planned":
            heading += (
                "**In preparation** — this release has not been published. "
                "Documentation and changelog links open development previews "
                "and may change before release.\n\n"
            )
        else:
            heading += f"Published {release.date} · [GitHub release]({release.github_url})\n\n"
        sections.append(heading + demote_headings(release.body).rstrip("\n") + "\n")
    return "".join(sections)


def build(output_dir: Path) -> None:
    from mkdocs.commands.build import build as build_site
    from mkdocs.config import load_config

    releases = load_releases(ROOT / "release-notes")
    with tempfile.TemporaryDirectory(prefix="async-batcher-release-notes-") as temporary:
        docs = Path(temporary) / "docs"
        docs.mkdir()
        (docs / "index.md").write_text(render_index(releases), encoding="utf-8")
        config = load_config(
            config_file=str(ROOT / "mkdocs-release-notes.yml"),
            docs_dir=str(docs),
            site_dir=str(output_dir.resolve()),
        )
        config.markdown_extensions.append(
            preview_links_extension({release.version for release in releases if release.status == "planned"})
        )
        build_site(config)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("check", help="Validate all canonical release notes")
    extract = commands.add_parser("extract", help="Write the unchanged body for a GitHub release")
    extract.add_argument("version")
    extract.add_argument("--output", type=Path, required=True)
    site = commands.add_parser("build", help="Build the independent public release history")
    site.add_argument("--output-dir", type=Path, required=True)
    arguments = parser.parse_args()
    try:
        if arguments.command == "build":
            build(arguments.output_dir)
        elif arguments.command == "extract":
            version_key(arguments.version)
            release = load_release(ROOT / "release-notes" / f"{arguments.version}.md")
            arguments.output.write_bytes(release.body.encode("utf-8"))
        else:
            load_releases(ROOT / "release-notes")
    except (OSError, ValueError) as error:
        parser.exit(1, f"{error}\n")


if __name__ == "__main__":
    main()
