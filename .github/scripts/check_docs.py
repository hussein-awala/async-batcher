"""Check built documentation links, anchors, assets, redirects and version indexes offline."""

from __future__ import annotations

import argparse
import json
import re
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import unquote, urlsplit


class Page(HTMLParser):
    def __init__(self, text: str):
        super().__init__(convert_charrefs=True)
        self.anchors: set[str] = set()
        self.links: list[str] = []
        self.feed(text)

    def handle_starttag(self, tag, attrs):
        attributes = dict(attrs)
        for name in ("id", "name") if tag == "a" else ("id",):
            if attributes.get(name):
                self.anchors.add(attributes[name])
        attribute = {
            "a": "href",
            "link": "href",
            "img": "src",
            "script": "src",
            "iframe": "src",
            "source": "src",
            "video": "src",
            "audio": "src",
        }.get(tag)
        if attribute and attributes.get(attribute):
            self.links.append(attributes[attribute])
        if tag == "video" and attributes.get("poster"):
            self.links.append(attributes["poster"])
        if tag in {"img", "source"} and attributes.get("srcset"):
            self.links.extend(srcset_urls(attributes["srcset"]))
        if tag == "meta" and (attributes.get("http-equiv") or "").lower() == "refresh":
            content = attributes.get("content") or ""
            match = re.search(r";\s*url\s*=\s*(.+)$", content, re.IGNORECASE)
            if match:
                self.links.append(match.group(1).strip().strip("\"'"))

    handle_startendtag = handle_starttag


def srcset_urls(value: str) -> list[str]:
    """Extract candidate URLs without splitting commas inside a data URL."""
    urls = []
    while value:
        value = value.lstrip(" ,\t\n\r\f")
        if not value:
            break
        match = re.match(r"\S+", value)
        assert match is not None
        candidate = match.group()
        value = value[match.end() :]
        if candidate.startswith("data:"):
            # Data candidates are external to this filesystem check.
            if candidate.endswith(","):
                continue
        else:
            pieces = candidate.split(",")
            urls.extend(piece for piece in pieces if piece)
            if candidate.endswith(","):
                continue
        # Skip the optional width/density descriptor up to the next candidate.
        _, separator, value = value.partition(",")
        if not separator:
            break
    return urls


def version_errors(root: Path) -> list[str]:
    manifest = root / "versions.json"
    if not manifest.exists():
        return []
    try:
        versions = json.loads(manifest.read_text(encoding="utf-8"))
        if not isinstance(versions, list) or not versions:
            raise ValueError("expected a nonempty version list")
        seen = set()
        errors = []
        for version in versions:
            if not isinstance(version, dict) or not isinstance(version.get("aliases", []), list):
                raise ValueError("expected version objects with an alias list")
            for name in [version.get("version"), *version.get("aliases", [])]:
                if (
                    not isinstance(name, str)
                    or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", name)
                    or name == "versions.json"
                ):
                    raise ValueError(f"invalid version or alias {name!r}")
                if name in seen:
                    raise ValueError(f"duplicate version or alias {name!r}")
                seen.add(name)
                homepage = root / name / "index.html"
                if not homepage.resolve().is_relative_to(root) or not homepage.is_file():
                    errors.append(f"versions.json: {name!r}: missing version or alias homepage")
        return errors
    except (OSError, UnicodeError, ValueError) as error:
        return [f"versions.json: {error}"]


def check_site(
    directory: Path,
    base_path: str = "/async-batcher/",
    site_url: str = "https://hussein.awala.fr",
) -> list[str]:
    """Check one build mounted at base_path, or the complete version archive."""
    root = directory.resolve()
    if not (root / "index.html").is_file():
        return ["index.html: missing documentation homepage"]
    prefix = "/" + base_path.strip("/")
    if prefix != "/":
        prefix += "/"
    origin = urlsplit(site_url).netloc.lower()
    if not origin:
        raise ValueError("site_url must include a hostname")
    pages = {}
    errors = []
    for path in sorted(root.rglob("*.html")):
        if not path.resolve().is_relative_to(root):
            errors.append(f"{path.relative_to(root)}: HTML file leaves the documentation build")
            continue
        try:
            pages[path.resolve()] = Page(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError) as error:
            errors.append(f"{path.relative_to(root)}: cannot read HTML: {error}")
    for source, page in pages.items():
        for href in dict.fromkeys(page.links):
            try:
                url = urlsplit(href)
                if url.scheme and url.scheme.lower() not in {"http", "https"}:
                    continue
                if url.netloc and url.netloc.lower() != origin:
                    continue
                path = unquote(url.path)
                if path.startswith("/") or url.netloc:
                    if path == prefix.rstrip("/"):
                        path += "/"
                    if not path.startswith(prefix):
                        continue  # Another site or version outside this build's mount point.
                    target = (root / path[len(prefix) :]).resolve()
                else:
                    target = (source.parent / path).resolve() if path else source
                if not target.is_relative_to(root):
                    reason = "relative path leaves the documentation build"
                else:
                    if target.is_dir() or path.endswith("/"):
                        target /= "index.html"
                    if not target.is_file():
                        reason = "target does not exist"
                    elif (
                        url.fragment
                        and target in pages
                        and unquote(url.fragment) not in pages[target].anchors
                    ):
                        reason = "target anchor does not exist"
                    else:
                        continue
            except (OSError, ValueError) as error:
                reason = f"invalid URL or path: {error}"
            errors.append(f"{source.relative_to(root)}: {href!r}: {reason}")
    return errors + version_errors(root)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("site", type=Path, help="Built site directory or complete version archive")
    parser.add_argument("--base-path", default="/async-batcher/", help="URL path mounted at this directory")
    parser.add_argument(
        "--site-url", default="https://hussein.awala.fr", help="Own-site origin for absolute links"
    )
    args = parser.parse_args()
    try:
        errors = check_site(args.site, args.base_path, args.site_url)
    except ValueError as error:
        parser.error(str(error))
    if errors:
        for error in errors[:50]:
            print(error)
        print(f"Documentation check failed: {len(errors)} broken references")
        return 1
    print("Documentation links, anchors, assets, redirects and version indexes passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
