from __future__ import annotations

import argparse
import json
import re
import sys
import time
import tomllib
from datetime import UTC, date, datetime
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

API = "https://api.osv.dev/v1"
CONTROL = ("keras", "3.8.0")  # Known vulnerable; queried only, never installed.
BATCH_SIZE = 100
PYPI_REGISTRIES = {"https://pypi.org/simple", "https://pypi.org/simple/"}
PACKAGE_NAME = re.compile(r"[A-Za-z0-9](?:[A-Za-z0-9._-]*[A-Za-z0-9])?")
# uv serializes normalized PEP 440 versions. Accept literals, never constraints,
# wildcards, or URLs, without installing a dependency before the security audit.
EXACT_VERSION = re.compile(
    r"(?:[0-9]+!)?[0-9]+(?:\.[0-9]+)*(?:(?:a|b|rc)[0-9]+)?"
    r"(?:\.post[0-9]+)?(?:\.dev[0-9]+)?(?:\+[a-z0-9]+(?:\.[a-z0-9]+)*)?"
)


class AuditError(Exception):
    """The audit could not establish a complete result."""


def canonical_name(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


def verify_local_project(path: Path, name: str) -> None:
    """Only the project beside this lock may be excluded as a local source."""
    try:
        project = tomllib.loads(path.with_name("pyproject.toml").read_text()).get("project")
    except (OSError, ValueError) as error:
        raise AuditError("Cannot verify the local project beside the lockfile") from error
    project_name = project.get("name") if isinstance(project, dict) else None
    if (
        not isinstance(project_name, str)
        or not PACKAGE_NAME.fullmatch(project_name)
        or canonical_name(project_name) != canonical_name(name)
    ):
        raise AuditError(f"Local lockfile package {name} does not match the root project")


def load_packages(path: Path) -> dict:
    lock = tomllib.loads(path.read_text())
    if type(lock.get("version")) is not int or lock["version"] != 1:
        raise AuditError("Unsupported uv lockfile schema version; expected version 1")
    entries = lock.get("package")
    if not isinstance(entries, list) or not entries:
        raise AuditError("Lockfile must contain a nonempty package list")
    packages = {}
    local_project_seen = False
    for entry in entries:
        if (
            not isinstance(entry, dict)
            or not isinstance(entry.get("name"), str)
            or not PACKAGE_NAME.fullmatch(entry["name"])
        ):
            raise AuditError("Lockfile package is missing a valid name")
        source = entry.get("source")
        if source in ({"editable": "."}, {"virtual": "."}):
            verify_local_project(path, entry["name"])
            if local_project_seen:
                raise AuditError("Lockfile contains duplicate local project entries")
            local_project_seen = True
            continue
        # PyPI advisories cannot establish the safety of a private index, VCS,
        # URL, or local dependency with the same public package name/version.
        if (
            not isinstance(source, dict)
            or set(source) != {"registry"}
            or not isinstance(source["registry"], str)
            or source["registry"] not in PYPI_REGISTRIES
        ):
            raise AuditError(f"Cannot audit non-PyPI source for {entry['name']}")
        if not isinstance(entry.get("version"), str) or not EXACT_VERSION.fullmatch(entry["version"]):
            raise AuditError(f"Lockfile package {entry['name']} is missing an exact version")
        key = (canonical_name(entry["name"]), entry["version"])
        item = packages.setdefault(key, {"name": key[0], "version": key[1], "lock_entries": []})
        item["lock_entries"].append(
            {
                "source": source,
                "resolution_markers": entry.get("resolution-markers", []),
            }
        )
    if not packages:
        raise AuditError("Lockfile contains no auditable PyPI packages")
    return packages


def request_json(path: str, payload: dict | None = None) -> dict:
    data = None if payload is None else json.dumps(payload).encode()
    request = Request(API + path, data=data, headers={"Content-Type": "application/json"})
    for attempt in range(3):
        try:
            with urlopen(request, timeout=30) as response:
                result = json.load(response)
            if not isinstance(result, dict):
                raise AuditError("OSV returned a non-object response")
            return result
        except HTTPError as error:
            if error.code not in {408, 429, 500, 502, 503, 504} or attempt == 2:
                raise AuditError(f"OSV request failed with HTTP {error.code}") from error
        except (URLError, TimeoutError, OSError) as error:
            if attempt == 2:
                raise AuditError(f"OSV request failed: {error}") from error
        except (ValueError, UnicodeError) as error:
            raise AuditError("OSV returned invalid JSON") from error
        time.sleep(2**attempt)
    raise AuditError("OSV request did not complete")


def query_packages(keys, fetch=request_json) -> dict:
    keys = list(keys)
    findings = {key: set() for key in keys}
    for offset in range(0, len(keys), BATCH_SIZE):
        pending = [(key, None) for key in keys[offset : offset + BATCH_SIZE]]
        seen_tokens = {key: set() for key, _ in pending}
        while pending:
            queries = []
            for (name, version), token in pending:
                query = {"package": {"name": name, "ecosystem": "PyPI"}, "version": version}
                if token:
                    query["page_token"] = token
                queries.append(query)
            response = fetch("/querybatch", {"queries": queries})
            results = response.get("results")
            if not isinstance(results, list) or len(results) != len(pending):
                raise AuditError("OSV returned an incomplete query batch")
            next_page = []
            for (key, _), result in zip(pending, results, strict=True):
                if not isinstance(result, dict) or "error" in result:
                    raise AuditError("OSV returned an invalid package result")
                vulns = result.get("vulns", [])
                if not isinstance(vulns, list):
                    raise AuditError("OSV returned an invalid vulnerability list")
                for vuln in vulns:
                    if not isinstance(vuln, dict) or not isinstance(vuln.get("id"), str) or not vuln["id"]:
                        raise AuditError("OSV returned a vulnerability without an ID")
                    findings[key].add(vuln["id"])
                token = result.get("next_page_token")
                if token is not None and not isinstance(token, str):
                    raise AuditError("OSV returned an invalid pagination token")
                if token:
                    if token in seen_tokens[key] or len(seen_tokens[key]) >= 100:
                        raise AuditError("OSV pagination did not terminate")
                    seen_tokens[key].add(token)
                    next_page.append((key, token))
            pending = next_page
    return findings


def load_exceptions(path: Path) -> dict:
    data = json.loads(path.read_text())
    if not isinstance(data, dict) or not isinstance(data.get("exceptions"), list):
        raise AuditError("Exceptions file must contain an exceptions list")
    exceptions = {}
    for item in data["exceptions"]:
        if not isinstance(item, dict) or not all(
            isinstance(item.get(field), str) and item[field].strip()
            for field in ("name", "version", "id", "reason", "expires")
        ):
            raise AuditError("Each exception needs name, version, ID, reason and expiry")
        try:
            date.fromisoformat(item["expires"])
        except ValueError as error:
            raise AuditError("Exception expiry must be an ISO date") from error
        key = (canonical_name(item["name"]), item["version"], item["id"])
        if key in exceptions:
            raise AuditError(f"Duplicate exception: {key}")
        exceptions[key] = item
    return exceptions


def audit(lock_path: Path, exceptions_path: Path, fetch=request_json, today: date | None = None) -> dict:
    today = today or datetime.now(UTC).date()
    packages = load_packages(lock_path)
    exceptions = load_exceptions(exceptions_path)
    # An all-empty response must not turn a broken upstream feed into a green gate.
    if not query_packages([CONTROL], fetch)[CONTROL]:
        raise AuditError("OSV positive control returned no known vulnerabilities")
    matches = query_packages(packages, fetch)
    details = {}
    for identifier in sorted(set().union(*matches.values())):
        record = fetch("/vulns/" + quote(identifier, safe=""))
        if record.get("id") != identifier:
            raise AuditError(f"OSV returned the wrong advisory record for {identifier}")
        if record.get("withdrawn") is not None:
            try:
                datetime.fromisoformat(record["withdrawn"].replace("Z", "+00:00"))
            except (AttributeError, ValueError) as error:
                raise AuditError(f"OSV advisory {identifier} has an invalid withdrawal date") from error
            continue
        if not isinstance(record.get("affected"), list) or not record["affected"]:
            raise AuditError(f"OSV advisory {identifier} has no affected package data")
        details[identifier] = record
    findings = []
    used_exceptions = set()
    for (name, version), identifiers in matches.items():
        for identifier in sorted(identifiers & details.keys()):
            key = (name, version, identifier)
            exception = exceptions.get(key)
            accepted = exception is not None and date.fromisoformat(exception["expires"]) > today
            if accepted:
                used_exceptions.add(key)
            findings.append(
                {
                    **packages[(name, version)],
                    "id": identifier,
                    "url": "https://osv.dev/vulnerability/" + quote(identifier, safe=""),
                    "summary": details[identifier].get("summary", ""),
                    "accepted": accepted,
                    "exception": exception,
                }
            )
    blocking = [item for item in findings if not item["accepted"]]
    return {
        "status": "fail" if blocking else "pass",
        "audited_at": datetime.now(UTC).isoformat(),
        "packages_audited": len(packages),
        "positive_control": {"name": CONTROL[0], "version": CONTROL[1], "passed": True},
        "findings": findings,
        "blocking_findings": len(blocking),
        "accepted_findings": len(findings) - len(blocking),
        "unused_exceptions": [exceptions[key] for key in exceptions.keys() - used_exceptions],
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Audit every exact version in uv.lock against OSV")
    parser.add_argument("--lock", type=Path, default=Path("uv.lock"))
    parser.add_argument("--exceptions", type=Path, default=Path(".github/lock-audit-exceptions.json"))
    parser.add_argument("--output", type=Path, default=Path("lock-audit.json"))
    args = parser.parse_args(argv)
    try:
        report = audit(args.lock, args.exceptions)
        code = 1 if report["blocking_findings"] else 0
    except (AuditError, OSError, ValueError, TypeError) as error:
        report = {"status": "error", "error": str(error)}
        code = 2
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))
    return code


if __name__ == "__main__":
    sys.exit(main())
