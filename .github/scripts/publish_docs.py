"""Stage mike documentation, then persist its checked tree with a GitHub-signed commit.

Only ``publish`` contacts GitHub. mike's temporary local commits are never pushed;
GitHub's createCommitOnBranch mutation signs the single published archive commit.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import re
import subprocess
from pathlib import Path, PurePosixPath
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

BRANCH = "gh-pages"
REMOTE_REF = "refs/remotes/origin/gh-pages"
OID = re.compile(r"[0-9a-f]{40}")
NUMBER = r"(?:0|[1-9][0-9]*)"
STABLE = re.compile(rf"({NUMBER})\.({NUMBER})\.({NUMBER})")
VERSION = re.compile(rf"{NUMBER}\.{NUMBER}\.{NUMBER}(?:(?:a|b|rc){NUMBER})?")
COMMIT_FIELDS = "oid url tree { oid } signature { isValid state wasSignedByGitHub }"
CREATE_COMMIT = """
mutation PublishDocs($input: CreateCommitOnBranchInput!) {
  createCommitOnBranch(input: $input) { commit { COMMIT_FIELDS } }
}
""".replace("COMMIT_FIELDS", COMMIT_FIELDS)
CURRENT_COMMIT = """
query CurrentDocs($owner: String!, $name: String!) {
  repository(owner: $owner, name: $name) {
    ref(qualifiedName: "refs/heads/gh-pages") {
      target { ... on Commit { COMMIT_FIELDS } }
    }
  }
}
""".replace("COMMIT_FIELDS", COMMIT_FIELDS)


def git(*args: str) -> bytes:
    return subprocess.run(["git", *args], check=True, stdout=subprocess.PIPE).stdout


def oid(value: str) -> str:
    if OID.fullmatch(value) is None:
        raise ValueError("Expected a full lowercase Git object ID")
    return value


def publication(ref: str, tags: list[str]) -> dict:
    if ref == "refs/heads/main":
        return {
            "version": "develop",
            "channel": "develop",
            "title": "Development (unreleased)",
            "latest": False,
        }
    if not ref.startswith("refs/tags/"):
        raise ValueError("Documentation can publish only main or a version tag")
    version = ref.removeprefix("refs/tags/")
    if VERSION.fullmatch(version) is None:
        raise ValueError("Documentation tags must use X.Y.Z, X.Y.ZaN, X.Y.ZbN, or X.Y.ZrcN")
    stable = STABLE.fullmatch(version)
    highest = max(
        (tuple(map(int, match.groups())) for tag in tags if (match := STABLE.fullmatch(tag))), default=()
    )
    return {
        "version": version,
        "channel": "release" if stable else "prerelease",
        "title": version if stable else f"{version} (prerelease)",
        "latest": bool(stable and tuple(map(int, stable.groups())) >= highest),
    }


def files_at(tree: str) -> dict[str, str]:
    """Read a tree, rejecting links and modes the GraphQL API cannot preserve."""
    result = {}
    for entry in git("ls-tree", "-r", "-z", oid(tree)).split(b"\0"):
        if not entry:
            continue
        metadata, raw_path = entry.split(b"\t", 1)
        mode, kind, blob = metadata.decode("ascii").split()
        path = raw_path.decode("utf-8")
        parts = PurePosixPath(path).parts
        if mode != "100644" or kind != "blob":
            raise ValueError(f"Documentation must contain ordinary non-executable files: {path}")
        if not parts or path.startswith("/") or ".." in parts or "\\" in path or ".git" in parts:
            raise ValueError(f"Unsafe documentation path: {path}")
        result[path] = oid(blob)
    return result


def read_versions(tree: str) -> list[dict]:
    files = files_at(tree)
    if "versions.json" not in files:
        if files:
            raise ValueError("An existing documentation archive must contain versions.json")
        return []
    versions = json.loads(git("cat-file", "blob", files["versions.json"]))
    if not isinstance(versions, list) or not all(isinstance(row, dict) for row in versions):
        raise ValueError("Invalid documentation versions.json")
    return versions


def default_version(versions: list[dict]) -> str:
    if any("latest" in row.get("aliases", []) for row in versions):
        return "latest"
    if any(row["version"] == "develop" for row in versions):
        return "develop"
    raise ValueError("Bootstrap develop documentation before publishing releases")


def validate_previous(versions: list[dict], policy: dict, source_sha: str) -> None:
    previous = next((row for row in versions if row["version"] == policy["version"]), None)
    if previous is None:
        return
    previous_sha = previous.get("properties", {}).get("source_sha")
    if policy["channel"] != "develop":
        if previous_sha != source_sha:
            raise ValueError("An archived release cannot be replaced with a different source commit")
    elif previous_sha and previous_sha != source_sha:
        # Out-of-order main jobs must not roll the public development site backward.
        subprocess.run(["git", "merge-base", "--is-ancestor", oid(previous_sha), source_sha], check=True)


def extract(tree: str, output_dir: Path) -> None:
    files = files_at(tree)
    output_dir.mkdir(parents=True, exist_ok=False)
    for path, blob in files.items():
        destination = output_dir / path
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(git("cat-file", "blob", blob))


def verify_output(tree: str, output_dir: Path) -> None:
    files = files_at(tree)
    if not output_dir.is_dir() or output_dir.is_symlink():
        raise ValueError("The checked documentation directory is missing or is a symbolic link")
    actual = set()
    for path in output_dir.rglob("*"):
        relative = path.relative_to(output_dir).as_posix()
        if path.is_symlink():
            raise ValueError(f"Documentation artifact contains a symbolic link: {relative}")
        if path.is_dir():
            continue
        if not path.is_file() or path.stat().st_nlink != 1:
            raise ValueError(f"Documentation artifact contains a non-regular or linked file: {relative}")
        actual.add(relative)
        if relative not in files or path.read_bytes() != git("cat-file", "blob", files[relative]):
            raise ValueError(f"Documentation artifact differs from the staged archive: {relative}")
    if actual != files.keys():
        raise ValueError("Documentation artifact is missing staged files")


def prepare(ref: str, source_sha: str, output_dir: Path, state_path: Path) -> dict:
    oid(source_sha)
    if git("rev-parse", "HEAD").decode().strip() != source_sha:
        raise ValueError("Documentation source SHA does not match the checkout")
    if (
        ref == "refs/heads/main"
        and git("rev-parse", "refs/remotes/origin/main").decode().strip() != source_sha
    ):
        raise ValueError("Development documentation must build the current fetched main commit")
    if output_dir.exists() or state_path.exists():
        raise ValueError("Use new output and state paths for each preparation")
    expected = oid(git("rev-parse", REMOTE_REF).decode().strip())
    if git("rev-parse", BRANCH).decode().strip() != expected:
        raise ValueError("Local gh-pages must match the freshly fetched origin/gh-pages")
    baseline_tree = oid(git("rev-parse", f"{expected}^{{tree}}").decode().strip())
    policy = publication(ref, git("tag", "--list").decode().splitlines())
    if (
        policy["channel"] != "develop"
        and git("rev-parse", f"{ref}^{{commit}}").decode().strip() != source_sha
    ):
        raise ValueError("Documentation tag does not identify the checked-out source commit")
    previous = read_versions(baseline_tree)
    validate_previous(previous, policy, source_sha)
    if not previous and policy["channel"] != "develop":
        raise ValueError("Bootstrap develop documentation before publishing releases")
    env = {
        **os.environ,
        "DOCS_SOURCE_SHA": source_sha,
        "DOCS_VERSION": policy["version"],
        "DOCS_CHANNEL": policy["channel"],
        "GIT_COMMITTER_NAME": os.environ.get("GIT_COMMITTER_NAME") or "github-actions[bot]",
        "GIT_COMMITTER_EMAIL": os.environ.get("GIT_COMMITTER_EMAIL")
        or "41898282+github-actions[bot]@users.noreply.github.com",
    }
    command = [
        "mike",
        "deploy",
        "--branch",
        BRANCH,
        "--alias-type",
        "redirect",
        "--title",
        policy["title"],
        "--prop-set-string",
        f"source_sha={source_sha}",
        "--prop-set-string",
        f"channel={policy['channel']}",
    ]
    if policy["latest"]:
        command.append("--update-aliases")
    command.append(policy["version"])
    if policy["latest"]:
        command.append("latest")
    # No --push: mike's local archive commits are staging objects, never public history.
    subprocess.run(command, env=env, check=True)
    staged = oid(git("rev-parse", f"{BRANCH}^{{tree}}").decode().strip())
    default = default_version(read_versions(staged))
    subprocess.run(["mike", "set-default", "--branch", BRANCH, default], env=env, check=True)
    staged = oid(git("rev-parse", f"{BRANCH}^{{tree}}").decode().strip())
    files = files_at(staged)
    if f"{policy['version']}/index.html" not in files or "index.html" not in files:
        raise ValueError("The staged archive lacks its versioned homepage or root redirect")
    extract(staged, output_dir)
    state = {"expected_head": expected, "tree": staged, "source_sha": source_sha, **policy}
    state_path.write_text(json.dumps(state, indent=2) + "\n", encoding="utf-8")
    return state


def changes(before: str, after: str) -> dict:
    old = files_at(before)
    new = files_at(after)
    return {
        "additions": [
            {"path": path, "contents": base64.b64encode(git("cat-file", "blob", blob)).decode("ascii")}
            for path, blob in new.items()
            if old.get(path) != blob
        ],
        "deletions": [{"path": path} for path in old if path not in new],
    }


def graphql(query: str, variables: dict, token: str) -> dict:
    request = Request(
        "https://api.github.com/graphql",
        data=json.dumps({"query": query, "variables": variables}).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": "application/vnd.github+json",
            "User-Agent": "async-batcher-docs",
        },
        method="POST",
    )
    try:
        with urlopen(request, timeout=120) as response:
            body = json.load(response)
    except (HTTPError, URLError) as error:
        raise RuntimeError(f"GitHub documentation API request failed: {error}") from error
    if body.get("errors") or not isinstance(body.get("data"), dict):
        raise RuntimeError(
            f"GitHub documentation API rejected the request: {body.get('errors', 'missing data')}"
        )
    return body["data"]


def checked_commit(commit: dict | None, tree: str, *, github_signed: bool) -> str:
    if not commit or commit.get("tree", {}).get("oid") != tree:
        raise RuntimeError("GitHub returned a different documentation tree")
    signature = commit.get("signature") or {}
    if signature.get("isValid") is not True or signature.get("state") != "VALID":
        raise RuntimeError("Documentation commit signature is not verified; do not deploy")
    if github_signed and signature.get("wasSignedByGitHub") is not True:
        raise RuntimeError("The published documentation commit was not signed by GitHub")
    return oid(commit["oid"])


def publish(repository: str, state: dict, output_dir: Path, token: str) -> dict:
    if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9-]*/[A-Za-z0-9_.-]+", repository) is None:
        raise ValueError("Expected owner/repository")
    if not token:
        raise ValueError("GH_TOKEN is required for publication")
    expected, tree, source_sha = (oid(state[key]) for key in ("expected_head", "tree", "source_sha"))
    verify_output(tree, output_dir)
    baseline = oid(git("rev-parse", f"{expected}^{{tree}}").decode().strip())
    delta = changes(baseline, tree)
    if not delta["additions"] and not delta["deletions"]:
        owner, name = repository.split("/", 1)
        data = graphql(CURRENT_COMMIT, {"owner": owner, "name": name}, token)
        commit = data["repository"]["ref"]["target"]
        if checked_commit(commit, tree, github_signed=False) != expected:
            raise RuntimeError("Documentation branch changed after preparation; prepare again")
    else:
        data = graphql(
            CREATE_COMMIT,
            {
                "input": {
                    "branch": {"repositoryNameWithOwner": repository, "branchName": BRANCH},
                    "expectedHeadOid": expected,
                    "message": {"headline": f"Publish documentation {state['version']} from {source_sha}"},
                    "fileChanges": delta,
                }
            },
            token,
        )
        commit = data["createCommitOnBranch"]["commit"]
        checked_commit(commit, tree, github_signed=True)
    return {"commit": commit["oid"], "url": commit["url"], "tree": tree, "source_sha": source_sha}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    stage = commands.add_parser("prepare", help="Build and extract a versioned archive without publishing")
    stage.add_argument("--ref", required=True)
    stage.add_argument("--source-sha", required=True)
    stage.add_argument("--output-dir", type=Path, required=True)
    stage.add_argument("--state", type=Path, required=True)
    send = commands.add_parser(
        "publish", help="Persist the checked archive with a verified GitHub-signed commit"
    )
    send.add_argument("--repository", required=True)
    send.add_argument("--output-dir", type=Path, required=True)
    send.add_argument("--state", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "prepare":
        result = prepare(args.ref, args.source_sha, args.output_dir, args.state)
    else:
        result = publish(
            args.repository,
            json.loads(args.state.read_text(encoding="utf-8")),
            args.output_dir,
            os.environ.get("GH_TOKEN", ""),
        )
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
