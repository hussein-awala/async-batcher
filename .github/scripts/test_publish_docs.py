from __future__ import annotations

import base64
import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from urllib.error import URLError

import publish_docs as docs

SOURCE = "1" * 40
HEAD = "2" * 40
TREE = "3" * 40
PUBLISHED = "4" * 40


def signed_commit(tree=TREE, sha=PUBLISHED):
    return {
        "oid": sha,
        "url": f"https://github.com/test/repo/commit/{sha}",
        "tree": {"oid": tree},
        "signature": {"isValid": True, "state": "VALID", "wasSignedByGitHub": True},
    }


class VersionPolicyTests(unittest.TestCase):
    def test_main_is_explicitly_unreleased_and_never_latest(self):
        result = docs.publication("refs/heads/main", ["99.0.0"])
        self.assertEqual(
            (result["version"], result["channel"], result["latest"]), ("develop", "develop", False)
        )
        self.assertIn("unreleased", result["title"])

    def test_numeric_tag_order_prevents_maintenance_release_rolling_back_latest(self):
        tags = ["0.9.0", "0.10.0", "0.11.0rc1", "not-a-version"]
        self.assertFalse(docs.publication("refs/tags/0.9.1", tags)["latest"])
        self.assertTrue(docs.publication("refs/tags/0.10.0", tags)["latest"])
        self.assertTrue(docs.publication("refs/tags/0.11.0", tags)["latest"])

    def test_highest_existing_stable_tag_blocks_lower_promotion_even_if_its_build_failed(self):
        self.assertFalse(docs.publication("refs/tags/0.10.1", ["0.11.0"])["latest"])

    def test_prereleases_have_a_visible_label_and_never_promote_latest(self):
        for version in ["1.0.0a1", "1.0.0b2", "1.0.0rc3"]:
            with self.subTest(version=version):
                result = docs.publication(f"refs/tags/{version}", [])
                self.assertFalse(result["latest"])
                self.assertEqual(result["channel"], "prerelease")
                self.assertIn("prerelease", result["title"])

    def test_rejects_unsafe_and_noncanonical_refs(self):
        for ref in [
            "refs/heads/topic",
            "refs/pull/1/merge",
            "refs/tags/../latest",
            "refs/tags/1.2",
            "refs/tags/v1.2.3",
            "refs/tags/01.2.3",
            "refs/tags/1.2.3.post1",
            "refs/tags/1.2.3rc01",
            "refs/tags/1.2.3/child",
            "refs/tags/1.2.3\n",
        ]:
            with self.subTest(ref=ref), self.assertRaises(ValueError):
                docs.publication(ref, [])

    def test_root_uses_develop_until_first_stable_release_then_keeps_latest(self):
        self.assertEqual(docs.default_version([{"version": "develop"}]), "develop")
        self.assertEqual(docs.default_version([{"version": "1.0.0", "aliases": ["latest"]}]), "latest")
        with self.assertRaisesRegex(ValueError, "Bootstrap"):
            docs.default_version([{"version": "1.0.0rc1"}])

    def test_archived_release_cannot_be_replaced_by_another_commit(self):
        previous = [{"version": "1.0.0", "properties": {"source_sha": SOURCE}}]
        policy = docs.publication("refs/tags/1.0.0", [])
        docs.validate_previous(previous, policy, SOURCE)
        with self.assertRaisesRegex(ValueError, "cannot be replaced"):
            docs.validate_previous(previous, policy, HEAD)

    def test_develop_requires_previous_source_to_be_an_ancestor(self):
        previous = [{"version": "develop", "properties": {"source_sha": SOURCE}}]
        with patch.object(docs.subprocess, "run") as run:
            docs.validate_previous(previous, docs.publication("refs/heads/main", []), HEAD)
        run.assert_called_once_with(["git", "merge-base", "--is-ancestor", SOURCE, HEAD], check=True)
        with patch.object(docs.subprocess, "run", side_effect=subprocess.CalledProcessError(1, "git")):
            with self.assertRaises(subprocess.CalledProcessError):
                docs.validate_previous(previous, docs.publication("refs/heads/main", []), HEAD)


class GitTreeTests(unittest.TestCase):
    """Exercise binary Git tree handling without creating or pushing any commits."""

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.repo = self.root / "repo"
        self.repo.mkdir()
        subprocess.run(["git", "init", "--quiet", str(self.repo)], check=True)
        self.original_cwd = Path.cwd()
        os.chdir(self.repo)
        self.addCleanup(os.chdir, self.original_cwd)

    def make_tree(self, files):
        subprocess.run(["git", "read-tree", "--empty"], check=True)
        for path, data in files.items():
            blob = subprocess.run(
                ["git", "hash-object", "-w", "--stdin"], input=data, stdout=subprocess.PIPE, check=True
            )
            subprocess.run(
                [
                    "git",
                    "update-index",
                    "--add",
                    "--cacheinfo",
                    f"100644,{blob.stdout.decode().strip()},{path}",
                ],
                check=True,
            )
        return docs.git("write-tree").decode().strip()

    def test_delta_preserves_binary_files_archives_and_only_deletes_removed_files(self):
        previous = self.make_tree(
            {"old/index.html": b"archived", "develop/removed.html": b"old", "asset.bin": b"\x00\xff"}
        )
        staged = self.make_tree(
            {"old/index.html": b"archived", "develop/index.html": b"new", "asset.bin": b"\x00\xfe"}
        )
        delta = docs.changes(previous, staged)
        self.assertEqual(delta["deletions"], [{"path": "develop/removed.html"}])
        self.assertEqual(
            {entry["path"]: base64.b64decode(entry["contents"]) for entry in delta["additions"]},
            {"develop/index.html": b"new", "asset.bin": b"\x00\xfe"},
        )

    def test_extract_and_verify_preserve_the_exact_archive(self):
        tree = self.make_tree({"develop/index.html": b"\xc3\xa9\n", "asset.bin": b"\x00\xff"})
        output = self.root / "pages"
        docs.extract(tree, output)
        docs.verify_output(tree, output)
        self.assertEqual((output / "asset.bin").read_bytes(), b"\x00\xff")
        with self.assertRaises(FileExistsError):
            docs.extract(tree, output)

    def test_verification_rejects_changed_missing_and_extra_files(self):
        tree = self.make_tree({"index.html": b"original"})
        for mutation in ["changed", "missing", "extra"]:
            with self.subTest(mutation=mutation):
                output = self.root / mutation
                docs.extract(tree, output)
                if mutation == "changed":
                    (output / "index.html").write_bytes(b"not the checked page")
                elif mutation == "missing":
                    (output / "index.html").unlink()
                else:
                    (output / "unreviewed.html").write_bytes(b"new")
                with self.assertRaises(ValueError):
                    docs.verify_output(tree, output)

    def test_git_tree_symlinks_and_executables_are_rejected(self):
        for mode in ["120000", "100755"]:
            with self.subTest(mode=mode):
                tree = self.make_tree({"index.html": b"data"})
                blob = docs.files_at(tree)["index.html"]
                subprocess.run(
                    ["git", "update-index", "--cacheinfo", f"{mode},{blob},index.html"], check=True
                )
                with self.assertRaisesRegex(ValueError, "ordinary"):
                    docs.files_at(docs.git("write-tree").decode().strip())

    def test_artifact_symlinks_and_hardlinks_are_rejected(self):
        tree = self.make_tree({"index.html": b"original"})
        for kind in ["symbolic", "hard"]:
            with self.subTest(kind=kind):
                output = self.root / kind
                docs.extract(tree, output)
                target = self.root / f"target-{kind}"
                (output / "index.html").rename(target)
                if kind == "symbolic":
                    (output / "index.html").symlink_to(target)
                else:
                    os.link(target, output / "index.html")
                with self.assertRaises(ValueError):
                    docs.verify_output(tree, output)

    def test_existing_non_archive_tree_is_rejected(self):
        self.assertEqual(docs.read_versions(self.make_tree({})), [])
        with self.assertRaisesRegex(ValueError, "versions.json"):
            docs.read_versions(self.make_tree({"README.md": b"source tree"}))
        with self.assertRaisesRegex(ValueError, "Invalid"):
            docs.read_versions(self.make_tree({"versions.json": b"{}"}))


class PublishTests(unittest.TestCase):
    def setUp(self):
        self.state = {"expected_head": HEAD, "tree": TREE, "source_sha": SOURCE, "version": "develop"}
        self.output = Path("checked-pages")
        self.delta = {"additions": [{"path": "develop/index.html", "contents": "eA=="}], "deletions": []}
        for name, value in [("verify_output", None), ("git", TREE.encode()), ("changes", self.delta)]:
            mock = patch.object(docs, name, return_value=value)
            setattr(self, name, mock.start())
            self.addCleanup(mock.stop)

    def test_publication_uses_atomic_expected_head_and_only_gh_pages(self):
        response = {"createCommitOnBranch": {"commit": signed_commit()}}
        with patch.object(docs, "graphql", return_value=response) as api:
            result = docs.publish("test/repo", self.state, self.output, "test-token")
        self.verify_output.assert_called_once_with(TREE, self.output)
        query, variables, token = api.call_args.args
        self.assertEqual(query, docs.CREATE_COMMIT)
        self.assertEqual(token, "test-token")
        self.assertEqual(variables["input"]["expectedHeadOid"], HEAD)
        self.assertEqual(
            variables["input"]["branch"], {"repositoryNameWithOwner": "test/repo", "branchName": "gh-pages"}
        )
        self.assertEqual(variables["input"]["fileChanges"], self.delta)
        self.assertEqual(result["commit"], PUBLISHED)

    def test_conflict_is_not_retried_or_force_overwritten(self):
        with patch.object(docs, "graphql", side_effect=RuntimeError("expectedHeadOid mismatch")) as api:
            with self.assertRaisesRegex(RuntimeError, "mismatch"):
                docs.publish("test/repo", self.state, self.output, "test-token")
        self.assertEqual(api.call_count, 1)

    def test_changed_artifact_fails_before_api_mutation(self):
        self.verify_output.side_effect = ValueError("changed artifact")
        with patch.object(docs, "graphql") as api:
            with self.assertRaisesRegex(ValueError, "changed artifact"):
                docs.publish("test/repo", self.state, self.output, "test-token")
        api.assert_not_called()

    def test_missing_invalid_or_non_github_signature_fails_before_deployment(self):
        for signature in [
            None,
            {},
            {"isValid": False, "state": "INVALID"},
            {"isValid": True, "state": "VALID", "wasSignedByGitHub": False},
        ]:
            with self.subTest(signature=signature):
                commit = signed_commit()
                commit["signature"] = signature
                with patch.object(docs, "graphql", return_value={"createCommitOnBranch": {"commit": commit}}):
                    with self.assertRaisesRegex(RuntimeError, "signature|signed"):
                        docs.publish("test/repo", self.state, self.output, "test-token")

    def test_server_tree_must_match_the_checked_artifact(self):
        with patch.object(
            docs, "graphql", return_value={"createCommitOnBranch": {"commit": signed_commit(tree=SOURCE)}}
        ):
            with self.assertRaisesRegex(RuntimeError, "different documentation tree"):
                docs.publish("test/repo", self.state, self.output, "test-token")

    def test_noop_checks_current_head_and_signature_without_creating_commit(self):
        self.changes.return_value = {"additions": [], "deletions": []}
        commit = signed_commit(sha=HEAD)
        commit["signature"]["wasSignedByGitHub"] = (
            False  # A valid maintainer-signed archive is also acceptable.
        )
        with patch.object(docs, "graphql", return_value={"repository": {"ref": {"target": commit}}}) as api:
            self.assertEqual(docs.publish("test/repo", self.state, self.output, "test-token")["commit"], HEAD)
        self.assertEqual(api.call_args.args[0], docs.CURRENT_COMMIT)
        commit["oid"] = PUBLISHED
        with patch.object(docs, "graphql", return_value={"repository": {"ref": {"target": commit}}}):
            with self.assertRaisesRegex(RuntimeError, "changed after preparation"):
                docs.publish("test/repo", self.state, self.output, "test-token")

    def test_token_repository_and_object_ids_are_validated_before_network(self):
        for repository, token, state in [
            ("../repo", "test-token", self.state),
            ("test/repo", "", self.state),
            ("test/repo", "test-token", {**self.state, "tree": "HEAD"}),
        ]:
            with self.subTest(repository=repository, token=bool(token)), patch.object(docs, "graphql") as api:
                with self.assertRaises(ValueError):
                    docs.publish(repository, state, self.output, token)
                api.assert_not_called()


class PrepareTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.output, self.state = self.root / "pages", self.root / "state.json"

    def mock_git(self, *args):
        if args == ("rev-parse", "HEAD") or args == ("rev-parse", "refs/remotes/origin/main"):
            return SOURCE.encode()
        if args == ("rev-parse", docs.REMOTE_REF) or args == ("rev-parse", docs.BRANCH):
            return HEAD.encode()
        if args == ("tag", "--list"):
            return b"0.2.2\n"
        if args == ("rev-parse", f"{HEAD}^{{tree}}"):
            return TREE.encode()
        if args == ("rev-parse", f"{docs.BRANCH}^{{tree}}"):
            return PUBLISHED.encode()
        raise AssertionError(args)

    def test_prepare_builds_locally_with_metadata_and_never_publishes(self):
        with (
            patch.object(docs, "git", side_effect=self.mock_git),
            patch.object(docs, "read_versions", side_effect=[[], [{"version": "develop"}]]),
            patch.object(docs, "files_at", return_value={"develop/index.html": SOURCE, "index.html": HEAD}),
            patch.object(docs, "extract") as extract,
            patch.object(docs.subprocess, "run") as run,
            patch.object(docs, "graphql") as api,
            patch.dict(os.environ, {}, clear=True),
        ):
            result = docs.prepare("refs/heads/main", SOURCE, self.output, self.state)
        api.assert_not_called()
        extract.assert_called_once_with(PUBLISHED, self.output)
        self.assertEqual(result["expected_head"], HEAD)
        self.assertEqual(json.loads(self.state.read_text()), result)
        self.assertEqual(len(run.call_args_list), 2)
        first = run.call_args_list[0]
        self.assertEqual(first.args[0][:2], ["mike", "deploy"])
        self.assertNotIn("--push", first.args[0])
        self.assertNotIn("latest", first.args[0])
        self.assertIn("source_sha=" + SOURCE, first.args[0])
        self.assertEqual(first.kwargs["env"]["DOCS_CHANNEL"], "develop")
        self.assertEqual(first.kwargs["env"]["DOCS_SOURCE_SHA"], SOURCE)
        self.assertEqual(first.kwargs["env"]["GIT_COMMITTER_NAME"], "github-actions[bot]")
        self.assertEqual(run.call_args_list[1].args[0][-1], "develop")

    def test_prepare_refuses_old_main_before_build_or_api(self):
        def old_main(*args):
            return (
                HEAD.encode() if args == ("rev-parse", "refs/remotes/origin/main") else self.mock_git(*args)
            )

        with patch.object(docs, "git", side_effect=old_main), patch.object(docs.subprocess, "run") as run:
            with self.assertRaisesRegex(ValueError, "current fetched main"):
                docs.prepare("refs/heads/main", SOURCE, self.output, self.state)
        run.assert_not_called()

    def test_prepare_does_not_overwrite_existing_output(self):
        self.output.mkdir()
        existing = self.output / "user-file"
        existing.write_text("preserve me")
        with (
            patch.object(docs, "git", side_effect=self.mock_git),
            patch.object(docs.subprocess, "run") as run,
        ):
            with self.assertRaisesRegex(ValueError, "new output"):
                docs.prepare("refs/heads/main", SOURCE, self.output, self.state)
        run.assert_not_called()
        self.assertEqual(existing.read_text(), "preserve me")

    def test_prepare_requires_exact_checkout(self):
        with (
            patch.object(docs, "git", return_value=HEAD.encode()),
            patch.object(docs.subprocess, "run") as run,
        ):
            with self.assertRaisesRegex(ValueError, "does not match"):
                docs.prepare("refs/heads/main", SOURCE, self.output, self.state)
        run.assert_not_called()


class ApiTests(unittest.TestCase):
    def test_graphql_errors_and_missing_data_fail_closed(self):
        for body in [{"errors": [{"message": "head changed"}]}, {}, {"data": None}]:
            with self.subTest(body=body), patch.object(docs, "urlopen") as opening:
                opening.return_value.__enter__.return_value.read.return_value = json.dumps(body)
                with self.assertRaisesRegex(RuntimeError, "rejected"):
                    docs.graphql("query {}", {}, "test-token")

    def test_network_failure_is_not_retried(self):
        with patch.object(docs, "urlopen", side_effect=URLError("unreachable")) as opening:
            with self.assertRaisesRegex(RuntimeError, "request failed"):
                docs.graphql("query {}", {}, "test-token")
        self.assertEqual(opening.call_count, 1)

    def test_success_keeps_token_in_header_and_uses_bounded_network_wait(self):
        with patch.object(docs, "urlopen") as opening:
            opening.return_value.__enter__.return_value.read.return_value = '{"data":{"result":1}}'
            self.assertEqual(docs.graphql("query {}", {"value": "x"}, "test-token"), {"result": 1})
        request = opening.call_args.args[0]
        self.assertEqual(request.get_header("Authorization"), "Bearer test-token")
        self.assertNotIn(b"test-token", request.data)
        self.assertEqual(opening.call_args.kwargs["timeout"], 120)


if __name__ == "__main__":
    unittest.main()
