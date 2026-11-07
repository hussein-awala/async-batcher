# Release preparation and publication commands

These are examples for the existing tools, not a script to execute wholesale. Re-read the
selected target's workflow before use. Prefer connected GitHub tools when they support the
operation; `gh` is the fallback for releases, workflow inspection, and discussions. Values
below must come from the user-authorized release decision, not from this document.

## Non-publishing inventory and preview

```bash
git fetch origin main --tags
git status --short
gh release list --repo hussein-awala/async-batcher
gh api 'repos/hussein-awala/async-batcher/tags?per_page=100'
curl --fail --silent --show-error https://pypi.org/pypi/async-batcher/json
```

Follow API pagination when the release/tag list grows beyond one page. Check the remote
literal tag and the normalized PyPI version; an existing version is not a new-release target.
Inspect existing releases/runs when asked to recover or finish a partially completed release.

Set `release_version`, `release_sha`, and `previous_tag` to the decided values. Require an
exact commit SHA after resolving the target. Use `git log "$previous_tag..$release_sha"` and
`git diff "$previous_tag" "$release_sha"` for notes; review the target's checks using the
GitHub connector or `gh run list --commit "$release_sha" --repo hussein-awala/async-batcher`.
A branch that moves during preparation is not permission to change the tested target.

The current `CI` workflow selects every adapter and service on `workflow_dispatch`; a
normal main push always covers the full core matrix but can select only changed adapters.
The advisory workflow can be absent on a source-only push because it has path filters.
When full evidence for the target is missing and running checks is authorized, dispatch the
two existing workflows against an existing branch or tag that resolves to `release_sha`:

```bash
gh workflow run ci.yml --ref "$release_ref" --repo hussein-awala/async-batcher
gh workflow run dependency-audit.yml --ref "$release_ref" --repo hussein-awala/async-batcher
gh run list --workflow ci.yml --event workflow_dispatch --commit "$release_sha" \
  --repo hussein-awala/async-batcher
gh run list --workflow dependency-audit.yml --event workflow_dispatch --commit "$release_sha" \
  --repo hussein-awala/async-batcher
```

Verify the new runs' `headSha` equals the target, inspect the selected matrix and all required
jobs, and wait for completion. If the ref moved, those runs do not validate the old target.
Do not create a release tag merely to obtain a test ref: tag push itself publishes. Reuse
adequate completed evidence rather than dispatching duplicate sweeps. An audit report must
be complete and acceptable under current policy; scanner/network errors are not a clean audit.

For a local preview, export the exact commit into a new temporary directory. Use the uv
version pinned by the setup action, with Python 3.12 as in the release workflow:

```bash
release_preview=$(mktemp -d)
git archive "$release_sha" | tar -x -C "$release_preview"
(
  cd "$release_preview"
  uv version --no-sync --python 3.12 "$release_version"
  uv lock --check --python 3.12
  uv build --python 3.12
  uv run --no-project --python 3.12 python .github/scripts/check_distributions.py
)
```

`uv version --no-sync` still re-locks the project; it skips environment synchronization,
not the lock update. Compare the preview's project/lock with the originals: the root
version and corresponding metadata may change, while external dependency versions should
remain fixed. The existing distribution checker builds both formats, rebuilds the wheel
from the sdist, compares packaged source and metadata, and tests installed imports outside
the checkout. Adapter installed-wheel checks use their SDK environments and remain part
of the full CI evidence; a core-only preview does not replace them.

Inspect the output wheel/sdist metadata for the chosen version, Python floor, extras, and
`py.typed`. Remove only the scratch directory you created when its artifacts are no longer
needed. Do not commit the preview's version override or publish from it.

## Authorized publication only

Recheck version/tag availability and the exact source SHA immediately before tag creation.
Follow the user's signing configuration; preparation commits use `git commit -S` and new
release tags use `git tag -s`. If signing is unavailable, report that blocker instead of
silently creating an unsigned tag.

SSH signatures also require an already trusted allowed-signers file for verification. If
`gpg.ssh.allowedSignersFile` is not configured, use
`git -c gpg.ssh.allowedSignersFile="$release_allowed_signers" verify-tag "$release_version"`
instead of bare `git verify-tag` below, with the existing trusted file's path. Do not trust
a key solely because the tag supplies it, generate trust from an unverified signature, or
change global Git configuration to suppress a verification failure.

```bash
(
  set -e
  git tag -s "$release_version" "$release_sha" -m "Async Batcher $release_version"
  git verify-tag "$release_version"
  test "$(git rev-parse "$release_version^{commit}")" = "$release_sha"
)
```

Proceed only if that subshell exits successfully, with the SSH verification command
substituted when required. On a failure, keep the tag local and resolve the verification or
SHA mismatch; do not run the publication command below. The separate push starts automatic
PyPI publication:

```bash
git push origin "refs/tags/$release_version:refs/tags/$release_version"
```

Push only that ref; never use `--tags`, `--force`, or a replacement tag. Watch the new
`Python Release` run for this tag/SHA. Verify
both `build-wheels` and `pypi-publish`, then inspect
`https://pypi.org/pypi/async-batcher/NORMALIZED_VERSION/json` and the project release page.
Confirm the expected wheel and sdist, metadata, and file digests against the run's build
artifacts when available. Do not announce success based only on a tag or GitHub release.

If publication fails or its outcome is uncertain, inspect both the run and PyPI before
retrying. Do not re-run a successful upload, move/delete a tag, upload a different file under
the same version, or mint another version to avoid diagnosing the failure. The current publish
action does not enable `skip-existing`, so classify the actual PyPI state before recovery:

- **No expected files uploaded:** diagnose the failure, then a single justified failed-job
  retry can be appropriate if the same verified artifacts and authorization still apply.
- **Both expected files exist with matching digests:** package upload is complete. Investigate
  any remaining workflow error without uploading the files again.
- **Only a subset exists, or any digest differs:** stop and report the exact files and digests.
  A blind failed-job rerun can fail when it uploads an already existing filename. Recover
  only under an explicit plan that preserves the verified artifacts; do not enable
  `skip-existing`, rebuild different files, or delete published files as an automatic workaround.

Stop and report an unresolved failure after a justified retry; do not loop on publication.

After confirmed package publication, create the authorized GitHub release from that existing
tag and the reviewed notes file:

```bash
gh release create "$release_version" \
  --repo hussein-awala/async-batcher \
  --verify-tag \
  --title "Async Batcher $release_version" \
  --notes-file /absolute/path/to/reviewed-release-notes.md
```

For an authorized prerelease, add `--prerelease --latest=false`. Do not mark a backport as
latest merely because it was just published. `--verify-tag` prevents the CLI from creating
an absent tag implicitly. Do not add `--discussion-category` unless announcement posting
was explicitly requested; when authorized, the existing category is `Announcements`.
If the release already exists, inspect and update that release within the requested scope
instead of creating a duplicate. Preserve notes files' real newlines with `--notes-file`.

References: [uv version](https://docs.astral.sh/uv/reference/cli/#uv-version),
[uv build and packaging](https://docs.astral.sh/uv/guides/package/),
[GitHub release CLI](https://cli.github.com/manual/gh_release_create),
and [PyPI trusted publishing](https://docs.pypi.org/trusted-publishers/using-a-publisher/).
