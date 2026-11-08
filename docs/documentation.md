# Documentation publication

The [documentation site](https://hussein.awala.fr/async-batcher/) follows the
MkDocs Material and mike setup used by
[spark-on-k8s](https://github.com/hussein-awala/spark-on-k8s). The existing Markdown
guides remain readable on GitHub. Search, navigation, code copying, light/dark
mode, and the version selector are provided by Material.

## Build and preview

From the repository root:

```bash
uv sync --locked --no-default-groups --python 3.12 --group docs
uv run --no-sync python -m unittest discover -s docs/tests -p 'test_*.py'
uv run --no-sync python -m unittest discover -s .github/scripts -p '*docs*.py'
uv run --no-sync mkdocs build --strict
uv run --no-sync python .github/scripts/check_docs.py site --version-preview
uv run --no-sync mkdocs serve
```

These commands do not publish. The docs group adds no core runtime dependencies
and does not need optional backend clients. Every Markdown guide must appear in
`mkdocs.yml`; missing pages, broken internal links, and unknown anchors fail the
strict build or the rendered-site checker. The checker also validates local
assets and redirects; it does not make network requests to external sites.

Keep normal repository-relative links in Markdown. `docs/hooks.py` converts links
that leave `docs/` into GitHub source links at `DOCS_SOURCE_SHA` during rendering.
A local preview without that variable links to `main`. The homepage checkout command is pinned to that source SHA; other code samples
are not rewritten. Every PR builds the site in CI and retains a `documentation-preview`
artifact, including when only documentation changes.

## Versions and source identity

| Trigger | Published location | Meaning |
| --- | --- | --- |
| Push to `main` | `develop/` | Unreleased development documentation |
| Stable numeric tag `X.Y.Z` | `X.Y.Z/` | Documentation from that exact tag |
| Prerelease tag such as `X.Y.Zrc1` | `X.Y.Zrc1/` | Candidate documentation, not stable |
| Local `mkdocs build` or `serve` | Local output | Unpublished preview |

Each generated page names its channel and links its exact source commit.
`versions.json` records the source SHA and channel for each published version.
Only the highest stable numeric Git tag may move `latest`; prereleases and lower
maintenance releases leave it unchanged. The root initially opens `develop` and
switches to `latest` once stable documentation is published. Old version trees
are preserved rather than rebuilt with new tooling.

Older package releases that predate this workflow are not relabeled with current
content. Their documentation remains in their tagged source. A documentation tag
and successful site deployment do not prove that the corresponding PyPI upload
succeeded: verify the package release separately. **Pushing any tag also triggers
the existing PyPI release workflow. Never push a test tag to exercise docs.**

## How publishing works

`.github/workflows/deploy-doc.yml` runs on main and tag pushes. Its manual action
can recover a deployment for an existing main or tag ref; it does publish. It uses
Python 3.12 and the locked docs group. The workflow:

1. Fetches the existing `gh-pages` archive and builds the selected version locally
   with mike. Mike's temporary staging commits never enter published history.
2. Checks the complete versioned tree's links, anchors, assets, redirects, and
   version manifest before making an archive update.
3. Writes the checked tree to `gh-pages` through GitHub's `createCommitOnBranch`
   API and requires a valid GitHub signature. The expected parent SHA prevents
   overwriting a concurrent update. No local mike commit is pushed.
4. Uploads that same checked tree to GitHub Pages and deploys it through the
   `github-pages` environment. The `documentation-site` artifact retains the
   checked tree for 14 days; permanent version history lives on `gh-pages`.

Unlike spark-on-k8s's legacy branch publishing, this repository uses GitHub's
Pages artifact deployment. This permits signed archive commits without a stored
private signing key and does not rely on a bot branch push triggering another
workflow. Alias pages are regular redirect files, not symlinks.

Deployment runs share a concurrency group. An active run is not cancelled, but
GitHub retains only one pending run: rapid pushes can replace a pending tag run.
Inspect the run list after releases and rerun a displaced tag publication when
needed. Archive compare-and-swap failures stop rather than force-pushing.

## Initial repository setup

This is a one-time maintainer setup, not part of ordinary documentation edits:

- Enable GitHub Pages with **GitHub Actions** as the source. The project inherits
  the account's `hussein.awala.fr` domain; do not add a project `CNAME` file.
- Create an empty `gh-pages` branch with a signed commit and verify its signature
  before pushing it. Do not branch it from main with the package source files.
- Allow the `github-pages` environment to deploy from main and intended version
  tags, keeping any existing maintainer protection rules. The workflow requires
  contents write permission for its archive job and Pages/OIDC permissions for
  its deployment job.

The workflow deliberately fails if the archive or Pages configuration is absent.
It does not silently create unsigned history or change repository settings.
No additional token or signing-key secret is needed: publication uses the workflow's
scoped `GITHUB_TOKEN` and GitHub's commit signature.

## Verify or recover a deployment

Check both jobs of the exact `Deploy documentation` run. Then verify the public
version manifest at `https://hussein.awala.fr/async-batcher/versions.json`, the new
version's home page, channel label, and source link against the workflow SHA.
The `latest/` alias changes only for an eligible stable version.

A successful archive update can precede a failed Pages deployment. In that case,
the public site may still show its previous content. Inspect the failed job and
rerun the existing workflow for the intended ref after fixing the cause. A deployment
job refuses an artifact superseded by a newer archive commit; use the current main
run to publish the complete updated archive in that case. Do not
move release tags or create a new package release to repair documentation. If an
archive API request has an ambiguous outcome, inspect `gh-pages` and its manifest
before retrying. A conflicting or unsigned archive result is a publishing failure,
not permission to force-push or bypass signature verification.

## Shared release notes

[Release notes](https://hussein.awala.fr/async-batcher/release-notes/) are a project-wide
page independent of the selected documentation version. Every version's navigation
links to that permanent URL. Main deployments rebuild it from `release-notes/*.md`;
tag deployments preserve its existing archive bytes, so an older release cannot
replace newer history. It is not an entry in the documentation version selector.

Each file contains metadata followed by the canonical GitHub release body. Published
0.2.0, 0.2.1, and 0.2.2 bodies were copied from GitHub, preserving their wording and
normalizing line endings. Planned releases are labeled **In preparation**. To preview:

```bash
uv run --no-sync python .github/scripts/release_notes.py build --output-dir release-notes-site
uv run --no-sync python .github/scripts/check_docs.py release-notes-site --base-path /async-batcher/release-notes/
```

Before creating an authorized GitHub release, export its reviewed body rather than
maintaining a second copy. For example:

```bash
uv run --no-sync python .github/scripts/release_notes.py extract 0.3.0 --output /tmp/async-batcher-0.3.0-notes.md
```

Use that file with the existing release process's `--notes-file` argument. Extraction
only writes a local file; it does not publish. After package and GitHub publication
are confirmed, update the file's `status`, `date`, and `github_url` to the actual
release in a signed documentation PR. Its main deployment updates the shared page
without rebuilding archived version docs. Preserve historical bodies; release-note
corrections should be deliberate and reviewed.
