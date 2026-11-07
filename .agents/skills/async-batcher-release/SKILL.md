---
name: async-batcher-release
description: Prepare or publish an async-batcher release, including versioned build validation, release notes, tag-triggered PyPI publication, and an explicitly requested announcement. Use for release work in this repository, not ordinary dependency updates or pull requests.
---

# Async Batcher release

Use this skill from the repository root. The repository is `hussein-awala/async-batcher`,
the distribution is `async-batcher`, and the import package is `async_batcher`.
Read applicable `AGENTS.md` instructions and `CONTRIBUTING.md` for the current verification
commands. Read [history.md](references/history.md) when choosing the comparison baseline or
writing notes; it distinguishes observed releases from proposed practice.

## Scope and authorization

Default to **prepare**: inspect history and the target, validate a local versioned build, and
produce release notes plus the exact publication plan. Do not infer permission to publish
from a request to prepare notes, review readiness, or update these instructions.

Publishing needs the user's explicit release authorization, chosen version, and target
commit or branch. Resolve an authorized branch to an exact commit and report it. Reuse
these decisions if already provided; do not ask again. If a required decision is missing,
finish independent preparation before asking for that decision. Discussion posting needs
explicit authorization, even when release publication is authorized.

## Repository-specific behavior

- **Pushing any tag triggers `.github/workflows/release.yml` and can publish to PyPI.**
  This includes prerelease and non-version tags. Do not push a throwaway/test tag, use
  `git push --tags`, or let a release tool create an unreviewed tag.
- The checkout currently declares `0.0.1`. The workflow runs
  `uv version --no-sync "$RELEASE_VERSION"` in its checkout, updating project and lock
  metadata before building. Do not choose a new version by incrementing that placeholder,
  or add a permanent bump commit solely because the placeholder differs from PyPI.
- The workflow uses the `pypi` environment and OIDC trusted publishing. It builds a wheel
  and sdist and uploads them automatically; it does **not** run the full test suite or
  wait for the ordinary CI workflow. Inspect the environment's actual protection rules;
  the environment name alone does not imply a human approval gate.
- GitHub release notes and an Announcements discussion are separate from that workflow.
  Publishing the tag does not create either. Creating a GitHub release for a missing tag
  can create the tag and start PyPI publication; use an existing verified tag.
- Source installation and current documentation can describe unreleased features. Check
  the selected tag and PyPI metadata before promising support or APIs in a published release.

## Prepare a concrete release

1. Refresh the chosen ref, remote tags, GitHub releases, and PyPI versions. Resolve the
   target SHA and previous comparable release. Check normalized version availability on
   PyPI as well as the literal tag name; the historical `0.1.0-rc.1` tag became `0.1.0rc1`.
   Respect the user's version; flag a conflict or compatibility concern without silently
   selecting another version. Stable tags historically use `X.Y.Z` without a `v` prefix.
2. Compare the previous release with that exact target. Write notes about resulting
   behavior, supported Python versions, API/migration changes, new adapters, fixes, and
   security changes that the diff actually supports. Include relevant PR links and a full
   compare link. Use the categorized `0.2.0` notes for a feature release or the compact
   `0.2.1`/`0.2.2` pattern for a patch. Mention contributors when supported by the history.
   Do not claim old performance figures or incomplete service tests describe the release.
3. Check CI and the locked advisory audit for the target SHA, including required artifact,
   installed-distribution, typing, and service checks. Follow current repository verification
   instructions; a scoped PR run is not evidence of a complete release sweep. If evidence is
   missing, run or request the authorized full checks on the target. Keep unresolved checks
   visible, and follow any user-approved exception only within its stated scope.
4. Reproduce the version override in an isolated temporary copy of the target using the uv
   version pinned by `.github/actions/setup-project/action.yml`. Run `uv lock --check`, build
   wheel and sdist, and run `.github/scripts/check_distributions.py` against that versioned
   copy. Check archive names, package version, `Requires-Python`, extras, `py.typed`, and
   absence of undeclared core runtime dependencies. Keep dependency versions unchanged;
   investigate unexpected lock changes instead of including an opportunistic update.
5. Present the exact version, target SHA, previous tag, notes, test/build evidence, and any
   remaining blocker. Make publication the final action when it still needs authorization.
   Do not modify main's version or retain preview artifacts in the repository by default.

## Publish and verify

Only with the required authorization, follow [publication.md](references/publication.md).
Create and verify a signed tag on the exact tested commit; never move a published tag or
force a tag push. Push only that tag, monitor both release jobs, and verify the wheel and
sdist on PyPI before announcing success. Stop on a failed or ambiguous publication and
inspect uploaded files before considering a bounded retry; do not create a new version or
replace credentials to work around a failure without authorization.

Create the GitHub release with reviewed notes and `--verify-tag`, preserving stable versus
prerelease intent. Add an Announcements discussion only when requested. Finish with the
version, source commit, release/PyPI/workflow URLs, and the result of each authorized action.
State any incomplete step directly.
