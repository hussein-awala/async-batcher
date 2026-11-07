# Verified release history

Observed on 2026-11-07. Recheck live state before a release; this file records evidence,
not a permanent statement about the latest version. The source baseline for this review
was commit `1fadc539e77e74886e022c094f83dfbcf75092cf`.

## Published versions and source tags

| Git tag | Tagged commit | First PyPI upload (UTC) | Python requirement | GitHub release |
| --- | --- | --- | --- | --- |
| `0.1.0-rc.1` | `f8f12eba2ddad356c5a2bce0df56c9220c70e861` | 2024-02-19 23:22 | `>=3.8,<4.0` | None returned by the releases API |
| `0.1.0` | `07469e204cc7944acd489c4899f41c0e1c9ad129` | 2024-02-20 12:53 | `>=3.8,<4.0` | None returned by the releases API |
| `0.2.0` | `00f47e798c61834c2c027af59715b31123e9e6ef` | 2024-02-29 23:21 | `>=3.10,<4.0` | [Async Batcher 0.2.0](https://github.com/hussein-awala/async-batcher/releases/tag/0.2.0) |
| `0.2.1` | `5b43297ca6fc3069b6606307603ac48f2fbe6d9c` | 2024-09-02 20:57 | `>=3.10,<4.0` | [Async Batcher 0.2.1](https://github.com/hussein-awala/async-batcher/releases/tag/0.2.1) |
| `0.2.2` | `aaed863223ed9605e36ab30d2dfade524662838b` | 2025-02-17 21:45 | `>=3.10,<4.0` | [Async Batcher 0.2.2](https://github.com/hussein-awala/async-batcher/releases/tag/0.2.2) |

Sources: [remote tags](https://api.github.com/repos/hussein-awala/async-batcher/tags?per_page=100),
[GitHub releases](https://api.github.com/repos/hussein-awala/async-batcher/releases?per_page=100),
and [PyPI JSON](https://pypi.org/pypi/async-batcher/json). Each version has a wheel and sdist;
none was yanked at review time. PyPI normalizes `0.1.0-rc.1` to `0.1.0rc1`.
These old refs are lightweight tags, so their commit timestamps do not establish tag creation
or publication times. Prefer the upload/release timestamps for those events.

The selected tagged commits are normal source changes, not dedicated version-bump commits:

- `0.1.0-rc.1`: [add the original publication workflow](https://github.com/hussein-awala/async-batcher/commit/f8f12eba2ddad356c5a2bce0df56c9220c70e861).
- `0.1.0`: [add Keras and sklearn extras](https://github.com/hussein-awala/async-batcher/commit/07469e204cc7944acd489c4899f41c0e1c9ad129).
- `0.2.0`: [PR #23, bounded buffered inputs](https://github.com/hussein-awala/async-batcher/pull/23).
- `0.2.1`: [PR #35, AWS dependency security updates](https://github.com/hussein-awala/async-batcher/pull/35).
- `0.2.2`: [PR #39, README details](https://github.com/hussein-awala/async-batcher/pull/39).

This is evidence of tagging ordinary source commits, not a rule that a release must end with
one particular kind of PR. Signed tags are the current release instruction, not a claim
that these historical lightweight tags were signed.

## Notes and announcements

All three GitHub releases are named `Async Batcher VERSION`, have a full comparison link,
and link individual changes to PRs. Their release assets lists are empty; package files
were published to PyPI rather than uploaded as GitHub release assets.

`0.2.0` groups notes into significant changes, fixes, features, improvements, and contributors.
It calls out dropping Python 3.8/3.9 and introduces concurrency, executor selection, SQLAlchemy,
and bounded buffering. `0.2.1` and `0.2.2` use a compact “What's Changed” list; the former
includes several dependency security fixes. These are useful note patterns, not an exhaustive
required template or a documented semantic-version policy.

[Discussion #29](https://github.com/hussein-awala/async-batcher/discussions/29) is the only
release announcement found. It is in **Announcements**, repeats the `0.2.0` notes, and says
it was created from that GitHub release. The other discussion is the
[welcome announcement #20](https://github.com/hussein-awala/async-batcher/discussions/20).
No `0.2.1`/`0.2.2` announcement was returned. Do not infer that every release must create
one or that permission to publish includes permission to post a discussion.

## Current pipeline and differences from published releases

- [Current reviewed workflow](https://github.com/hussein-awala/async-batcher/blob/1fadc539e77e74886e022c094f83dfbcf75092cf/.github/workflows/release.yml)
  triggers on every pushed tag, uses Python 3.12, installs only the project, sets its version
  from the tag, checks the lock, builds wheel/sdist, and passes `release-dists` to the PyPI
  publishing job. Artifacts are retained for seven days.
- [Setup action](https://github.com/hussein-awala/async-batcher/blob/1fadc539e77e74886e022c094f83dfbcf75092cf/.github/actions/setup-project/action.yml)
  pins uv 0.13.7. [PR #71](https://github.com/hussein-awala/async-batcher/pull/71) removed the
  optional-runtime install requirement from release builds;
  [PR #116](https://github.com/hussein-awala/async-batcher/pull/116) replaced Poetry with uv.
- The PyPI job has `id-token: write` and uses environment `pypi` with the PyPA publish action,
  matching [trusted publishing](https://docs.pypi.org/trusted-publishers/using-a-publisher/).
  The public workflow is not proof that the current server-side PyPI trust configuration
  still matches. The [GitHub environment API](https://api.github.com/repos/hussein-awala/async-batcher/environments/pypi)
  returned empty protection rules and no branch policy at review time; there was no visible
  manual environment approval gate. Recheck configuration when investigating a release failure.
- There is no CI dependency, GitHub release creation, or discussion creation in that workflow.
  The historical release-run API returned no retained runs at review time. That is not
  evidence that prior releases failed; the uploaded PyPI files independently establish publication.
- Both [tag 0.2.2 metadata](https://github.com/hussein-awala/async-batcher/blob/0.2.2/pyproject.toml)
  and [reviewed main metadata](https://github.com/hussein-awala/async-batcher/blob/1fadc539e77e74886e022c094f83dfbcf75092cf/pyproject.toml)
  declare `0.0.1`. The workflow override explains published version `0.2.2`; the source
  value is not a release counter. Main requires Python 3.11 and contains extensive new
  behavior not present in `0.2.2`. Published metadata links documentation to `main`, so use
  the release tag's files when explaining an installed historical version.

There is no observed release PR template, automatic changelog generator configuration, or
committed per-release bump convention in this history. Do not invent one as a prerequisite.
A future release must explicitly describe the Python floor and other compatibility changes
actually included since its previous tag.
