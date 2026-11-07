# Async Batcher repository instructions

`async-batcher` is the distribution; `async_batcher` is the import package. The core is a
Python 3.11+ asyncio library with no runtime dependencies. Backend clients and ML libraries
belong in optional extras, not core imports or the default test environment.

## Working agreements

- Preserve unrelated edits. Use a suitable isolated worktree when the main checkout contains
  user changes. Keep local notes, environments, credentials, and generated reports out of commits.
- Sign every commit, including commits rewritten by a rebase (`git commit -S`,
  `git rebase --gpg-sign`). Verify signatures before pushing; do not disable signing to proceed.
- Keep PRs focused. Run the relevant local checks before creating a PR; a draft may describe
  unavailable checks honestly. Merge only when authorized and the current PR head has passing
  required CI. Resolve conflicts, sign the rewritten commits, and recheck the updated head.
- A one-off authorization to waive a check is not a permanent repository exemption. Do not
  change security settings or coverage thresholds to conceal a failing check.
- Release preparation and release publication are separate tasks. **Pushing any tag triggers
  the PyPI release workflow.** Do not push a test tag or publish a release during ordinary PR work.

## Where work belongs

| Location | Purpose |
| --- | --- |
| `async_batcher/` | Core lifecycle/composition modules and optional adapter subpackages |
| `tests/core`, `tests/adapters` | Default pytest suites; adapter dependencies must be installed |
| `tests/integration`, `tests/examples`, `tests/provider_recipes` | Explicitly selected service, HTTP, and provider recipe suites |
| `tests/typing` | Installed-package positive and negative typing consumers |
| `examples/recipes`, `docs/`, `README.md` | Runnable patterns, contracts, and onboarding |
| `.github/` | Path selection, interpreter matrix, build/type/security helpers, and publishing |
| `benchmarks/` | Reproducible measurements and raw results; no timing pass/fail thresholds |

Use these repo-local skills when the task needs their workflow:

- [async-batcher-navigate](.agents/skills/async-batcher-navigate/SKILL.md): locate the implementation,
  ownership boundary, tests, and documentation for a feature or bug.
- [async-batcher-verify](.agents/skills/async-batcher-verify/SKILL.md): choose tests for a change,
  interpret skips/coverage, and prepare a PR with accurate validation evidence.

Read only the relevant guides. [CONTRIBUTING.md](CONTRIBUTING.md) and the current workflow/configuration
files are the executable-command sources of truth; update guidance when those commands change.

## Environment and validation

Use uv and the committed lockfile. A basic core environment is:

```bash
uv sync --locked --python 3.11 --group lint
uv run --no-sync python -m pytest tests/core
uv run --no-sync pre-commit run --all-files
```

`uv sync` removes unselected dependencies; `uv run --no-sync` preserves the selected environment.
Do not run competing syncs in the same `.venv`. Default pytest does not run HTTP/provider recipes
or live services. Missing optional dependencies can produce skips, not adapter validation.
The verification skill maps changes to explicit suites and prerequisites.

## Contracts to preserve

- Caller cancellation, accepted work, backend side effects, and shutdown are different events.
  Keep ownership until real work settles; do not claim cancellation prevents an external write.
- Keep clients/executors alive through batcher cleanup. Core lifecycle/composition changes need
  cancellation, partial-batch, bounded-admission, failure, and shutdown/restart coverage as applicable.
- Keep output-to-input correlation and per-item errors intact across batching and adapters.
  Use event gates for concurrency regressions instead of machine-speed timing assertions.
- Update public exports, shared option typing, installed typing consumers, and affected docs when
  changing public contracts. Document source-checkout features separately from published releases.
