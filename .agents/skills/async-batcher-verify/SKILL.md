---
name: async-batcher-verify
description: Select and run change-appropriate tests in async-batcher, check installed API and package contracts, and prepare a PR with exact validation evidence. Use for code, tests, examples, dependencies, CI, or documentation changes in this repository.
---

# Verify a change and prepare its PR

Follow [AGENTS.md](../../../AGENTS.md). Run commands from the repository root. Use
[the test matrix](references/test-matrix.md) for the affected area; do not install every
adapter or launch every service for an unrelated edit.

## Choose the evidence

1. Inspect the diff against the intended base and identify runtime, public API, adapter,
   application recipe, packaging, CI, and documentation impact. Tests moved or changed can
   affect CI selection even when runtime files are unchanged.
2. For a bug, add a focused regression that fails on the previous behavior where practical.
   Exercise observable results, resource ownership, and failure paths. For asyncio races,
   use events/barriers and bounded waits; arbitrary sleeps and throughput assertions are brittle.
3. Run the focused tests during development, then the affected suite before opening the PR.
   Core lifecycle changes need the full core suite. Check Python 3.11 and the newest endpoint
   in `ci_plan.py` when cancellation, task factories, executors, or interpreter semantics matter.
   Let hosted CI supply operating systems or service environments unavailable locally.
4. Include tests for meaningful new failure/partial-success cases and update affected API docs,
   examples, exports, and typing fixtures. A prose-only correction needs link/content checks,
   not a new runtime test that merely checks documentation wording.

## Keep the environment honest

Use `uv sync --locked` with explicit extras/groups, then `uv run --no-sync`. Syncing a different
suite can remove its predecessor's packages. Use separate environments/worktrees for parallel
suites; never race sync commands in one `.venv`.

Before adapter tests run `.github/scripts/check_adapter.py SUITE`; inspect pytest skip reasons.
A skipped optional suite is not a pass for that adapter. Default pytest selects only core and
adapter tests; HTTP, provider recipes, and service integration suites need explicit paths.
Use the checked-in local services and local provider HTTP tests, not paid cloud calls.

After Python edits, run the checked-in pre-commit hooks and inspect any changes they make.
Run installed typing/package checks when those contracts change, using the matrix. Build/install
probes must not accidentally import the source checkout; the existing helper scripts enforce this.
The adapter distribution probe replaces the current editable installation, so run it after runtime
tests or resync before continuing source-based tests.

## Before creating the PR

- Review the final diff and `git diff --check`; include only the intended files.
- Record commands, interpreter/dependency selection, passed/skipped results, and unavailable
  checks. Fix local failures; if a required environment is unavailable, describe the gap in a draft.
- For public behavior changes, ensure the relevant guide and realistic regression agree on
  cancellation, cleanup, bounded memory, per-item correlation, and partial external effects.
- Sign each commit and verify it. Create a focused draft PR unless the user requested otherwise.
  The description should state the concrete problem, resulting behavior, validation, and material
  limitations. Use a structured body or `gh pr create --body-file` to preserve literal text.

## Before an authorized merge

Inspect checks on the **current head SHA**, including the selected adapter/service/typing jobs,
not just the aggregate name. Use `.github/filters.yml` and `ci_plan.py` to identify missing
coverage of changed paths. A scoped PR can legitimately skip unrelated jobs, but it does not prove
that the full matrix ran. A fresh full run is appropriate for release readiness or unresolved
integration risk, not as a mandatory repeat after every small edit.

Required checks must finish successfully unless the user explicitly authorizes a specific waiver.
Rebase conflicts with signed commits, inspect the resolved diff, and wait for current-head checks
before merging. Do not reuse success from an old head. When force-pushing an owned rebased branch,
use an explicit lease matching the previously observed remote head.

CI combines every planned coverage artifact and enforces 95% combined statement-and-branch
coverage for its selected source scope. Missing artifacts are failures. A core-only local report
must not be presented as full-package coverage; inspect uncovered behavior rather than weakening
the threshold. Give the user the PR link, validation outcome, and any remaining limitation.
