---
name: async-batcher-navigate
description: Locate implementation, ownership boundaries, tests, and documentation in async-batcher before investigating a bug or adding a feature. Use when the affected repository area is unclear.
---

# Navigate Async Batcher

Start at the repository root and follow its [AGENTS.md](../../../AGENTS.md). Locate the
smallest set of implementation files and public contracts relevant to the request.

## Map the request to its owner

| Behavior | Implementation | Contract / first tests |
| --- | --- | --- |
| Admission, cost/count collection, dispatch, cancellation, flush, stop/restart | `async_batcher/batcher.py` | `docs/api.md`, `docs/cost-aware-batching.md`; matching `tests/core/test_*` regressions |
| Wrapping an async or sync bulk callable; context-manager ownership | `async_batcher/callable.py` | `docs/functions.md`; `test_callable_batcher.py`, executor/context tests |
| Bounded iteration, ordering, indexed item outcomes | `async_batcher/map.py` | `docs/bulk-processing.md`; `test_bounded_map.py`, `test_map_outcomes.py` |
| Child creation/eviction, compatible keys, aggregate admission | `async_batcher/grouped.py` | `docs/grouped-batching.md`; grouped batcher/streams/pending-cap tests |
| Deduplicating concurrent requests without caching completed results | `async_batcher/coalescing.py` | `docs/coalescing.md`; `test_coalescing.py` |
| Payload-free events, immutable statistics, lifecycle/pressure gauges | `async_batcher/observability.py` and producers in `batcher.py` | `docs/observability.md`; observability/pressure tests |
| Public imports and constructor option types | `async_batcher/__init__.py`, `async_batcher/_options.py`, `async_batcher/py.typed` | `docs/typing.md`, `tests/typing`, installed-wheel checking scripts |
| Backend-specific request construction, response mapping, retry ownership | `async_batcher/aws`, `kafka`, `mongodb`, `pinecone`, `sqlalchemy`, `scylladb`, `ml` | Corresponding `docs/` guide, `tests/adapters/<suite>`, then service tests if applicable |
| Application lifecycle or multi-stage workflow | `examples/recipes` | `docs/cookbook.md` and linked guide; matching core, HTTP, or provider recipe tests |
| Test selection, supported interpreter combinations, package installation | `.github/filters.yml`, `.github/scripts/ci_plan.py`, `.github/workflows/ci.yml`, `pyproject.toml` | CI helper tests and `CONTRIBUTING.md` |

Paths in the table are repository-relative; filenames without a directory refer to
`tests/core/`. The distribution name is `async-batcher`; the repository is
`hussein-awala/async-batcher`, even if the local checkout directory is named `asyncio-batcher`.

## Trace ownership before changing behavior

A normal submission reaches `AsyncBatcher.process`, is admitted, is collected into a batch,
and runs through an async processor or thread executor before its result is routed back.
`map` bounds producers above this path; grouped batching owns children; coalescing borrows
an existing batcher. Adapters implement backend work below the core lifecycle.

Inspect the relevant guide when a change crosses one of these boundaries:

- An accepted request may outlive its cancelled caller. Flush tracks accepted work, not merely
  unfinished caller futures. Queue capacity, active batches, admission waiters, and grouped
  aggregate capacity describe different sets of work.
- `AsyncBatcher` can restart on its owning loop after cleanup. A grouped manager or a coalescing
  wrapper closes permanently; coalescing does not take ownership of the borrowed batcher.
- Backend errors may follow partial external writes. Core result validation cannot roll back
  those effects. Retry and replay semantics belong to the adapter or documented recipe.
- Optional SDK imports must stay out of the core package import path. Check public compatibility
  at the installed-wheel boundary as well as in the source tree.

Use `rg` to follow callers and existing regression tests instead of reading every guide.
Report the likely code owner, relevant tests/docs, and any unresolved contract question.
Continue the requested implementation when scope is clear. For execution and PR checks, use
[async-batcher-verify](../async-batcher-verify/SKILL.md).
