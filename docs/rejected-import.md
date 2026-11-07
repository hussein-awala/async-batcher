# Import valid records and retain explicit rejections

The [rejected-record recipe](../examples/recipes/rejected_import.py) imports an immutable JSONL file
into SQLite, while recording invalid input as source locations and fixed reason codes. The original
[streaming](cookbook.md) and [resumable](resumable-import.md) importers remain fail-fast.

From the current [source checkout](../README.md#install):

```bash
uv sync --locked --no-default-groups --python 3.12
uv run --no-sync python -m examples.recipes.rejected_import \
  --input /tmp/documents.jsonl --database /tmp/reviewed-import.sqlite --run-id review-1
```

Valid lines contain exactly a nonempty string `id` and a string `text`. Invalid UTF-8/JSON, invalid
fields, and lines above 64 KiB become one rejection each. An oversized line is drained in bounded
pieces through its real newline; its suffix is never treated as another record. Offsets are zero-based
UTF-8 byte positions, including line endings. A final line need not end in a newline.

A rejection stores `run_id`, starting/ending byte offsets, and one of `invalid_utf8_or_json`,
`invalid_document_fields`, or `line_too_large`. It does not store the rejected payload or decoder
exception. The run stores the source path and SHA-256 digest; valid documents retain their text.
Keep the original source if rejected records need review, and protect both source and database.

The reader deliberately yields success/rejection envelopes instead of raising for known input errors.
`map_outcomes()` alone would not do this: source iteration errors still terminate that iterator.
Filesystem errors, changed source contents, and SQLite failures **still stop this recipe**. They are
not classified as bad records and are never silently skipped or retried.

## Bounds and transaction ownership

The reader buffers at most 32 lines of 64 KiB each. Rejected oversized lines retain only metadata.
A bounded map retains at most 64 submitted/unconsumed rows by default, and the writer uses one
32-row batch at a time. No list of all successes or rejected records is accumulated in memory.
The initial SHA-256 scan uses one-MiB chunks. These bounds exclude SQLite's own caches and Python
object overhead; use application-specific sizes before increasing them.

Each transaction atomically stores its audit rows and valid document upserts. Duplicate IDs within
one run overwrite earlier values in source order. The output counts input records, not unique IDs.
A database failure rolls back its entire batch while earlier transactions remain committed.

Inspect rejections with an application-owned SQLite client:

```sql
SELECT r.source_path, r.sha256, x.start, x.end, x.rejection
FROM rejected_import_rows AS x JOIN rejected_import_runs AS r USING (run_id)
WHERE x.run_id = 'review-1' AND x.rejection IS NOT NULL ORDER BY x.start;
```

Use a fresh run ID for every invocation; reuse fails even if the earlier run was interrupted. Rows
from different runs are isolated. This example does not implement resume, automatic repair, or a
transaction across the entire file. For atomic resume of valid immutable input, use the separate
resumable recipe; do not edit a failing source and reuse its checkpoint.

Cancellation closes the map and drains accepted writes, so more rows may commit than the caller
observed. The caller owns the shared thread pool and must join it before releasing resources; the
CLI does that after the event loop finishes. Connections use WAL and `synchronous=FULL`; durability
still depends on storage honoring synchronization. Keep SQLite database/WAL files together and
use a supported local filesystem. Source-change checks detect ordinary mutation, not adversarial races.

```bash
uv sync --locked
uv run --no-sync python -m pytest tests/core/test_rejected_import_recipe.py -q
```
