# Resume a JSONL import after a crash

[This runnable recipe](../examples/recipes/resumable_sqlite.py) extends the
[basic SQLite writer](cookbook.md#import-a-file-or-async-stream-into-sqlite) with a durable checkpoint.
It requires only the core package and Python's standard library. It does not change the library's
batching API or turn an entire import into one transaction.

Each JSONL line must contain exactly a nonempty string `id` and a string `text`:

```json
{"id": "document-1", "text": "First document"}
{"id": "document-2", "text": "été 🌎"}
```

From a [source checkout](../README.md#install), run:

```bash
uv sync --locked --no-default-groups --python 3.12
uv run --no-sync python -m examples.recipes.resumable_sqlite \
  --input /tmp/documents.jsonl --database /tmp/import.sqlite --job-id catalog-v1
```

After interruption, run the same command with the same job ID. A completed import reads no more
records, although it still hashes the file to verify its identity. The report includes the prior
processed count, this run's acknowledged results, total committed records, byte checkpoint,
elapsed import time, actual batch-size histogram, and batcher statistics. Elapsed time excludes
the initial whole-file identity scan and schema/checkpoint initialization; it is not a speedup claim.

## What is atomic

One SQLite transaction performs both operations:

1. Upsert a contiguous batch of records into `import_documents`, keyed by job ID and document ID.
2. Advance that job's byte offset and processed-record count in `import_jobs`.

A row failure rolls back the entire batch, including its checkpoint. Earlier batches remain
committed. If the process dies after commit but before it receives the batch results, the next
run resumes after those records. Tests exercise this window with a real subprocess exit.
Duplicate IDs overwrite earlier values in file order; the processed count counts input records,
not distinct document IDs. Different job IDs have separate document rows.

The writer uses one batch at a time and `BEGIN IMMEDIATE`. Every batch checks that its first offset
matches the saved checkpoint and that subsequent offsets are contiguous. A stale concurrent importer
fails with `CheckpointConflict`; it cannot advance the job past records it did not write. Run one
importer per job. Other SQLite writers can still contend for the database's write lock; the connection
uses a five-second busy timeout, not an end-to-end import deadline.

Every worker call owns and closes its SQLite connection. Connections explicitly enable WAL mode,
`synchronous=FULL`, and foreign keys. WAL plus FULL synchronizes each commit; durability still depends
on the filesystem and storage honoring SQLite's synchronization requests. Keep the database and its
WAL files together, use a supported local filesystem, and use SQLite-aware backup procedures.
See SQLite's [synchronization documentation](https://www.sqlite.org/pragma.html#pragma_synchronous)
and [WAL restrictions](https://www.sqlite.org/wal.html). This checkpoint covers only these SQLite
rows, not external API calls or other databases.

## Immutable input and bounded memory

The first run binds the job ID to the file's SHA-256 digest and byte length. Every restart hashes
the full source again in one-MiB chunks; a changed file is rejected even when its size is unchanged.
An identical copy can resume the same job from a different path. For new content, choose a new job
ID deliberately. To recover a failed existing job, restore the original file rather than edit out
the bad line and reuse its checkpoint.

Keep the source immutable throughout the run. The recipe checks file identity, size, and modification
timestamps before and after reads and before commits to detect ordinary replacement or mutation.
These checks are not a filesystem snapshot or protection against an adversary deliberately racing
file changes. For a live producer, first publish a completed immutable file or use a transactional
message source instead.

Record validation errors identify the filename and zero-based byte offset of the failing line.
Their messages and normal traceback chains omit record contents and decoder exception details.
Paths themselves can be sensitive; apply your application logging policy.

Offsets count UTF-8 **bytes**, including line endings, so multibyte text and a final line without a
newline resume correctly. Blank lines, invalid UTF-8/JSON, extra fields, and invalid IDs are rejected.
The reader buffers at most 32 records, each limited to 64 KiB including its newline, beyond the
map's default 64 pending items. Parsing a later invalid line may discard that unsubmitted chunk;
the saved checkpoint still identifies the last committed prefix. Reading ahead never advances it.

The caller owns a shared two-thread executor for hashing, reading, and writing. Cancellation closes
the map and gracefully drains already accepted writes; it can therefore leave more committed rows
than the caller observed. An asyncio cancellation does not kill a Python worker thread. The CLI
joins the pool after the event loop exits, including any cancelled read or hash operation. For a hard
deployment deadline, close intake and apply a supervisor policy as described in the
[operational guide](operations.md); do not assume forced batcher shutdown interrupts SQLite I/O.
