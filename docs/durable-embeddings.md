# Save embeddings before delivering vectors

The [durable handoff recipe](../examples/recipes/durable_embeddings.py) separates two operations:

1. Read an immutable JSONL file, obtain embeddings, validate each entire model response, and commit
   the vectors to a local SQLite spool.
2. Read pending vectors from that spool, upsert them to the destination, and mark acknowledged rows.

Retrying stage 1 reuses embeddings already committed for the same job. Retrying stage 2 reads saved
vectors and never calls a model. This is an application recipe, not a persistent queue in AsyncBatcher.
The two stages run sequentially, so disk must hold the full set of embeddings before delivery starts.

## Run without credentials

From the current [source checkout](../README.md#install):

```bash
uv sync --locked --no-default-groups --python 3.12
uv run --no-sync python -m examples.recipes.durable_embeddings \
  --input /tmp/documents.jsonl --spool /tmp/embedding-spool.sqlite \
  --sink /tmp/delivered-vectors.sqlite --job-id catalog-v1
```

Input lines contain exactly nonempty string `id` and string `text` fields. IDs must be unique within
the file. This demo uses deterministic text features, **not semantic embeddings**, and a SQLite
vector sink without similarity search. Run the same command again after interruption. It prints
counts of persisted embeddings, acknowledged vectors, and vectors still pending. A completed rerun
scans the source for identity/reuse checks but sends neither new model calls nor completed upserts.

## Bind identity before paying for work

`Spool(database, job_id, Identity(...))` requires explicit provider, model, revision, preprocessing,
input type, vector dimension, and destination identity. Use a destination identifier that includes
index/namespace/tenant as appropriate; never put credentials in it. Initialization binds the job to
that complete configuration plus the immutable file's SHA-256 and byte length. A configuration or
source mismatch fails before any embedding call. An identical source copy can use a different path.

Each spool row also stores its source byte offset, document ID, text digest, validated vector, and
acknowledgement state. There is no cross-job cache, eviction policy, or raw-text copy in the spool.
Vectors, IDs, and configuration can still be sensitive. Protect the database and its backups.

These identities are application declarations. They cannot prove that a remote client actually uses
the named model/revision, preprocessing, tenant, or destination. Configure those clients consistently
and use genuinely pinned model behavior when reproducibility matters. If a provider changes a model
behind an unchanged name, this recipe cannot detect that change. Duplicate IDs within a dispatch are
rejected before calling the model; a later duplicate can be discovered after earlier embedding costs.

## Use application-owned providers

`stage_file(path, spool, model, executor=pool)` borrows a model with async
`embed(texts) -> list[list[float]]`. `deliver_pending(spool, sink, executor=pool)` borrows a sink with
async `upsert(records) -> list[str]`, where each `VectorRecord` has an ID and saved vector. The sink
must return every input ID in order and must converge when the **same ID and vector** are replayed.
Client/model contexts must outlive these helpers and their accepted work. Keep the caller-owned
thread pool alive and join it after the event loop, as the CLI does.

For a Cohere/Pinecone application, adapt the existing [provider recipe](cohere-pinecone.md): pass texts
to `CohereEmbedder`, extract its validated vector values, and upsert `VectorRecord` values to the
already compatible Pinecone index/namespace. Validate the index dimension before starting stage 1,
as that recipe does. Choose count/byte/token limits for those actual providers; the generic spool
example does not impose their quotas or wire-size constraints. Local tests exercise the native
Cohere HTTP and Pinecone SDK boundary, including a lost acknowledgement after a committed write.

Run only one application worker per job, and finish stage 1 before stage 2. There is no distributed
lease, concurrent-producer protocol, background worker, or automatic retry loop. Different job IDs
have separate local rows; destinations can still conflict if those jobs use the same remote IDs.

## Recovery boundaries

- Crash after the embedding transaction commits: stage 1 finds the saved vectors and skips those calls.
- Crash/lost response before embedding persistence: that paid call may need repeating. The spool
  cannot recover an embedding response it never durably received.
- Sink failure, lost response, or crash before the local acknowledgement update: vectors remain pending.
  Stage 2 may replay a write the sink already committed. Stable upserts converge; exactly-once delivery
  and a transaction spanning local/remote systems are not provided.
- Cancellation: map intake closes and accepted work drains. It can commit more vectors/acknowledgements
  than the caller observed. Keep clients open through drain, and join worker threads.
- Invalid model output: wrong counts, dimensions, nonnumeric or nonfinite values fail the entire
  dispatch before any of that response is saved. Earlier committed embeddings remain usable.

The source is hashed in one-MiB chunks and read in chunks of at most 32 lines, each at most 64 KiB.
Both stages use one active batch, a default batch size of 32, and a map window of 64. Delivery reads
pages of at most that window; one page can be retained in addition to the map's pending items.
Vectors consume dimension-dependent memory, and disk use grows with the complete job. Monitor
available disk and define your own retention policy. No automatic deletion occurs.

SQLite uses WAL and `synchronous=FULL`; storage must honor synchronization. Keep the database/WAL
files together and use SQLite-aware backups on a supported local filesystem. Source checks detect
ordinary mutation, not adversarial filesystem races. `acknowledged_vectors` records the sink's
acknowledgement contract, not immediate search visibility or independent proof of remote durability.

```bash
uv sync --locked
uv run --no-sync python -m pytest tests/core/test_durable_embeddings_recipe.py -q
# Optional native provider checks, using local HTTP servers and dummy credentials:
uv sync --locked --group example-test --extra pinecone
uv run --no-sync python -m pytest tests/provider_recipes/test_durable_embeddings.py -q
```
