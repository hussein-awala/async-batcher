# Cohere embeddings into an existing Pinecone index

[This source-checkout recipe](../examples/recipes/cohere_pinecone.py) uses Cohere's real Embed v2
REST endpoint through HTTPX2, then the native Pinecone `AsyncIndex` and
[`AsyncPineconeUpsertBatcher`](pinecone.md). It performs paid provider calls when you run its CLI.
The repository tests use temporary local HTTP servers and dummy credentials; they do not create
indexes, download models, contact cloud services, or require your API keys.

For a credential-free demonstration first, use the [deterministic SQLite pipeline](cookbook.md).
That demonstration is explicitly not a semantic embedding model. This recipe uses the model you
select from Cohere; it does not include a guessed model or dimension default.

## Prepare compatible resources

You must already have:

- A Cohere API key and an Embed v4-or-newer model that supports your chosen output dimension.
- An existing Pinecone **vector index** with that dimension, suitable similarity metric, and its
  exact HTTPS data-plane host. This example uses the Vectors API, not the newer Documents API or
  Pinecone integrated embedding. It does not create or reconfigure an index.
- A namespace reserved for this tenant, embedding model/revision, and preprocessing policy.
  Equal dimensions alone do not make different models compatible. Query embeddings must use the
  corresponding model and query input type in your separate search application.
- JSONL records with unique, stable IDs and text. Each line has exactly `id` and `text` string fields.

The pipeline calls `describe_index_stats()` and compares the dimension **before** consuming input
or sending embedding requests. This catches dimension mismatches, not model, metric, tenant,
authorization, or preprocessing mistakes. Creating a previously unused namespace is an upsert
side effect; the recipe does not claim the namespace already exists. Its host and namespace are
explicit configuration, never values taken from an untrusted document.

Namespaces and IDs follow Pinecone's [identifier limits](https://docs.pinecone.io/reference/api/database-limits/identifier-limits):
at most 512 ASCII characters without NUL. This recipe additionally requires nonblank namespaces
and nonempty IDs. Pinecone treats `__default__` as the default namespace; choose a deliberate
application namespace if isolation matters.

## Install and run

From the repository root:

```bash
uv sync --locked --no-default-groups --group example-test --extra pinecone --python 3.12
```

`example-test` supplies the already locked HTTPX2 dependency; it also includes the optional HTTP
recipe tools. No Cohere SDK or additional dependency group is needed. Set `COHERE_API_KEY` and
`PINECONE_API_KEY` through your normal environment/secret manager, then supply your actual configuration:

```bash
uv run --no-sync python -m examples.recipes.cohere_pinecone \
  --input /tmp/documents.jsonl \
  --host "https://YOUR-EXISTING-VECTOR-INDEX-HOST" \
  --namespace "tenant-model-revision" \
  --model "YOUR-COHERE-EMBED-V4-OR-NEWER-MODEL" \
  --dimension YOUR-SUPPORTED-DIMENSION
```

Replace every uppercase placeholder; the dimension must be an integer. The CLI reads keys from
the environment and does not print them. It closes both clients after its batchers drain. The
`index_documents()` function instead borrows application-owned clients and leaves them open.

## Provider contracts and limits

The [Cohere request](https://docs.cohere.com/v2/reference/embed) uses `texts`,
`input_type="search_document"`, `embedding_types=["float"]`, your `output_dimension`, and
`truncate="NONE"`. Cohere permits at most 96 texts per call; the recipe defaults to 32. Setting
`NONE` makes overlong model inputs fail rather than silently discard text. The recipe's separate
64 KiB UTF-8 limit per document is an application memory policy, **not** a token-count estimate.

The response must contain exactly one finite numeric vector per input, with the configured
dimension. Every row is validated before any vector from that response is returned to storage.
Malformed output fails that entire embedding dispatch. Document IDs stay attached to vectors;
the write stage sends only IDs and values, without raw text metadata.

Two bounded maps each retain at most 64 submitted-but-unconsumed items by default. The JSONL
reader adds one chunk of up to 32 lines, each at most 64 KiB. Embedding batches also have a
256 KiB **raw UTF-8 text** cost budget. JSON escaping and protocol overhead can make the actual
request larger; this is not a total memory or wire-size cap. Each stage uses one concurrent call.
These defaults trade throughput for predictable pressure; measure provider latency and quotas
before increasing pending work or concurrency.

Pinecone's [vector upsert limits](https://docs.pinecone.io/guides/legacy/vectors-api/upsert-vectors)
include 1,000 records and a 2 MB request maximum. This recipe uses the smaller configured batch
count plus a conservative 1,800,000-byte estimate: 32 bytes per finite float64 value, an escaped
ID, per-record overhead, and reserved namespace/request overhead. Tests inspect actual native SDK
request bodies, including extreme finite floats and escaped identifiers. This estimate applies
only to the recipe's dense vectors without metadata or sparse fields; recalculate it if extending
the payload. Count limits alone are insufficient for high-dimensional vectors.

## Failures, replay, and ownership

Use unique source IDs. The adapter rejects duplicates within one upsert dispatch, which can happen
after embedding costs were incurred. Duplicates in different dispatches overwrite existing values;
there is no global deduplication set. The pipeline keeps input order and storage concurrency one,
but other processes writing the same IDs can still race.

This example adds no retry loop. Cohere HTTP errors, including throttling, propagate from HTTPX2.
The native Pinecone REST client has its own [built-in retries](https://sdk.pinecone.io/python/guides/retries.html),
including retries of upserts. The CLI's 30-second client timeouts do not form a whole-import deadline;
Pinecone can make several attempts and back off. For separate response, backend, and deployment
deadlines, see the [operational guide](operations.md). For application-owned shared quotas and
finite throttling retries, compose the [provider policy recipe](provider-policy.md), choosing one
retry owner per operation and counting actual attempts.

A successful result means Pinecone acknowledged the vector, not that a subsequent query can
already see it. Errors, cancellation, or a lost response can leave earlier writes committed and
embedding requests billed. The map contexts close their sources and the batcher contexts drain
accepted work before the CLI closes its clients. There is no transaction across Cohere and
Pinecone, rollback, persistent checkpoint, or exactly-once claim. Replay stable IDs only with the
same intended content/model/namespace policy; upsert convergence does not remove repeated embedding
charges. The [durable embedding handoff recipe](durable-embeddings.md) demonstrates persisting
validated embeddings before delivery and replaying unfinished upserts. It binds the job to source
contents and explicit model/configuration identity; a lost embedding response before persistence
can still require a repeated paid call.

Batcher workers use an empty explicit `Context` and metadata-only exception logging. Provider
exceptions still reach callers and may contain response details; apply your own logging/redaction
policy there. The report contains elapsed pipeline time, completed item count, stage statistics,
and actual batch-size histograms; it excludes the preflight request and makes no speedup claim.

Run the local provider-boundary checks without credentials:

```bash
uv sync --locked --no-default-groups --group test --group example-test --extra pinecone --python 3.12
uv run --no-sync python -m pytest tests/provider_recipes -q
```

CI runs this optional suite on Python 3.11 and 3.15 and requires the real clients to import first,
so missing dependencies cannot silently skip the expected checks. Core-only installations remain
free of these optional dependencies.
