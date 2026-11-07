# Compatibility and migration

These docs describe the source checkout. As of 2026-11-07, the latest published GitHub release is
[0.2.2](https://github.com/hussein-awala/async-batcher/releases/tag/0.2.2). Installing that release
does not provide the newer APIs used by the current quickstart. Read the
[0.2.2 README](https://github.com/hussein-awala/async-batcher/blob/0.2.2/README.md) for that version.

## Choose the matching package and documentation

| Capability | Published 0.2.2 | Current checkout |
| --- | --- | --- |
| Python minimum | 3.10 | 3.11 |
| Subclass `AsyncBatcher`, implement `process_batch`, submit with `process` | Available | Available; see stricter contracts below |
| `from_callable` and async context managers | Not available | [Functions and cleanup](functions.md) |
| Bounded `map`, `map_outcomes`, waiting admission, `flush` | Not available | [Streams](bulk-processing.md) and [flush](api.md#flush-without-stopping-intake) |
| Cost budgets, statistics, groups, coalescing | Not available | [API guide](api.md) and linked composition guides |
| DynamoDB, Scylla, SQLAlchemy, Keras, scikit-learn extras | Available with older SDK requirements | Available with refreshed requirements and corrected result/failure behavior |
| Kafka, MongoDB, Pinecone, Kinesis/Firehose adapters | Not available | [Adapter catalog](../README.md#optional-adapters) |
| Development environment | Poetry | uv with the committed `uv.lock` |

To stay on the published version, pin it explicitly:

```bash
python -m pip install 'async-batcher==0.2.2'
```

To try the documented source features reproducibly, pin a reviewed commit rather than a moving
branch. For example, the following revision contains the feature set listed above:

```bash
git clone https://github.com/hussein-awala/async-batcher.git
cd async-batcher
git checkout --detach aeeaa5630a0ad9d10ac8764cb9bf2ead1145f7f3
uv sync --locked --no-default-groups --python 3.12
uv run --no-sync python -c 'from async_batcher import AsyncBatcher; print(AsyncBatcher.from_callable)'
```

Use the docs in that checkout, and substitute a newer reviewed revision when adopting subsequent
fixes. This is a source snapshot, not a published release or a recommendation to freeze forever.
A direct pip VCS install can pin the same commit, but does not use the repository's dependency
lockfile. Extras and their clients still need explicit selection; see each adapter guide.

The source version `0.0.1` is a build placeholder, not the latest release number. The publication
workflow supplies the release version. Record the commit SHA when reporting a source-install bug;
`importlib.metadata.version("async-batcher")` alone cannot identify the checkout.

## Upgrade an application deliberately

1. Move the application and its CI to Python 3.11 or newer. Check each optional SDK's supported
   interpreters and install only the extras the application needs.
2. Keep existing subclass processors if they suit the application. Return one result per input in
   order, or `None` for all-`None` results. Current validation rejects unordered/unsized containers,
   strings, bytes, and incorrect result counts before reporting any success from that collection.
   Returning an exception at a result position fails that caller; it does not roll back backend writes.
3. Check constructor arguments. Invalid numeric limits and unknown options now fail explicitly.
   Replace deprecated `batch_size` with `max_batch_size`; `sleep_time` and `buffering_time` have no
   effect. `max_batch_size=1` is supported. Choose finite queue, batch, and concurrency limits for
   production; `max_queue_size=0` still means unlimited, not disabled.
4. Put batcher cleanup inside the lifetime of its clients and executors. An async context manager
   drains accepted work even on exceptional exit. Caller cancellation does not prove that a write
   was prevented. A shutdown timeout leaves cleanup in progress; keep dependencies open and await
   cleanup again. See the [lifecycle contract](api.md#lifecycle) before changing restart behavior.
5. Recheck adapter output, transaction, partial-error, and retry contracts against its current
   guide. SDK upgrades and corrected result mapping can expose assumptions in application code.
   Test duplicate IDs, missing results, partial failures, and replay using your own payload shapes.
6. Run application tests against the pinned candidate. For a stream, prefer bounded `map` over
   creating one task for every input. For a service, test overload and shutdown while work is active.
   The [starting configurations](starting-configurations.md) illustrate both paths.

Automatic restart belongs to a fully stopped `AsyncBatcher` on its original event loop. Grouped
managers and coalescing wrappers close permanently. Do not move existing instances between loops
or mutate their construction-time limits to reconfigure a running service.

New context, logging, cancellation-discard, grouping, and coalescing options are opt-in. Adopt them
only with the corresponding ownership and compatibility rules; an upgrade does not require using
every feature. Keep a pinned previous application environment available for rollback.
