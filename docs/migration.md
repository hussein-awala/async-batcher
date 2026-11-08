# Upgrade from 0.2.2 to 0.3.0

Async Batcher 0.3.0 adds public APIs and adapters while retaining the existing subclass
interface and configuration defaults. Review the compatibility changes below before
upgrading an application. The shared
[release history](https://hussein.awala.fr/async-batcher/release-notes/) records publication
status and release notes; use the version selector for the docs matching your installation.

## What changes

| Capability | 0.2.2 | 0.3.0 |
| --- | --- | --- |
| Python minimum | 3.10 | 3.11 |
| Subclass `AsyncBatcher`, implement `process_batch`, submit with `process` | Available | Available, with stricter result/configuration validation |
| `from_callable` and async context managers | Not available | [Functions and cleanup](functions.md) |
| Bounded `map`, `map_outcomes`, waiting admission, `flush` | Not available | [Streams](bulk-processing.md) and [flush](api.md#flush-without-stopping-intake) |
| Cost budgets, statistics, groups, coalescing | Not available | [API guide](api.md) and linked composition guides |
| DynamoDB, Scylla, SQLAlchemy, Keras, scikit-learn extras | Older SDK requirements | Updated SDK requirements and corrected result/failure behavior |
| Kafka, MongoDB, Pinecone, Kinesis/Firehose adapters | Not available | [Installation and adapters](installation.md#optional-adapters) |
| Contributor environment | Poetry | uv with the committed `uv.lock` |

Install the selected version, retaining the extras your application uses:

```bash
python -m pip install --upgrade 'async-batcher==0.3.0'
# For example, an AWS application:
python -m pip install --upgrade 'async-batcher[aws]==0.3.0'
```

The [installation guide](installation.md) covers application environments and optional clients.
For an application staying on 0.2.2, pin `async-batcher==0.2.2` and use its
[tagged documentation](https://github.com/hussein-awala/async-batcher/blob/0.2.2/README.md).

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
5. Recheck adapter output, transaction, partial-error, and retry contracts against its guide.
   SQLAlchemy now requires 2.0.10+; its bulk updates require mapped ORM models and primary
   keys, and reject Core `Table` updates and UPDATE RETURNING. Keras requires 3.15.1+;
   Keras 2 is no longer supported. Install and validate the chosen Keras backend separately.
   DynamoDB uses TLS by default and applies bounded retries to unprocessed requests. SDK upgrades
   and corrected result mapping can expose assumptions in application code.
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
