# Acknowledge after a durable database effect

The [consumer recipe](../examples/recipes/acknowledged_consumer.py) demonstrates a broker-neutral
contract: persist the chosen effect and its deduplication receipt in one SQLite transaction,
then acknowledge that particular delivery. It uses only the standard library and the core
batcher. It does not make a distributed exactly-once guarantee.

## Stable identity and delivery ownership

`Message.identifier` is an immutable business identity that stays the same on replay. Scope it
by the source/topic/tenant/operation as needed; a delivery attempt ID is not a substitute.
`Delivery.acknowledgement_token` identifies this attempt and may change on redelivery. The
application supplies an authenticated source and a callback that acknowledges that delivery
using the broker's actual token. Authorize counter names and payloads before submission.

The example applies integer increments to named counters. In one native batch transaction it:

1. Checks whether each stable message identity already has a receipt.
2. For a replay, verifies that its payload matches and returns the existing success.
3. For a new message, writes its receipt and counter increment together.
4. Commits before returning successful results to the consumer.

Reusing an identity with a different payload fails the transaction. An invalid item, database
failure, or integer overflow rolls back the whole native batch, including earlier items in that
batch. Previous batches stay committed. This recipe chooses all-or-nothing native batches;
it does not isolate poison messages automatically or acknowledge failed records.

SQLite connections open and close inside their worker call. The recipe uses a file-backed
database with WAL journaling and `synchronous=FULL`; it relies on the storage system honoring
SQLite's [durability contract](https://www.sqlite.org/pragma.html#pragma_synchronous).
Connections close explicitly because Python's [transaction context](https://docs.python.org/3/library/sqlite3.html#how-to-use-the-connection-context-manager)
does not close them. It does not manage backups, disk loss, receipt expiration, or broker state. Keep
receipts for the entire redelivery horizon: deleting them permits the effect to happen again.
The deduplication check cannot protect an HTTP call, email, or other side effect outside the
same database transaction. Such workflows need an application-specific outbox or reconciliation
protocol rather than a new automatic retry rule.

## Run a local demonstration

The consumer helper lives in the repository, outside the installed package. Set up the
matching [recipe checkout](installation.md#run-the-repository-recipes), then save and run this example from its root
with `uv run --no-sync python`:

```python
import asyncio
from pathlib import Path
from tempfile import TemporaryDirectory

from examples.recipes.acknowledged_consumer import demo

with TemporaryDirectory() as directory:
    report = asyncio.run(demo(Path(directory) / "consumer.sqlite"))
    assert report.acknowledged == 2
    assert report.replayed == 1
    assert report.failed == 0
```

The demo delivers the same message twice with different acknowledgement tokens. The counter
changes once and both deliveries can be acknowledged. Replace the finite list and print callback
with your broker's lazy delivery source and acknowledgement API:

```python
from examples.recipes.acknowledged_consumer import consume

# store.initialize() must finish before intake starts.
# source yields Delivery objects; acknowledge(delivery) awaits the broker's ack.
report = await consume(source, store, acknowledge, batch_size=32, max_pending=64)
```

This is a wiring fragment, not a standalone broker client. Configure acknowledgement timeout,
redelivery, negative acknowledgement, and dead-letter behavior in the application. Acknowledgement
failures propagate so the application can reconnect or replay. A normally returned report counts
acknowledged deliveries, acknowledged replays, and failed outcomes left unacknowledged. Inspect
`failed`; a successful helper return does not mean every delivery was acknowledged.

## Crash and cancellation boundaries

| Failure point | Database state | What replay must do |
| --- | --- | --- |
| Before transaction commit | Neither new receipts nor their effects survive | Retry the same stable identities |
| After commit, before acknowledgement | Effects and receipts are durable, delivery may return | Verify payload and acknowledge its new delivery token without reapplying |
| Acknowledgement response lost | Broker acknowledgement state is ambiguous | Allow redelivery; the receipt still prevents a repeated database effect |
| A later item/source/acknowledgement fails | Earlier or concurrently accepted batches may have committed | Replay unacknowledged deliveries; do not assume the whole stream rolled back |

`map_outcomes()` preserves ordinary per-item failure correlation. Database errors leave deliveries
unacknowledged. Source errors, consumer cancellation, and acknowledgement failures terminate
iteration; accepted writes can still finish during the batcher's graceful drain. The helper closes
its map and source, then drains accepted work before returning or raising. Keep the database and
broker client alive through that cleanup. A native thread cannot be interrupted by cancelling its
awaiter; backend-native timeouts and deployment policy are separate concerns.

The map bounds consumed but unyielded items to `max_pending`. The acknowledgement callback can hold
one additional yielded outcome, so this helper retains up to `max_pending + 1` source items, plus
any prefetch in the broker client itself. Bound broker prefetch and payload size too. The example
uses one batch worker and acknowledges sequentially; these choices simplify ordering but do not
control another consumer's delivery order.

## Verify the failure windows

```bash
uv sync --locked
uv run --no-sync python -m pytest tests/core/test_acknowledged_consumer.py -ra
```

Tests use real SQLite transactions and a subprocess that exits after commit but before its first
acknowledgement. Restarting with fresh delivery tokens acknowledges both replayed messages without
repeating their effects. Other cases cover transaction rollback, mismatched identities, overflow,
acknowledgement failure, bounded intake, and source cleanup. No broker credentials are required.
