# Public type contracts

Async Batcher 0.3.0 includes a [PEP 561 marker](https://typing.python.org/en/latest/spec/distributing.html)
in its wheel and source distribution. Type checkers can read the library's inline annotations
after a normal [package installation](installation.md). Use the version selector to match
these contracts to your installed release.

## Inference and explicit types

`AsyncBatcher[T, S]` accepts inputs of type `T` and returns `S` from `process()`. Its bounded
`map()` iterator yields the same result type. `from_callable()` infers these parameters from
an annotated synchronous or asynchronous processor:

```python
from typing import assert_type

from async_batcher import AsyncBatcher


async def render(items: list[int]) -> list[str]:
    return [str(item) for item in items]


batcher = AsyncBatcher.from_callable(
    render,
    max_batch_size=32,
    max_batch_cost=100,
    item_cost=lambda item: 1,
)
assert_type(batcher, AsyncBatcher[int, str])


async def use_batcher() -> None:
    async with batcher:
        assert_type(await batcher.process(1), str)
        async for result in batcher.map([1, 2, 3], max_pending=8):
            assert_type(result, str)
```

A processor returning only `None` produces `AsyncBatcher[T, None]`. A processor that may
return either results or `None` produces an optional result type. Per-item `Exception` and
`asyncio.CancelledError` values are raised for their callers, so they are not successful result
types. Annotate a processor's return type to express those alternatives accurately.

Processors can return ordered, sized iterables, including custom containers with `__len__`
and `__iter__`. Runtime checks still reject unordered containers, mappings, text, and a result
count that differs from the batch size. Type checking does not replace those checks or the
runtime validation of positive limits and finite costs. Ordinary integer and float costs, as
well as `numbers.Real` implementations such as `Fraction`, are accepted.

The result parameter is covariant: a batcher returning `str` can be used where one returning
`object` is required. Inputs remain invariant. `GroupedBatcher[K, T, S]` carries the group-key,
input, and result types separately. `BatcherStats` and `BatchEvent` are immutable typed
snapshots; monitoring callbacks must be synchronous and return `None`.

## Checking changes

The dedicated `typecheck` group pins mypy independently of optional SDK stubs in the `typing`
group. Run the same installed-package check as CI:

```bash
uv sync --locked --no-default-groups --group typecheck
uv run --no-sync python .github/scripts/check_public_typing.py
```

The helper builds and installs a wheel in an isolated temporary environment, then checks strict
consumer examples outside the checkout. Positive examples assert inferred types. Negative
examples require exact error locations and codes for invalid inputs, incompatible callbacks,
unknown options, and mutation of immutable snapshots. Removing the installed `py.typed` marker
must also make the positive example fail. Distribution checks independently verify the marker
in the direct wheel and the wheel rebuilt from the source distribution.

The core gate covers public callable, streaming, grouping, and observability APIs. Kafka and
Kinesis have additional installed-wheel consumer checks, run in their adapter CI jobs:

```bash
uv run --no-sync python .github/scripts/check_public_typing.py --adapter kafka
uv run --no-sync python .github/scripts/check_public_typing.py --adapter aws
```

After syncing the `typecheck` group above, these commands export the selected extra from
`uv.lock` and install its exact, hash-checked dependencies into a separate temporary environment.
The AWS check also installs the existing locked `typing` stub group. The main environment and
core-only CI jobs remain free of optional SDKs.

These checks verify record inputs, core keyword options, callback types, mapped results, and
SDK metadata. Kafka results are `RecordMetadata | None`, preserving the producer's `acks=0`
behavior. The gate reads aiokafka's inline annotations using mypy's narrowly scoped
[`follow_untyped_imports`](https://mypy.readthedocs.io/en/stable/config_file.html#confval-follow_untyped_imports)
option because the locked SDK does not ship a `py.typed` marker; metadata field assertions
prevent an unnoticed fallback to `Any`. Kinesis acknowledgements keep the existing
`dict[str, Any]` contract for service response fields. Unknown keywords and incompatible input
or result types must produce exact diagnostics; no missing-import errors are suppressed.

This does not claim strict checking of every private implementation or other optional SDK
adapter. The checker remains a development dependency; installing the core library adds no
runtime dependencies.
