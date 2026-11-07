# Public type contracts

The source checkout includes a [PEP 561 marker](https://typing.python.org/en/latest/spec/distributing.html)
in its wheel and source distribution. Type checkers can read the library's inline annotations
after a normal installation. These contracts describe the current checkout; use the documentation
for your installed release when working with an older published package.

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

This gate covers the public core, callable, streaming, grouping, and observability APIs. It
does not claim strict checking of every private implementation or optional SDK adapter. The
checker is a development dependency; installing the core library adds no runtime dependencies.
