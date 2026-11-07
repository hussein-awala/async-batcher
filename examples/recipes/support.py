from __future__ import annotations

import asyncio
import json
from collections import Counter
from dataclasses import asdict, dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path

    from async_batcher import AsyncBatcher, BatchEvent


@dataclass(frozen=True)
class Document:
    id: str
    text: str


@dataclass
class BatchMeasurements:
    # Sizes are bounded by the count/pending limits; no per-item data is retained.
    sizes: Counter = field(default_factory=Counter)

    def observe(self, event: BatchEvent) -> None:
        self.sizes[event.size] += 1

    def report(self, batcher: AsyncBatcher) -> dict:
        return {
            "stats": asdict(batcher.stats()),
            "batch_size_histogram": dict(sorted(self.sizes.items())),
        }


def _read_chunk(path: Path, offset: int, chunk_size: int, max_line_bytes: int):
    documents = []
    # Open, read and close on the same worker. Cancellation cannot close a handle
    # while a background read is still using it.
    with path.open("rb") as stream:
        stream.seek(offset)
        for _ in range(chunk_size):
            line = stream.readline(max_line_bytes + 1)
            if not line:
                break
            if len(line) > max_line_bytes:
                raise ValueError(f"JSONL record exceeds {max_line_bytes} bytes")
            value = json.loads(line)
            if not isinstance(value, dict) or set(value) != {"id", "text"}:
                raise ValueError("Each JSONL record must contain exactly id and text")
            if not isinstance(value["id"], str) or not isinstance(value["text"], str):
                raise ValueError("JSONL id and text must be strings")
            documents.append(Document(**value))
        return documents, stream.tell()


async def read_jsonl(
    path: Path, *, chunk_size: int = 32, max_line_bytes: int = 65536
) -> AsyncIterator[Document]:
    """Read bounded chunks from a local file that is not modified during import."""
    if chunk_size < 1 or max_line_bytes < 1:
        raise ValueError("chunk_size and max_line_bytes must be positive")
    offset = 0
    while True:
        documents, offset = await asyncio.to_thread(_read_chunk, path, offset, chunk_size, max_line_bytes)
        if not documents:
            return
        for document in documents:
            yield document


async def demo_documents(count: int) -> AsyncIterator[Document]:
    for index in range(count):
        yield Document(str(index), f"Demonstration document {index}")
