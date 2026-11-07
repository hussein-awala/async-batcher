from __future__ import annotations

import traceback

import pytest
from examples.recipes.resumable_sqlite import Source, read_chunk
from examples.recipes.support import JSONLRecordError, read_jsonl


@pytest.mark.asyncio
@pytest.mark.parametrize("resumable", [False, True])
@pytest.mark.parametrize(
    "bad",
    [b'{"private":"DO-NOT-LOG"}\n', b'{"text":"DO-NOT-LOG",}\n', b"\xffDO-NOT-LOG\n", b"DO-NOT-LOG" * 12],
)
async def test_invalid_record_reports_filename_and_utf8_byte_start_without_payload(tmp_path, resumable, bad):
    path = tmp_path / "documents.jsonl"
    prefix = '{"id":"one","text":"été 🌎"}\r\n'.encode()
    path.write_bytes(prefix + bad)
    with pytest.raises(JSONLRecordError) as caught:
        if resumable:
            read_chunk(Source.inspect(path), 0, max_line_bytes=64)
        else:
            await anext(read_jsonl(path, max_line_bytes=64))
    error = caught.value
    assert error.path == path
    assert error.offset == len(prefix)
    assert str(path) in str(error)
    assert f"byte {len(prefix)}" in str(error)
    rendered = "".join(traceback.format_exception(error))
    assert "DO-NOT-LOG" not in rendered
    assert "JSONDecodeError" not in rendered
    assert "UnicodeDecodeError" not in rendered


@pytest.mark.asyncio
async def test_diagnostics_keep_absolute_byte_offsets_across_chunks_and_no_final_newline(tmp_path):
    path = tmp_path / "rows.jsonl"
    prefix = b'{"id":"one","text":"ok"}\n'
    path.write_bytes(prefix + b'{"id":1,"text":"secret"}')
    source = read_jsonl(path, chunk_size=1)
    assert (await anext(source)).id == "one"
    with pytest.raises(JSONLRecordError, match="must be strings") as caught:
        await anext(source)
    assert caught.value.offset == len(prefix)
