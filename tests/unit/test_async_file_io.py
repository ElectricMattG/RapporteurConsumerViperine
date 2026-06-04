"""
tests/unit/test_async_file_io.py

Tests for src/utils/async_file_io.py

Contracts verified:
- async_write_json / async_read_json round-trip
- async_write_jsonl appends lines
- async_write_text / async_read_text round-trip
- async_write_binary / async_read_binary round-trip
- async_write_jsonl_batch writes all rows
- concurrent writes are serialized (last write wins with mode='w')
- parent directory is created automatically
"""
from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest

from utils.async_file_io import (async_read_binary, async_read_json,
                                 async_read_jsonl, async_read_text,
                                 async_write_binary, async_write_json,
                                 async_write_jsonl, async_write_jsonl_batch,
                                 async_write_text)

pytestmark = pytest.mark.anyio


class TestJson:
    async def test_write_and_read_round_trip(self, tmp_path):
        f = tmp_path / "data.json"
        data = {"key": "value", "num": 42, "nested": {"a": [1, 2, 3]}}
        await async_write_json(f, data)
        result = await async_read_json(f)
        assert result == data

    async def test_mode_w_overwrites(self, tmp_path):
        f = tmp_path / "data.json"
        await async_write_json(f, {"v": 1})
        await async_write_json(f, {"v": 2}, mode="w")
        result = await async_read_json(f)
        assert result == {"v": 2}

    async def test_parent_dir_created(self, tmp_path):
        f = tmp_path / "deep" / "nested" / "data.json"
        await async_write_json(f, {"x": 1})
        assert f.exists()

    async def test_non_ascii_preserved(self, tmp_path):
        f = tmp_path / "unicode.json"
        data = {"greeting": "こんにちは"}
        await async_write_json(f, data, ensure_ascii=False)
        result = await async_read_json(f)
        assert result["greeting"] == "こんにちは"

    async def test_indent_produces_pretty_output(self, tmp_path):
        f = tmp_path / "pretty.json"
        await async_write_json(f, {"a": 1}, indent=2)
        raw = f.read_text()
        assert "\n" in raw


class TestJsonl:
    async def test_single_append(self, tmp_path):
        f = tmp_path / "lines.jsonl"
        await async_write_jsonl(f, {"row": 1})
        rows = await async_read_jsonl(f)
        assert rows == [{"row": 1}]

    async def test_multiple_appends(self, tmp_path):
        f = tmp_path / "lines.jsonl"
        for i in range(3):
            await async_write_jsonl(f, {"row": i})
        rows = await async_read_jsonl(f)
        assert rows == [{"row": 0}, {"row": 1}, {"row": 2}]

    async def test_mode_w_truncates(self, tmp_path):
        f = tmp_path / "lines.jsonl"
        await async_write_jsonl(f, {"old": True}, mode="a")
        await async_write_jsonl(f, {"new": True}, mode="w")
        rows = await async_read_jsonl(f)
        assert rows == [{"new": True}]

    async def test_read_jsonl_skips_blank_lines(self, tmp_path):
        f = tmp_path / "lines.jsonl"
        f.write_text('{"a":1}\n\n{"b":2}\n', encoding="utf-8")
        rows = await async_read_jsonl(f)
        assert len(rows) == 2


class TestText:
    async def test_write_and_read_round_trip(self, tmp_path):
        f = tmp_path / "file.txt"
        await async_write_text(f, "hello world")
        result = await async_read_text(f)
        assert result == "hello world"

    async def test_append_mode(self, tmp_path):
        f = tmp_path / "log.txt"
        await async_write_text(f, "line1\n")
        await async_write_text(f, "line2\n", mode="a")
        result = await async_read_text(f)
        assert result == "line1\nline2\n"

    async def test_unicode_preserved(self, tmp_path):
        f = tmp_path / "unicode.txt"
        await async_write_text(f, "αβγ")
        result = await async_read_text(f)
        assert result == "αβγ"


class TestBinary:
    async def test_write_and_read_round_trip(self, tmp_path):
        f = tmp_path / "data.bin"
        payload = b"\x00\x01\x02\x03\xFF"
        await async_write_binary(f, payload)
        result = await async_read_binary(f)
        assert result == payload

    async def test_append_mode(self, tmp_path):
        f = tmp_path / "data.bin"
        await async_write_binary(f, b"\x01")
        await async_write_binary(f, b"\x02", mode="ab")
        result = await async_read_binary(f)
        assert result == b"\x01\x02"


class TestJsonlBatch:
    async def test_writes_all_rows(self, tmp_path):
        f = tmp_path / "batch.jsonl"
        rows = [{"i": i} for i in range(5)]
        await async_write_jsonl_batch(f, rows)
        result = await async_read_jsonl(f)
        assert result == rows

    async def test_mode_w_default_overwrites(self, tmp_path):
        f = tmp_path / "batch.jsonl"
        await async_write_jsonl_batch(f, [{"old": True}])
        await async_write_jsonl_batch(f, [{"new": True}], mode="w")
        result = await async_read_jsonl(f)
        assert result == [{"new": True}]

    async def test_mode_a_appends(self, tmp_path):
        f = tmp_path / "batch.jsonl"
        await async_write_jsonl_batch(f, [{"a": 1}])
        await async_write_jsonl_batch(f, [{"b": 2}], mode="a")
        result = await async_read_jsonl(f)
        assert len(result) == 2

    async def test_empty_list_creates_empty_file(self, tmp_path):
        f = tmp_path / "empty.jsonl"
        await async_write_jsonl_batch(f, [])
        rows = await async_read_jsonl(f)
        assert rows == []


class TestLocking:
    async def test_concurrent_appends_all_survive(self, tmp_path):
        """Concurrent writes must not lose data (lock serializes access)."""
        f = tmp_path / "concurrent.jsonl"
        rows = [{"i": i} for i in range(20)]
        await asyncio.gather(*[async_write_jsonl(f, row) for row in rows])
        result = await async_read_jsonl(f)
        assert len(result) == 20
        # All values must be present
        found = {r["i"] for r in result}
        assert found == set(range(20))
