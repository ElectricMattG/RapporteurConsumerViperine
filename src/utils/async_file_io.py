#!/usr/bin/env python3
"""
Async File I/O Utilities

Thread-safe and async file operations for the scraping pipeline.
Provides helpers for reading and writing JSON/JSONL files asynchronously.
"""

import asyncio
import json
from asyncio import Lock
from pathlib import Path
from typing import Any, Dict, List, Optional

import aiofiles

# Global lock for file operations to ensure thread safety
_file_locks: Dict[str, Lock] = {}
_locks_lock = Lock()


async def _get_file_lock(file_path: str) -> Lock:
    """
    Get or create a lock for a specific file path.

    Args:
        file_path: Path to the file

    Returns:
        asyncio.Lock for the file
    """
    async with _locks_lock:
        if file_path not in _file_locks:
            _file_locks[file_path] = Lock()
        return _file_locks[file_path]


async def async_write_json(
    file_path: Path,
    data: Dict[str, Any],
    mode: str = 'w',
    ensure_ascii: bool = False,
    indent: Optional[int] = None
) -> None:
    """
    Write JSON data to a file asynchronously with thread safety.

    Args:
        file_path: Path to the output file
        data: Dictionary to write as JSON
        mode: File mode ('w' for write, 'a' for append)
        ensure_ascii: Whether to escape non-ASCII characters
        indent: Indentation level (None for compact)
    """
    file_path = Path(file_path)
    file_path.parent.mkdir(parents=True, exist_ok=True)

    lock = await _get_file_lock(str(file_path))
    async with lock:
        async with aiofiles.open(file_path, mode, encoding='utf-8') as f:
            json_str = json.dumps(data, ensure_ascii=ensure_ascii, indent=indent)
            await f.write(json_str)


async def async_write_jsonl(
    file_path: Path,
    data: Dict[str, Any],
    mode: str = 'a',
    ensure_ascii: bool = False
) -> None:
    """
    Append a single JSON line to a JSONL file asynchronously with thread safety.

    Args:
        file_path: Path to the output file
        data: Dictionary to write as a JSON line
        mode: File mode ('w' for write, 'a' for append)
        ensure_ascii: Whether to escape non-ASCII characters
    """
    file_path = Path(file_path)
    file_path.parent.mkdir(parents=True, exist_ok=True)

    lock = await _get_file_lock(str(file_path))
    async with lock:
        async with aiofiles.open(file_path, mode, encoding='utf-8') as f:
            json_str = json.dumps(data, ensure_ascii=ensure_ascii)
            await f.write(json_str + '\n')


async def async_write_jsonl_batch(
    file_path: Path,
    data_list: List[Dict[str, Any]],
    mode: str = 'w',
    ensure_ascii: bool = False
) -> None:
    """
    Write multiple JSON objects to a JSONL file asynchronously with thread safety.

    Args:
        file_path: Path to the output file
        data_list: List of dictionaries to write as JSON lines
        mode: File mode ('w' for write, 'a' for append)
        ensure_ascii: Whether to escape non-ASCII characters
    """
    file_path = Path(file_path)
    file_path.parent.mkdir(parents=True, exist_ok=True)

    lock = await _get_file_lock(str(file_path))
    async with lock:
        async with aiofiles.open(file_path, mode, encoding='utf-8') as f:
            for data in data_list:
                json_str = json.dumps(data, ensure_ascii=ensure_ascii)
                await f.write(json_str + '\n')


async def async_read_json(file_path: Path) -> Dict[str, Any]:
    """
    Read JSON data from a file asynchronously with thread safety.

    Args:
        file_path: Path to the input file

    Returns:
        Parsed JSON data as dictionary
    """
    file_path = Path(file_path)
    lock = await _get_file_lock(str(file_path))

    async with lock:
        async with aiofiles.open(file_path, 'r', encoding='utf-8') as f:
            content = await f.read()
            return json.loads(content)


async def async_read_jsonl(file_path: Path) -> List[Dict[str, Any]]:
    """
    Read all JSON lines from a JSONL file asynchronously with thread safety.

    Args:
        file_path: Path to the input file

    Returns:
        List of parsed JSON objects
    """
    file_path = Path(file_path)
    lock = await _get_file_lock(str(file_path))

    data = []
    async with lock:
        async with aiofiles.open(file_path, 'r', encoding='utf-8') as f:
            async for line in f:
                line = line.strip()
                if line:
                    data.append(json.loads(line))
    return data


async def async_read_text(file_path: Path) -> str:
    """
    Read text from a file asynchronously with thread safety.

    Args:
        file_path: Path to the input file

    Returns:
        File contents as string
    """
    file_path = Path(file_path)
    lock = await _get_file_lock(str(file_path))

    async with lock:
        async with aiofiles.open(file_path, 'r', encoding='utf-8') as f:
            return await f.read()


async def async_write_text(
    file_path: Path,
    content: str,
    mode: str = 'w'
) -> None:
    """
    Write text to a file asynchronously with thread safety.

    Args:
        file_path: Path to the output file
        content: Text content to write
        mode: File mode ('w' for write, 'a' for append)
    """
    file_path = Path(file_path)
    file_path.parent.mkdir(parents=True, exist_ok=True)

    lock = await _get_file_lock(str(file_path))
    async with lock:
        async with aiofiles.open(file_path, mode, encoding='utf-8') as f:
            await f.write(content)


async def async_read_binary(file_path: Path) -> bytes:
    """
    Read binary data from a file asynchronously with thread safety.

    Args:
        file_path: Path to the input file

    Returns:
        File contents as bytes
    """
    file_path = Path(file_path)
    lock = await _get_file_lock(str(file_path))

    async with lock:
        async with aiofiles.open(file_path, 'rb') as f:
            return await f.read()


async def async_write_binary(
    file_path: Path,
    content: bytes,
    mode: str = 'wb'
) -> None:
    """
    Write binary data to a file asynchronously with thread safety.

    Args:
        file_path: Path to the output file
        content: Binary content to write
        mode: File mode ('wb' for write, 'ab' for append)
    """
    file_path = Path(file_path)
    file_path.parent.mkdir(parents=True, exist_ok=True)

    lock = await _get_file_lock(str(file_path))
    async with lock:
        async with aiofiles.open(file_path, mode) as f:
            await f.write(content)
