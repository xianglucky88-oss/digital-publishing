#!/usr/bin/env python3
"""Build the three same-prefix local HTTP response doubles used to verify
the streaming JSON collection path:

``feed_complete.json``
    A complete publication list: ``{"records": [...], "meta": {...}}``.

``feed_truncated.json``
    A byte prefix of the complete feed, cut only at the very tail.  The
    JSON is incomplete and must never be treated as a successful batch.

``feed_heavy_metadata.json``
    Shares its leading bytes with the complete feed (the whole records
    array and the start of ``meta``), but an irrelevant metadata field
    carries a multi-megabyte string.  Collection must skip that string
    without materializing it, so peak memory stays bounded.

The records themselves exercise multibyte UTF-8, JSON escapes (including a
\\uXXXX surrogate pair) and non-integer numbers.  All three files are
deterministic; re-running this script reproduces them byte-for-byte.

Usage:

    python3 scripts/make_streaming_fixtures.py [output-dir]

Defaults to ``fixtures/streaming`` under the repository root.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

RECORD_COUNT = 12
HEAVY_NOTE_REPEAT = 2_000_000  # "填" repeated: ~6 MB of UTF-8 in metadata


def _record(index: int) -> dict:
    isbn = f"978-7-{index:03d}"
    record = {
        "source": "publisher-api",
        "observed_at": f"2026-11-01T06:{index:02d}:00Z",
        "payload": {
            "isbn": isbn,
            "title": f"Data Pipelines 第{index}版",
            "publisher": "Example Press",
            "edition": "1",
            "language": "zh",
            "availability": "active",
        },
    }
    if index == 2:
        # Multibyte characters, a 4-byte emoji and escapes inside one string.
        record["payload"]["title"] = '采集工程😀 "引号" \\反斜杠\\ 换行\n制表\t'
    if index == 3:
        record["payload"]["pages"] = 320
        record["payload"]["weight_kg"] = 0.85
        record["payload"]["price"] = 1.25e2
    return record


def build_records() -> list[dict]:
    return [_record(index) for index in range(1, RECORD_COUNT + 1)]


def build_complete_bytes() -> bytes:
    """The complete feed, assembled by hand so the layout is deterministic."""
    lines = []
    for index, record in enumerate(build_records()):
        # One record keeps ensure_ascii=True so the feed also contains
        # literal \uXXXX escapes (a surrogate pair) next to raw UTF-8.
        ensure_ascii = index == RECORD_COUNT - 1
        lines.append("    " + json.dumps(record, ensure_ascii=ensure_ascii))
    records_block = ",\n".join(lines)
    return (
        '{\n  "records": [\n'
        + records_block
        + '\n  ],\n  "meta": {"page": 1, "note": "ok"}\n}\n'
    ).encode("utf-8")


def build_truncated_bytes(complete: bytes) -> bytes:
    """A pure prefix of the complete feed, cut only at the very tail."""
    assert complete.endswith(b'"}\n}\n')
    return complete[: -len(b'"}\n}\n')]


def build_heavy_metadata_bytes(complete: bytes) -> bytes:
    """The complete feed's prefix, with a huge string in irrelevant metadata."""
    marker = b'"meta": {"page": 1, "note": "'
    prefix, suffix = complete.split(marker, 1)
    heavy_note = "填".encode("utf-8") * HEAVY_NOTE_REPEAT
    return prefix + marker + heavy_note + b'"}\n}\n'


def main() -> None:
    output_dir = (
        Path(sys.argv[1])
        if len(sys.argv) > 1
        else Path(__file__).resolve().parents[1] / "fixtures" / "streaming"
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    complete = build_complete_bytes()
    truncated = build_truncated_bytes(complete)
    heavy = build_heavy_metadata_bytes(complete)

    # Sanity: the three doubles really do share their content prefix.
    assert complete.startswith(truncated[: len(truncated) - 3])
    shared = 0
    for a, b in zip(complete, heavy):
        if a != b:
            break
        shared += 1
    assert shared > len(complete) // 2

    targets = {
        "feed_complete.json": complete,
        "feed_truncated.json": truncated,
        "feed_heavy_metadata.json": heavy,
    }
    for name, payload in targets.items():
        path = output_dir / name
        path.write_bytes(payload)
        print(f"{path}: {len(payload)} bytes")
    print(f"shared prefix of complete vs heavy: {shared} bytes")
    print(f"truncated is a strict prefix of complete: {complete[: len(truncated)] == truncated}")


if __name__ == "__main__":
    main()
