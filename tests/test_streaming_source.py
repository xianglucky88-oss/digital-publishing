"""Streaming JSON collection: bounded memory, validation gates, cleanup.

These tests exercise the full HTTP -> staging -> validation -> pipeline
path (not just the parser) against local scripted endpoints, including the
three same-prefix response doubles: a complete feed, a tail-truncated feed
and a feed with a huge irrelevant metadata string.
"""

import json
import sqlite3
import threading
import tracemalloc
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

from digital_publishing.application import run_url
from digital_publishing.json_stream import StreamLimits, analyze_json, iter_records
from digital_publishing.sources import HttpJsonSource, RecordStream


# --------------------------------------------------------------------- feeds
def _record(index: int) -> dict:
    return {
        "source": "publisher-api",
        "observed_at": f"2026-11-01T06:{index % 60:02d}:00Z",
        "payload": {
            "isbn": f"978-7-{index:05d}",
            "title": f"Data Pipelines 第{index}版",
            "publisher": "Example Press",
            "edition": "1",
            "language": "zh",
            "availability": "active",
        },
    }


def make_feed(count: int, *, note: str = "ok") -> bytes:
    records = [_record(i) for i in range(count)]
    lines = ["    " + json.dumps(record, ensure_ascii=False) for record in records]
    return (
        '{\n  "records": [\n' + ",\n".join(lines) + f'\n  ],\n  "meta": {{"page": 1, "note": "{note}"}}}}\n'
    ).encode("utf-8")


def make_heavy_metadata_feed(count: int, note_repeat: int) -> bytes:
    """Same leading bytes as ``make_feed`` but a huge metadata string."""
    complete = make_feed(count)
    marker = b'"meta": {"page": 1, "note": "'
    prefix, _suffix = complete.split(marker, 1)
    return prefix + marker + "填".encode("utf-8") * note_repeat + b'"}}\n'


def truncate_tail(body: bytes, cut: int) -> bytes:
    """A pure prefix of ``body``, cut only at the very tail."""
    return body[:-cut]


FEED_RECORDS = 12
COMPLETE = make_feed(FEED_RECORDS)
TRUNCATED = truncate_tail(COMPLETE, 5)
HEAVY = make_heavy_metadata_feed(FEED_RECORDS, 2_000_000)  # ~6 MB metadata


def observations(database: str) -> int:
    conn = sqlite3.connect(database)
    try:
        return conn.execute("SELECT COUNT(*) FROM observations").fetchone()[0]
    finally:
        conn.close()


# --------------------------------------------------------------------- server
class _Handler(BaseHTTPRequestHandler):
    def do_GET(self):  # noqa: N802 - BaseHTTPRequestHandler API
        server: _Server = self.server  # type: ignore[assignment]
        with server.lock:
            entry = server.state.get(self.path)
        if entry is None:
            self.send_error(404)
            return
        body = entry["body"]
        if entry.get("disconnect"):
            # Declare the full length, deliver half, then drop the connection.
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body[: len(body) // 2])
            self.wfile.flush()
            self.connection.close()
            return
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        # Deliberately dribble the body in odd-sized writes so the client
        # cannot rely on convenient chunk boundaries.
        step = entry.get("write_step", 65536)
        for offset in range(0, len(body), step):
            self.wfile.write(body[offset : offset + step])
        self.wfile.flush()

    def log_message(self, *_args):
        return


class _Server(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.state: dict[str, dict] = {}
        self.lock = threading.Lock()

    def set(self, path: str, *, body: bytes, disconnect: bool = False, write_step: int = 65536):
        with self.lock:
            self.state[path] = {"body": body, "disconnect": disconnect, "write_step": write_step}

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.server_port}"


@pytest.fixture
def server():
    srv = _Server(("127.0.0.1", 0), _Handler)
    thread = threading.Thread(target=srv.serve_forever, daemon=True)
    thread.start()
    try:
        yield srv
    finally:
        srv.shutdown()
        thread.join(timeout=5)
        srv.server_close()


# ------------------------------------------------------- the three doubles
def test_complete_feed_is_collected(server, tmp_path):
    server.set("/feed", body=COMPLETE)
    database = str(tmp_path / "obs.sqlite3")
    reports: list[dict] = []
    result = run_url(f"{server.url}/feed", database, report=reports)
    assert result["processed"] == FEED_RECORDS
    assert result["rejected"] == 0
    assert result["observations"] == FEED_RECORDS
    assert reports[0]["status"] == "200"
    assert reports[0]["record_count"] == FEED_RECORDS
    assert reports[0]["downloaded_bytes"] == len(COMPLETE)


def test_truncated_feed_fails_and_preserves_formal_observations(server, tmp_path):
    database = str(tmp_path / "obs.sqlite3")
    server.set("/feed", body=COMPLETE)
    run_url(f"{server.url}/feed", database)
    assert observations(database) == FEED_RECORDS

    server.set("/feed", body=TRUNCATED)
    with pytest.raises(ValueError, match="not valid"):
        run_url(f"{server.url}/feed", database)
    # The truncated response was never treated as a successful batch:
    # every previously collected observation is still intact.
    assert observations(database) == FEED_RECORDS


def test_heavy_metadata_feed_is_skipped_in_bounded_memory(server, tmp_path):
    server.set("/feed", body=HEAVY)
    database = str(tmp_path / "obs.sqlite3")
    tracemalloc.start()
    try:
        result = run_url(f"{server.url}/feed", database)
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    assert result["processed"] == FEED_RECORDS
    assert result["observations"] == FEED_RECORDS
    # The ~6 MB metadata string is validated and skipped without being
    # materialized: peak Python allocations stay far below its size.
    assert peak < 2 * 1024 * 1024


def test_heavy_metadata_ahead_of_records_is_skipped_too(server, tmp_path):
    body = (
        b'{"meta": {"note": "' + "填".encode("utf-8") * 1_000_000 + b'"}, "records": '
        + json.dumps([_record(0)], ensure_ascii=False).encode("utf-8")
        + b"}"
    )
    server.set("/feed", body=body)
    database = str(tmp_path / "obs.sqlite3")
    tracemalloc.start()
    try:
        result = run_url(f"{server.url}/feed", database)
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    assert result["processed"] == 1
    assert peak < 2 * 1024 * 1024


# ------------------------------------------------------- chunk boundaries
def test_tiny_chunks_keep_multibyte_escapes_and_numbers_intact(server, tmp_path):
    payload = [
        {
            "source": "http",
            "observed_at": "2026-11-01T06:00:00Z",
            "payload": {
                "isbn": "978-1",
                "title": '采集工程😀 "引号" \\换行\n制表\t',
                "publisher": "Example Press",
                "edition": "1",
                "pages": 320,
                "weight": 0.85,
                "price": 1.25e2,
                "big": 123456789012345678901234567890,
            },
        }
    ]
    body = json.dumps({"records": payload}, ensure_ascii=False).encode("utf-8")
    server.set("/feed", body=body, write_step=17)
    source = HttpJsonSource(f"{server.url}/feed", chunk_size=3)
    assert list(source.read()) == payload


def test_parser_matches_json_loads_across_chunk_splits():
    document = {
        "records": [
            {"a": "采集😀工程", "b": [1, -2.5e-3, True, False, None], "c": {"x": 'esc"\\\n\t'}},
            {"n": 123456789012345678901234567890, "f": 1.0, "g": -0.5e+10},
            "plain",
            42,
            [],
            {},
        ],
        "meta": {"note": "unrelated"},
    }
    body = json.dumps(document, ensure_ascii=False).encode("utf-8")
    limits = StreamLimits()
    for size in (1, 2, 3, 5, 7, 64, 4096, len(body)):
        chunks = [body[i : i + size] for i in range(0, len(body), size)]
        info = analyze_json(chunks, None, limits)
        chunks = [body[i : i + size] for i in range(0, len(body), size)]
        assert list(iter_records(chunks, info, limits)) == document["records"]
        assert info.record_count == len(document["records"])


# ------------------------------------------------------- container shapes
def test_top_level_array_is_supported(server, tmp_path):
    body = json.dumps([_record(0), _record(1)], ensure_ascii=False).encode("utf-8")
    server.set("/feed", body=body)
    result = run_url(f"{server.url}/feed", str(tmp_path / "obs.sqlite3"))
    assert result["processed"] == 2


def test_default_container_keys_and_priority(server, tmp_path):
    for key in ("records", "data", "items", "results"):
        body = json.dumps({key: [_record(0)]}, ensure_ascii=False).encode("utf-8")
        server.set(f"/{key}", body=body)
        result = run_url(f"{server.url}/{key}", str(tmp_path / f"{key}.sqlite3"))
        assert result["processed"] == 1
    # Priority order wins over document order: records beats data even
    # though data appears first.
    body = json.dumps({"data": [_record(0), _record(1)], "records": [_record(2)]}).encode("utf-8")
    server.set("/priority", body=body)
    source = HttpJsonSource(f"{server.url}/priority")
    assert list(source.read()) == [_record(2)]


def test_records_key_nested_path(server, tmp_path):
    body = json.dumps(
        {"outer": {"inner": [_record(0), _record(1)]}, "tail": {"z": [1, 2, 3]}},
        ensure_ascii=False,
    ).encode("utf-8")
    server.set("/feed", body=body)
    result = run_url(
        f"{server.url}/feed", str(tmp_path / "obs.sqlite3"), records_key="outer.inner"
    )
    assert result["processed"] == 2


def test_records_key_missing_or_not_a_list_fails(server, tmp_path):
    server.set("/missing", body=b'{"a": 1}')
    with pytest.raises(ValueError, match="records key not found"):
        run_url(f"{server.url}/missing", str(tmp_path / "a.sqlite3"), records_key="b")
    server.set("/notlist", body=b'{"a": {"b": 5}}')
    with pytest.raises(ValueError, match="JSON list of records"):
        run_url(f"{server.url}/notlist", str(tmp_path / "b.sqlite3"), records_key="a.b")


def test_content_after_the_container_is_checked(server, tmp_path):
    # Valid trailing members are fine.
    body = b'{"records": [' + json.dumps(_record(0)).encode() + b'], "meta": {"more": [1, 2, 3]}}'
    server.set("/ok", body=body)
    assert run_url(f"{server.url}/ok", str(tmp_path / "ok.sqlite3"))["processed"] == 1
    # Garbage after the container fails the whole read.
    server.set("/garbage", body=body + b" garbage")
    with pytest.raises(ValueError, match="trailing data"):
        run_url(f"{server.url}/garbage", str(tmp_path / "g.sqlite3"))
    # Garbage inside the trailing metadata fails too.
    bad_tail = body[:-1] + b', "x": ]}'
    server.set("/badtail", body=bad_tail)
    with pytest.raises(ValueError, match="not valid"):
        run_url(f"{server.url}/badtail", str(tmp_path / "bt.sqlite3"))


# ------------------------------------------------------------------ limits
def test_total_byte_limit(server, tmp_path):
    database = str(tmp_path / "obs.sqlite3")
    server.set("/feed", body=COMPLETE)
    run_url(f"{server.url}/feed", database)
    source = HttpJsonSource(f"{server.url}/feed", max_bytes=len(COMPLETE) - 1)
    with pytest.raises(ValueError, match="byte limit"):
        source.read()
    assert observations(database) == FEED_RECORDS


def test_single_record_size_limit(server, tmp_path):
    database = str(tmp_path / "obs.sqlite3")
    server.set("/feed", body=COMPLETE)
    run_url(f"{server.url}/feed", database)

    fat = _record(99)
    fat["payload"]["title"] = "x" * 5000
    body = json.dumps({"records": [_record(0), fat, _record(1)]}, ensure_ascii=False).encode("utf-8")
    server.set("/fat", body=body)
    source = HttpJsonSource(f"{server.url}/fat", max_record_chars=1000)
    with pytest.raises(ValueError, match="record exceeds size limit"):
        source.read()
    # The failure happened during validation: nothing was imported.
    assert observations(database) == FEED_RECORDS


def test_depth_limit(server, tmp_path):
    database = str(tmp_path / "obs.sqlite3")
    server.set("/feed", body=COMPLETE)
    run_url(f"{server.url}/feed", database)

    nested = _record(0)
    cursor = nested["payload"]
    for _ in range(40):
        cursor["nested"] = {}
        cursor = cursor["nested"]
    body = json.dumps({"records": [nested]}).encode("utf-8")
    server.set("/deep", body=body)
    source = HttpJsonSource(f"{server.url}/deep", max_depth=16)
    with pytest.raises(ValueError, match="depth limit"):
        source.read()
    assert observations(database) == FEED_RECORDS


# ------------------------------------------------------- resource lifecycle
def test_early_close_releases_the_staging_file(server, tmp_path):
    server.set("/feed", body=make_feed(50))
    staging = tmp_path / "staging"
    source = HttpJsonSource(f"{server.url}/feed", staging_dir=str(staging))
    stream = source.read()
    iterator = iter(stream)
    first = next(iterator)
    assert first["payload"]["isbn"] == "978-7-00000"
    leftovers = list(staging.glob(".staging-*"))
    assert len(leftovers) == 1  # still staged while the stream is open
    stream.close()
    assert list(staging.glob(".staging-*")) == []


def test_full_consumption_releases_the_staging_file(server, tmp_path):
    server.set("/feed", body=make_feed(50))
    staging = tmp_path / "staging"
    source = HttpJsonSource(f"{server.url}/feed", staging_dir=str(staging))
    assert len(list(source.read())) == 50
    assert list(staging.glob(".staging-*")) == []


def test_record_stream_close_before_any_iteration_releases(tmp_path):
    snapshot = tmp_path / "snapshot.json"
    snapshot.write_bytes(b'{"records": [{"a": 1}]}')
    stream = RecordStream(
        snapshot,
        analyze_json([snapshot.read_bytes()], None, StreamLimits()),
        StreamLimits(),
        65536,
        cleanup_path=snapshot,
    )
    stream.close()
    assert not snapshot.exists()


def test_network_disconnect_mid_body_releases_and_preserves(server, tmp_path):
    database = str(tmp_path / "obs.sqlite3")
    server.set("/feed", body=COMPLETE)
    run_url(f"{server.url}/feed", database)
    assert observations(database) == FEED_RECORDS

    staging = tmp_path / "staging"
    server.set("/feed", body=make_feed(500), disconnect=True)
    source = HttpJsonSource(f"{server.url}/feed", staging_dir=str(staging))
    with pytest.raises(RuntimeError, match="truncated|failed"):
        source.read()
    assert list(staging.glob(".staging-*")) == []
    assert observations(database) == FEED_RECORDS


def test_failed_validation_leaves_no_staging_file(server, tmp_path):
    staging = tmp_path / "staging"
    server.set("/feed", body=TRUNCATED)
    source = HttpJsonSource(f"{server.url}/feed", staging_dir=str(staging))
    with pytest.raises(ValueError):
        source.read()
    assert list(staging.glob(".staging-*")) == []


# ------------------------------------------------------- memory vs records
def test_peak_memory_does_not_grow_with_record_count(server, tmp_path):
    peaks: dict[int, int] = {}
    for count in (400, 4000):
        server.set("/feed", body=make_feed(count))
        tracemalloc.start()
        try:
            result = run_url(f"{server.url}/feed", str(tmp_path / f"obs{count}.sqlite3"))
            _, peaks[count] = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()
        assert result["processed"] == count
        assert result["observations"] == count
    # Ten times the records must not mean ten times the memory: the peak is
    # bounded by chunk, record and depth, not by the record count.
    assert peaks[4000] < peaks[400] + 1024 * 1024
