#!/usr/bin/env python3
"""Memory and failure-path evidence for the streaming JSON collection path.

Every case runs the real end-to-end path — local HTTP server, chunked
staging, full-document validation, pipeline, SQLite sink — inside a fresh
subprocess, and reports that process's peak RSS (``ru_maxrss``).  The
record counts increase step by step so the memory-versus-record-count
curve is directly observable; failure cases use a huge single record,
trailing garbage after the container, a tail-truncated body, a depth bomb
and a total-bytes limit, each verifying that previously collected
observations survive untouched.

Run from the repository root:

    PYTHONPATH=src python3 scripts/measure_streaming_memory.py

The script prints one JSON report.  It exits non-zero if any expectation
(flat memory, bounded skip, clean failure) is violated.
"""

from __future__ import annotations

import argparse
import json
import os
import resource
import sqlite3
import subprocess
import sys
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"

RECORD_COUNTS = (1_000, 8_000, 24_000)
HEAVY_METADATA_BYTES = 48 * 1024 * 1024
FLATNESS_SLACK_KB = 24 * 1024  # interpreter/allocator noise allowance


# --------------------------------------------------------------------- child
def _record_line(index: int) -> bytes:
    record = {
        "source": "publisher-api",
        "observed_at": f"2026-11-01T06:{index % 60:02d}:00Z",
        "payload": {
            "isbn": f"978-7-{index:07d}",
            "title": f"Data Pipelines 第{index}版",
            "publisher": "Example Press",
            "edition": "1",
            "language": "zh",
            "availability": "active",
        },
    }
    return json.dumps(record, ensure_ascii=False).encode("utf-8")


def write_feed(path: Path, count: int, *, heavy_metadata: int = 0) -> int:
    """Stream a feed to disk without holding it; return its byte size."""
    with open(path, "wb") as handle:
        handle.write(b'{\n  "records": [\n')
        for index in range(count):
            handle.write(b"    " + _record_line(index) + (b",\n" if index < count - 1 else b"\n"))
        handle.write(b'  ],\n  "meta": {"page": 1, "note": "')
        if heavy_metadata:
            block = "填".encode("utf-8") * 100_000
            remaining = heavy_metadata
            while remaining > 0:
                piece = block[:remaining]
                handle.write(piece)
                remaining -= len(piece)
        else:
            handle.write(b"ok")
        handle.write(b'"}}\n')
    return path.stat().st_size


class _Handler(BaseHTTPRequestHandler):
    def do_GET(self):  # noqa: N802 - BaseHTTPRequestHandler API
        feed = self.server.feed_path  # type: ignore[attr-defined]
        size = feed.stat().st_size
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(size))
        self.end_headers()
        with open(feed, "rb") as handle:
            while True:
                chunk = handle.read(65536)
                if not chunk:
                    break
                self.wfile.write(chunk)

    def log_message(self, *_args):
        return


def _observations(database: str) -> int:
    conn = sqlite3.connect(database)
    try:
        return conn.execute("SELECT COUNT(*) FROM observations").fetchone()[0]
    finally:
        conn.close()


def _run_collection(feed: Path, database: str, **kwargs) -> dict:
    from digital_publishing.application import run_url

    server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    server.feed_path = feed  # type: ignore[attr-defined]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        url = f"http://127.0.0.1:{server.server_port}/feed"
        return run_url(url, database, **kwargs)
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()


def child_main(case: dict, work: Path) -> dict:
    kind = case["kind"]
    database = str(work / "observations.sqlite3")
    if kind == "records":
        feed = work / "feed.json"
        size = write_feed(feed, case["count"])
        result = _run_collection(feed, database)
        return {"body_bytes": size, "result": result}
    if kind == "heavy_metadata":
        feed = work / "feed.json"
        size = write_feed(feed, case["count"], heavy_metadata=case["metadata_bytes"])
        result = _run_collection(feed, database)
        return {"body_bytes": size, "result": result}
    if kind == "failure":
        # Seed the store with a good feed, then attack it and verify the
        # previously collected observations survive untouched.
        good = work / "good.json"
        write_feed(good, 200)
        seeded = _run_collection(good, database)
        before = _observations(database)

        bad = work / "bad.json"
        name = case["failure"]
        kwargs: dict = {}
        if name == "huge_record":
            with open(bad, "wb") as handle:
                handle.write(b'{"records": [' + _record_line(0)[:-1] + b',"x":"')
                block = b"y" * 1_000_000
                for _ in range(2):  # a single ~2 MB record field
                    handle.write(block)
                handle.write(b'"}]}')
        elif name == "trailing_garbage":
            size = write_feed(bad, 200)
            with open(bad, "ab") as handle:
                handle.write(b" garbage")
        elif name == "truncated":
            size = write_feed(bad, 200)
            with open(bad, "r+b") as handle:
                handle.truncate(size - 5)
        elif name == "depth_bomb":
            with open(bad, "wb") as handle:
                handle.write(b'{"records": [' + b"[" * 500 + b"]" * 500 + b"]}")
        elif name == "byte_limit":
            write_feed(bad, 200)
            kwargs["max_bytes"] = 10_000
        else:  # pragma: no cover - script misuse
            raise SystemExit(f"unknown failure case: {name}")

        error = None
        try:
            _run_collection(bad, database, **kwargs)
        except (ValueError, RuntimeError) as exc:
            error = f"{type(exc).__name__}: {exc}"
        after = _observations(database)
        return {
            "failure": name,
            "seeded_observations": before,
            "observations_after_failure": after,
            "observations_preserved": before == after == seeded["observations"],
            "error": error,
        }
    raise SystemExit(f"unknown case kind: {kind}")  # pragma: no cover


def run_child(case: dict) -> dict:
    with tempfile.TemporaryDirectory(prefix="measure-streaming-") as temporary:
        outcome = child_main(case, Path(temporary))
    peak_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    print(json.dumps({"peak_rss_kb": peak_kb, **outcome}, ensure_ascii=False, sort_keys=True))
    return outcome


# --------------------------------------------------------------------- parent
def run_case(case: dict) -> dict:
    environment = dict(os.environ)
    existing = environment.get("PYTHONPATH", "")
    environment["PYTHONPATH"] = (
        str(SRC_ROOT) + os.pathsep + existing if existing else str(SRC_ROOT)
    )
    completed = subprocess.run(
        [sys.executable, str(Path(__file__).resolve()), "--child", json.dumps(case)],
        capture_output=True,
        text=True,
        env=environment,
        timeout=900,
    )
    if completed.returncode != 0:
        return {"case": case, "error": completed.stderr.strip()[-500:], "failed": True}
    return {"case": case, **json.loads(completed.stdout.strip().splitlines()[-1])}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--child", metavar="CASE_JSON", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.child:
        run_child(json.loads(args.child))
        return

    cases = (
        [{"kind": "records", "count": n} for n in RECORD_COUNTS]
        + [{"kind": "heavy_metadata", "count": 500, "metadata_bytes": HEAVY_METADATA_BYTES}]
        + [
            {"kind": "failure", "failure": name}
            for name in (
                "huge_record",
                "trailing_garbage",
                "truncated",
                "depth_bomb",
                "byte_limit",
            )
        ]
    )
    measurements = []
    for case in cases:
        label = case.get("failure") or f"{case['kind']}:{case.get('count', '')}"
        print(f"running {label} ...", file=sys.stderr, flush=True)
        measurements.append(run_case(case))

    report: dict = {"cases": {}, "verdicts": {}}
    record_peaks: dict[int, int] = {}
    ok = True
    for measurement in measurements:
        case = measurement["case"]
        if measurement.get("failed"):
            ok = False
            report["cases"][str(case)] = measurement
            continue
        peak_kb = measurement["peak_rss_kb"]
        if case["kind"] == "records":
            count = case["count"]
            result = measurement["result"]
            entry = {
                "records": count,
                "body_bytes": measurement["body_bytes"],
                "peak_rss_kb": peak_kb,
                "processed": result["processed"],
                "observations": result["observations"],
            }
            entry["ok"] = result["processed"] == result["observations"] == count
            ok &= entry["ok"]
            record_peaks[count] = peak_kb
            report["cases"][f"records_{count}"] = entry
        elif case["kind"] == "heavy_metadata":
            result = measurement["result"]
            entry = {
                "records": case["count"],
                "body_bytes": measurement["body_bytes"],
                "metadata_bytes": case["metadata_bytes"],
                "peak_rss_kb": peak_kb,
                "processed": result["processed"],
                "observations": result["observations"],
            }
            # A 48 MB ignored string must not be pulled into memory: the
            # peak stays within noise of the plain records baseline even
            # though the body is ~200x larger.  (The baseline peak is
            # filled in after all cases have run.)
            entry["ok"] = result["processed"] == case["count"]
            ok &= entry["ok"]
            report["cases"]["heavy_metadata"] = entry
        else:
            entry = {
                "failure": measurement["failure"],
                "peak_rss_kb": peak_kb,
                "error": measurement["error"],
                "observations_preserved": measurement["observations_preserved"],
            }
            entry["ok"] = measurement["error"] is not None and measurement[
                "observations_preserved"
            ]
            ok &= entry["ok"]
            report["cases"][f"failure_{measurement['failure']}"] = entry

    if len(record_peaks) == len(RECORD_COUNTS):
        smallest, largest = RECORD_COUNTS[0], RECORD_COUNTS[-1]
        growth_kb = record_peaks[largest] - record_peaks[smallest]
        report["verdicts"]["memory_vs_record_count"] = {
            "records": [smallest, largest],
            "peak_rss_kb": [record_peaks[smallest], record_peaks[largest]],
            "growth_kb": growth_kb,
            "flat": growth_kb < FLATNESS_SLACK_KB,
        }
        ok &= report["verdicts"]["memory_vs_record_count"]["flat"]
        heavy = report["cases"].get("heavy_metadata")
        if heavy is not None:
            heavy_peak = heavy["peak_rss_kb"]
            baseline_peak = record_peaks[smallest]
            heavy["baseline_peak_rss_kb"] = baseline_peak
            heavy["bounded_skip"] = heavy_peak < baseline_peak + FLATNESS_SLACK_KB
            heavy["ok"] = heavy["ok"] and heavy["bounded_skip"]
            ok &= heavy["bounded_skip"]
    report["ok"] = bool(ok)
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    if not ok:
        raise SystemExit("streaming memory evidence assertions failed")


if __name__ == "__main__":
    main()
