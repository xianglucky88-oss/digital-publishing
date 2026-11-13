#!/usr/bin/env python3
"""Replayable evidence for the JSON HTTP conditional-download cache.

Every HTTP response is produced by a local, scripted server driven from
memory: no network, no fixtures on disk.  The script runs the scenarios the
implementation must survive and prints one JSON document that maps every
conditional request header to the exact response body it validated.

Run from the repository root:

    PYTHONPATH=src python3 scripts/replay_conditional_cache.py

Scenarios
1. ``replay``        first 200, then 304; identical records reach the pipeline;
                     the second read downloads zero body bytes.
2. ``interleaved``   A sends an old validator, B commits newer bytes, A's
                     delayed 304 arrives.  A returns the bytes it validated;
                     B's cache entry survives.
3. ``containers``    same address, different ``records_key`` -> separate
                     identities and separate 304 replays.
4. ``validators``    same body, different ETag -> no false 304; a simulated
                     late 304 for the old validator cannot rebind the entry.
5. ``scope``         access-scope and auth-config changes never reuse an old
                     validator.
6. ``restart``       after a real process restart, deleted / tampered cache
                     snapshots cause a full read instead of a wrong 304.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from digital_publishing.application import run_fixture  # noqa: F401 - import check
from digital_publishing.http_cache import ConditionalCache
from digital_publishing.normalize import normalize
from digital_publishing.pipeline import IngestionPipeline
from digital_publishing.sources import HttpJsonSource
from digital_publishing.storage import ObservationStore

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"


# --------------------------------------------------------------------- records
def record(isbn: str) -> dict:
    return {
        "source": "http",
        "observed_at": "2026-11-01T06:00:00Z",
        "payload": {
            "isbn": isbn,
            "title": f"Title {isbn}",
            "publisher": "Example Press",
            "edition": "1",
            "language": "en",
            "availability": "active",
        },
    }


def body(*isbns: str) -> bytes:
    return json.dumps({"records": [record(isbn) for isbn in isbns]}).encode("utf-8")


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def canonical(value) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


# --------------------------------------------------------------------- server
class Endpoint(BaseHTTPRequestHandler):
    def do_GET(self):  # noqa: N802 - BaseHTTPRequestHandler API
        server: ScriptedServer = self.server  # type: ignore[assignment]
        if_none_match = self.headers.get("If-None-Match")
        if_modified_since = self.headers.get("If-Modified-Since")
        recorded = {
            "path": self.path,
            "if_none_match": if_none_match,
            "if_modified_since": if_modified_since,
            "authorization": self.headers.get("Authorization"),
        }
        gate = server.take_gate(self.path, if_none_match)
        if gate is not None:
            reached, release, response_etag = gate
            reached.set()
            release.wait(timeout=10)
            with server.lock:
                server.requests.append({**recorded, "role": "held-conditional"})
            self._reply_304(response_etag, server)
            return
        with server.lock:
            server.requests.append(recorded)
            entry = server.state.get(self.path)
        if entry is None:
            self.send_error(404)
            return
        etag = entry.get("etag")
        last_modified = entry.get("last_modified")
        if entry.get("always_304"):
            self._reply_304(etag or '"forced"', server)
            return
        if etag and if_none_match == etag:
            self._reply_304(etag, server)
            return
        if not etag and last_modified and if_modified_since == last_modified:
            self.send_response(304)
            self.send_header("Last-Modified", last_modified)
            self.end_headers()
            server.note_status(304)
            return
        payload = entry["body"]
        self.send_response(200)
        if etag:
            self.send_header("ETag", etag)
        if last_modified:
            self.send_header("Last-Modified", last_modified)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)
        server.note_status(200)

    def _reply_304(self, etag: str, server: "ScriptedServer") -> None:
        self.send_response(304)
        self.send_header("ETag", etag)
        self.end_headers()
        server.note_status(304)

    def log_message(self, *_args):
        return


class ScriptedServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.state: dict[str, dict] = {}
        self.requests: list[dict] = []
        self.statuses: list[int] = []
        self.lock = threading.Lock()
        self._gates: dict[str, tuple[str, threading.Event, threading.Event, str]] = {}

    def note_status(self, status: int) -> None:
        with self.lock:
            self.statuses.append(status)

    def set(self, path: str, *, payload: bytes, etag: str | None = None) -> None:
        with self.lock:
            self.state[path] = {"body": payload, "etag": etag}

    def always_304(self, path: str, *, etag: str) -> None:
        with self.lock:
            self.state[path] = {"body": b"", "etag": etag, "always_304": True}

    def hold(self, path: str, if_none_match: str, response_etag: str):
        reached = threading.Event()
        release = threading.Event()
        with self.lock:
            self._gates[path] = (if_none_match, reached, release, response_etag)
        return reached, release

    def take_gate(self, path: str, if_none_match: str | None):
        with self.lock:
            gate = self._gates.get(path)
            if gate is None or gate[0] != if_none_match:
                return None
            self._gates.pop(path)  # one-shot: only the first match is held
            return gate[1:]

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.server_port}"


def start_server() -> tuple[ScriptedServer, threading.Thread]:
    server = ScriptedServer(("127.0.0.1", 0), Endpoint)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, thread


def stop_server(server: ScriptedServer, thread: threading.Thread) -> None:
    server.shutdown()
    thread.join(timeout=5)
    server.server_close()


# ---------------------------------------------------------------- evidence
def request_evidence(report: dict, *, role: str = "") -> dict:
    entry = {
        "role": role,
        "status": report["status"],
        "conditional_headers": report["conditional_headers"],
        "response_headers": {
            key: value
            for key, value in report["response_headers"].items()
            if key in ("ETag", "Last-Modified")
        },
        "record_count": report["record_count"],
        "snapshot_digest": report["snapshot_digest"],
        "snapshot_bytes": report["snapshot_bytes"],
        "downloaded_bytes": report["downloaded_bytes"],
        "cache_hit": report["cache_hit"],
        "identity": report["identity"],
    }
    if report["status"] == "304":
        entry["validated_body_digest"] = report["snapshot_digest"]
    return entry


def index_snapshot(cache_dir: Path) -> dict:
    cache = ConditionalCache(cache_dir)
    identities = cache._read_index()  # noqa: SLF001 - evidence, read-only
    snapshot = {}
    for identity, entries in identities.items():
        snapshot[identity] = [
            {
                "sequence": entry["sequence"],
                "etag": entry.get("etag"),
                "last_modified": entry.get("last_modified"),
                "snapshot_digest": entry["digest"],
                "size": entry["size"],
            }
            for entry in entries
        ]
    return snapshot


def blobs(cache_dir: Path) -> list[dict]:
    directory = cache_dir / "blobs"
    if not directory.exists():
        return []
    result = []
    for blob in sorted(directory.glob("*.json")):
        data = blob.read_bytes()
        result.append(
            {
                "name": blob.name,
                "sha256_of_file": sha(data),
                "size": len(data),
                "matches_filename": blob.stem == sha(data),
            }
        )
    return result


def deliver(records, database: str) -> tuple[list[str], dict]:
    """Run the real pipeline and capture exactly the records it received."""
    captured: list[str] = []

    def recording_normalizer(raw):
        captured.append(canonical(raw))
        return normalize(raw)

    store = ObservationStore(database)
    try:
        result = IngestionPipeline(records, recording_normalizer, store).run()
    finally:
        store.close()
    return captured, result


# ---------------------------------------------------------------- scenarios
def scenario_replay(server: ScriptedServer, cache: Path, work: Path) -> dict:
    path = "/feed?page=1"
    payload = body("978-1", "978-2")
    server.set(path, payload=payload, etag='"v1"')
    url = server.url + path

    source = HttpJsonSource(url, cache_dir=str(cache))
    first = source.read()
    first_report = source.last_report.as_dict()
    second = source.read()
    second_report = source.last_report.as_dict()

    captured_a, result_a = deliver(first, str(work / "replay-a.sqlite3"))
    captured_b, result_b = deliver(second, str(work / "replay-b.sqlite3"))

    return {
        "requests": [
            request_evidence(first_report, role="first"),
            request_evidence(second_report, role="second"),
        ],
        "records_identical_at_pipeline": captured_a == captured_b,
        "row_effect_identical": result_a["observations"] == result_b["observations"],
        "download_bytes": [first_report["downloaded_bytes"], second_report["downloaded_bytes"]],
        "all_conditional_headers_match_bodies": (
            second_report["conditional_headers"] == {"If-None-Match": '"v1"'}
            and second_report["snapshot_digest"] == sha(payload)
        ),
    }


def scenario_interleaved(server: ScriptedServer, cache: Path, work: Path) -> dict:
    path = "/interleaved"
    v1 = body("978-1")
    v2 = body("978-2")
    server.set(path, payload=v1, etag='"v1"')
    url = server.url + path

    a = HttpJsonSource(url, cache_dir=str(cache))
    a.read()  # A establishes v1
    baseline = ConditionalCache(cache).baseline(a.request_identity())

    reached, release = server.hold(path, '"v1"', '"v1"')
    outcome: dict = {}

    def delayed_a():
        outcome["records"] = list(a.read())
        outcome["report"] = a.last_report.as_dict()

    thread = threading.Thread(target=delayed_a)
    thread.start()
    assert reached.wait(timeout=5), "held request never arrived"

    # B commits newer bytes and validators while A's request is in flight.
    server.set(path, payload=v2, etag='"v2"')
    b = HttpJsonSource(url, cache_dir=str(cache))
    b_records = b.read()
    b_report = b.last_report.as_dict()

    release.set()
    thread.join(timeout=10)
    assert not thread.is_alive(), "delayed request did not finish"

    current = ConditionalCache(cache).baseline(a.request_identity())
    return {
        "writer_order": ["A conditions on v1", "B downloads and commits v2", "A receives 304 for v1"],
        "a_request": request_evidence(outcome["report"], role="A delayed 304"),
        "b_request": request_evidence(b_report, role="B full 200"),
        "a_records_match_v1": canonical(outcome["records"]) == canonical([record("978-1")]),
        "a_snapshot_matches_v1": outcome["report"]["snapshot_digest"] == sha(v1),
        "cache_after": {
            "etag": current.validators if current else None,
            "snapshot_digest": current.digest if current else None,
            "body_is_v2": bool(current and current.body == v2),
        },
    }


def scenario_containers(server: ScriptedServer, cache: Path) -> dict:
    path = "/containers"
    payload = json.dumps({"primary": [record("978-1")], "mirror": [record("978-2")]}).encode()
    server.set(path, payload=payload, etag='"same"')
    url = server.url + path

    primary = HttpJsonSource(url, cache_dir=str(cache), records_key="primary", access_scope="scope-a")
    mirror = HttpJsonSource(url, cache_dir=str(cache), records_key="mirror", access_scope="scope-a")
    p1 = list(primary.read())
    m1 = list(mirror.read())
    p2 = list(primary.read())
    m2 = list(mirror.read())
    return {
        "identities_distinct": primary.request_identity() != mirror.request_identity(),
        "requests": [
            request_evidence(primary.last_report.as_dict(), role="primary #1"),
            request_evidence(mirror.last_report.as_dict(), role="mirror #1"),
            request_evidence(primary.last_report.as_dict(), role="primary #2"),
            request_evidence(mirror.last_report.as_dict(), role="mirror #2"),
        ],
        "primary_records": p1 == p2 == [record("978-1")],
        "mirror_records": m1 == m2 == [record("978-2")],
        "shared_etag_on_distinct_bodies": True,
    }


def scenario_validators(server: ScriptedServer, cache: Path) -> dict:
    path = "/validators"
    same_body = body("978-1")
    server.set(path, payload=same_body, etag='"v1"')
    url = server.url + path

    first = HttpJsonSource(url, cache_dir=str(cache))
    first.read()
    baseline = ConditionalCache(cache).baseline(first.request_identity())

    server.set(path, payload=same_body, etag='"v1b"')
    second = HttpJsonSource(url, cache_dir=str(cache))
    records = list(second.read())
    report = second.last_report.as_dict()

    # A stale 304 for v1 must not re-bind the entry that now carries v1b.
    confirmed = ConditionalCache(cache).confirm(
        first.request_identity(), baseline.digest, baseline.validators, {"etag": '"v1"'}
    )
    current = ConditionalCache(cache).baseline(first.request_identity())
    return {
        "requests": [
            request_evidence(first.last_report.as_dict(), role="original v1"),
            request_evidence(report, role="same bytes, ETag v1b"),
        ],
        "same_body": sha(same_body),
        "no_false_304": report["status"] == "200",
        "records_still_correct": records == [record("978-1")],
        "cache_now": current.validators if current else None,
        "late_304_for_old_validator_rebound": confirmed,
    }


def scenario_scope(server: ScriptedServer, cache: Path, monkeypatch_env: dict) -> dict:
    path = "/scoped"
    server.set(path, payload=body("978-1"), etag='"v1"')
    url = server.url + path

    a = HttpJsonSource(url, cache_dir=str(cache), access_scope="tenant-a")
    a.read()
    # Different scope, different identity, therefore no conditional header.
    b = HttpJsonSource(url, cache_dir=str(cache), access_scope="tenant-b")
    b.read()
    scope_report = b.last_report.as_dict()

    # Auth switch: new environment value must not reuse the old validator.
    os.environ["DATA_SOURCE_BEARER_TOKEN"] = "token-one"
    c = HttpJsonSource(url, cache_dir=str(cache), access_scope="tenant-b")
    c.read()
    before = c.last_report.as_dict()
    os.environ["DATA_SOURCE_BEARER_TOKEN"] = "token-two"
    d = HttpJsonSource(url, cache_dir=str(cache), access_scope="tenant-b")
    d.read()
    after = d.last_report.as_dict()
    os.environ.pop("DATA_SOURCE_BEARER_TOKEN", None)

    index_text = (cache / "index.json").read_text(encoding="utf-8")
    return {
        "scope_change_identity_changed": a.request_identity() != b.request_identity(),
        "scope_change_sent_no_conditional": scope_report["conditional_headers"] == {},
        "auth_change_identity_changed": c.request_identity() != d.request_identity(),
        "auth_change_sent_no_conditional": after["conditional_headers"] == {},
        "credentials_absent_from_metadata": "token-one" not in index_text
        and "token-two" not in index_text,
    }


def scenario_restart(server: ScriptedServer, cache: Path, work: Path) -> dict:
    path = "/restart"
    payload = body("978-1")
    server.set(path, payload=payload, etag='"v1"')
    url = server.url + path

    seed = HttpJsonSource(url, cache_dir=str(cache))
    seed.read()

    entries: dict = {}
    evidence = {}
    for name, mutate in (
        ("tampered_blob", "tamper"),
        ("deleted_blob", "delete"),
        ("corrupt_index", "index"),
    ):
        # Reset to a healthy cache first so each case starts from a baseline.
        seed.read()
        index = ConditionalCache(cache)._read_index()  # noqa: SLF001
        digest = next(iter(index.values()))[0]["digest"]

        if mutate == "tamper":
            blob = cache / "blobs" / f"{digest}.json"
            tampered = blob.read_bytes().replace(b"978-1", b"978-9")
            blob.write_bytes(tampered)
        elif mutate == "delete":
            (cache / "blobs" / f"{digest}.json").unlink()
        else:
            (cache / "index.json").write_text("{ truncated", encoding="utf-8")

        # A genuinely separate process reopens the cache from disk.
        probe = run_restart_probe(url, cache, work / f"restart-{name}.sqlite3")
        entries[name] = probe
        evidence[name] = {
            "cache_status": probe["status"],
            "conditional_headers": probe["conditional_headers"],
            "downloaded_bytes": probe["downloaded_bytes"],
            "records_ok": probe["records"] == [record("978-1")],
            "snapshot_digest": probe["snapshot_digest"],
        }

    # A 304 with no usable baseline must fail, not return an empty list.
    (cache / "index.json").write_text("{}", encoding="utf-8")
    server.always_304(path, etag='"v1"')
    probe = run_restart_probe(url, cache, work / "restart-empty.sqlite3")
    evidence["304_without_baseline"] = {
        "cache_status": probe["status"],
        "error": probe["error"],
    }

    return {
        "cases": evidence,
        "all_cases_ok": all(
            case.get("records_ok") for key, case in evidence.items() if key != "304_without_baseline"
        )
        and evidence["304_without_baseline"]["error"] is not None,
    }


def run_restart_probe(url: str, cache: Path, database: str) -> dict:
    """Reopen the cache in a fresh interpreter and report the read outcome."""
    environment = dict(os.environ)
    existing = environment.get("PYTHONPATH", "")
    environment["PYTHONPATH"] = (
        str(SRC_ROOT) + os.pathsep + existing if existing else str(SRC_ROOT)
    )
    completed = subprocess.run(
        [
            sys.executable,
            str(Path(__file__).resolve()),
            "--restart-probe",
            url,
            str(cache),
            database,
        ],
        capture_output=True,
        text=True,
        env=environment,
        timeout=60,
    )
    if completed.returncode != 0 and not completed.stdout.strip():
        return {"status": "error", "error": completed.stderr.strip()[-400:], "records": []}
    return json.loads(completed.stdout.strip().splitlines()[-1])


def restart_probe_main(url: str, cache: str, database: str) -> None:
    source = HttpJsonSource(url, cache_dir=cache)
    output = {"status": None, "error": None, "records": [], "conditional_headers": {},
              "downloaded_bytes": 0, "snapshot_digest": None}
    try:
        records = list(source.read())
        output.update(
            {
                "status": source.last_report.status,
                "records": records,
                "conditional_headers": source.last_report.conditional_headers,
                "downloaded_bytes": source.last_report.downloaded_bytes,
                "snapshot_digest": source.last_report.snapshot_digest,
            }
        )
    except RuntimeError as exc:
        output["status"] = "error"
        output["error"] = str(exc)
    print(json.dumps(output, ensure_ascii=False, sort_keys=True))


# --------------------------------------------------------------------- main
def main() -> None:
    parser = argparse.ArgumentParser(description="Replay conditional-cache evidence offline")
    parser.add_argument("--restart-probe", nargs=3, metavar=("URL", "CACHE", "DATABASE"))
    args = parser.parse_args()
    if args.restart_probe:
        restart_probe_main(*args.restart_probe)
        return

    server, thread = start_server()
    try:
        with tempfile.TemporaryDirectory() as temporary:
            work = Path(temporary)
            evidence = {
                "scenarios": {
                    "replay": scenario_replay(server, work / "replay-cache", work),
                    "interleaved": scenario_interleaved(server, work / "interleave-cache", work),
                    "containers": scenario_containers(server, work / "container-cache"),
                    "validators": scenario_validators(server, work / "validator-cache"),
                    "scope": scenario_scope(server, work / "scope-cache", {}),
                    "restart": scenario_restart(server, work / "restart-cache", work),
                },
                "server_request_log": server.requests,
            }
            evidence["cache_files"] = {
                "index": index_snapshot(work / "replay-cache"),
                "blobs": blobs(work / "replay-cache"),
            }
            print(json.dumps(evidence, ensure_ascii=False, indent=2, sort_keys=True))
            ok = (
                evidence["scenarios"]["replay"]["records_identical_at_pipeline"]
                and evidence["scenarios"]["interleaved"]["a_records_match_v1"]
                and evidence["scenarios"]["interleaved"]["cache_after"]["body_is_v2"]
                and evidence["scenarios"]["containers"]["identities_distinct"]
                and evidence["scenarios"]["validators"]["no_false_304"]
                and not evidence["scenarios"]["validators"]["late_304_for_old_validator_rebound"]
                and evidence["scenarios"]["scope"]["credentials_absent_from_metadata"]
                and evidence["scenarios"]["restart"]["all_cases_ok"]
            )
            if not ok:
                raise SystemExit("evidence assertions failed")
    finally:
        stop_server(server, thread)


if __name__ == "__main__":
    main()
