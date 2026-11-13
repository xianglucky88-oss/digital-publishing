import hashlib
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

from digital_publishing.application import run_url
from digital_publishing.http_cache import ConditionalCache
from digital_publishing.sources import HttpJsonSource


def _record(isbn: str, *, source: str = "http", observed_at: str = "2026-11-01T06:00:00Z"):
    return {
        "source": source,
        "observed_at": observed_at,
        "payload": {
            "isbn": isbn,
            "title": f"Title {isbn}",
            "publisher": "Example Press",
            "edition": "1",
            "language": "en",
            "availability": "active",
        },
    }


def _body(*isbns: str) -> bytes:
    return json.dumps({"records": [_record(isbn) for isbn in isbns]}).encode("utf-8")


class _Endpoint(BaseHTTPRequestHandler):
    """Scriptable offline endpoint that records every request it receives."""

    def do_GET(self):  # noqa: N802 - BaseHTTPRequestHandler API
        server: _Server = self.server  # type: ignore[assignment]
        if_none_match = self.headers.get("If-None-Match")
        if_modified_since = self.headers.get("If-Modified-Since")
        server.note_request(
            path=self.path,
            if_none_match=if_none_match,
            if_modified_since=if_modified_since,
            authorization=self.headers.get("Authorization"),
        )

        # "Arrange an offline response" hook: a registered request can be held
        # open until the test releases it, so a second writer can interleave.
        gate = server.gate_for(self.path, if_none_match)
        if gate is not None:
            reached, release, response_etag = gate
            reached.set()
            if not release.wait(timeout=10):
                self.send_error(500)
                return
            self.send_response(304)
            self.send_header("ETag", response_etag)
            self.end_headers()
            server.note_status(304)
            return

        with server.lock:
            entry = server.state.get(self.path)
        if entry is None:
            self.send_error(404)
            return
        if entry.get("always_304"):
            self.send_response(304)
            self.send_header("ETag", entry.get("etag") or '"x"')
            self.end_headers()
            server.note_status(304)
            return

        etag = entry.get("etag")
        last_modified = entry.get("last_modified")
        if etag and if_none_match == etag:
            self.send_response(304)
            self.send_header("ETag", etag)
            self.end_headers()
            server.note_status(304)
            return
        if not etag and last_modified and if_modified_since == last_modified:
            self.send_response(304)
            self.send_header("Last-Modified", last_modified)
            self.end_headers()
            server.note_status(304)
            return
        body = entry["body"]
        self.send_response(200)
        if etag:
            self.send_header("ETag", etag)
        if last_modified:
            self.send_header("Last-Modified", last_modified)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
        server.note_status(200)

    def log_message(self, *_args):
        return


class _Server(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.state: dict[str, dict] = {}
        self.requests: list[dict] = []
        self.statuses: list[int] = []
        self.lock = threading.Lock()
        self._gates: dict[str, tuple[threading.Event, threading.Event, str]] = {}

    def note_request(self, **fields):
        with self.lock:
            self.requests.append(fields)

    def note_status(self, status: int):
        with self.lock:
            self.statuses.append(status)

    def set(self, path: str, *, body: bytes, etag: str | None = None, last_modified: str | None = None):
        with self.lock:
            self.state[path] = {"body": body, "etag": etag, "last_modified": last_modified}

    def always_304(self, path: str, *, etag: str = '"x"'):
        with self.lock:
            self.state[path] = {"body": b"", "etag": etag, "always_304": True}

    def hold(self, path: str, if_none_match: str, response_etag: str):
        """Hold the next matching conditional request until ``release`` is set."""
        reached = threading.Event()
        release = threading.Event()
        with self.lock:
            self._gates[path] = (if_none_match, reached, release, response_etag)
        return reached, release

    def gate_for(self, path: str, if_none_match: str | None):
        with self.lock:
            gate = self._gates.get(path)
            if gate is None or gate[0] != if_none_match:
                return None
            # One-shot: consume now so only the first conditional request is
            # held; later requests (such as writer B's refresh) go through.
            self._gates.pop(path, None)
            return gate[1:]

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.server_port}"


@pytest.fixture
def server():
    srv = _Server(("127.0.0.1", 0), _Endpoint)
    thread = threading.Thread(target=srv.serve_forever, daemon=True)
    thread.start()
    try:
        yield srv
    finally:
        srv.shutdown()
        thread.join(timeout=5)
        srv.server_close()


# --------------------------------------------------------------------- core flow
def test_conditional_download_replays_same_records_without_re_downloading(server, tmp_path):
    server.set("/feed?page=1", body=_body("978-1"), etag='"v1"')
    source = HttpJsonSource(f"{server.url}/feed?page=1", cache_dir=str(tmp_path / "cache"))

    first = list(source.read())
    first_report = source.last_report
    second = list(source.read())
    second_report = source.last_report

    assert first == second
    assert first_report.status == "200"
    assert first_report.downloaded_bytes == len(_body("978-1"))
    assert second_report.status == "304"
    assert second_report.downloaded_bytes == 0
    assert second_report.cache_hit is True
    assert second_report.snapshot_digest == first_report.snapshot_digest
    assert second_report.record_count == len(first)
    assert second_report.conditional_headers == {"If-None-Match": '"v1"'}
    assert server.statuses == [200, 304]


def test_last_modified_is_supported_when_no_etag(server, tmp_path):
    server.set("/feed", body=_body("978-2"), last_modified="Wed, 01 Oct 2025 06:00:00 GMT")
    source = HttpJsonSource(f"{server.url}/feed", cache_dir=str(tmp_path / "cache"))
    assert list(source.read()) == [_record("978-2")]
    assert list(source.read()) == [_record("978-2")]
    assert source.last_report.status == "304"
    assert source.last_report.conditional_headers == {
        "If-Modified-Since": "Wed, 01 Oct 2025 06:00:00 GMT"
    }


def test_endpoint_without_validators_keeps_full_reads(server, tmp_path):
    server.set("/feed", body=_body("978-3"))
    source = HttpJsonSource(f"{server.url}/feed", cache_dir=str(tmp_path / "cache"))
    source.read()
    source.read()
    assert server.statuses == [200, 200]
    assert not (tmp_path / "cache" / "index.json").exists()
    assert source.last_report.downloaded_bytes == len(_body("978-3"))


def test_records_reach_the_pipeline_identically_when_not_modified(server, tmp_path):
    server.set("/feed", body=_body("978-1", "978-2"), etag='"v1"')
    database = str(tmp_path / "records.sqlite3")
    reports: list[dict] = []

    first = run_url(f"{server.url}/feed", database, cache_dir=str(tmp_path / "cache"), report=reports)
    second = run_url(f"{server.url}/feed", database, cache_dir=str(tmp_path / "cache"), report=reports)

    assert first == {
        "processed": 2,
        "changed": 2,
        "rejected": 0,
        "observations": 2,
    }
    # The second delivery is identical record-wise but changes no rows.
    assert {key: second[key] for key in ("processed", "rejected", "observations")} == {
        "processed": 2,
        "rejected": 0,
        "observations": 2,
    }
    assert second["changed"] == 0
    assert [report["status"] for report in reports] == ["200", "304"]
    assert reports[1]["downloaded_bytes"] == 0
    assert reports[0]["snapshot_digest"] == reports[1]["snapshot_digest"]


def test_raw_dir_retains_original_bytes_on_both_paths(server, tmp_path):
    body = _body("978-1")
    server.set("/feed", body=body, etag='"v1"')
    raw_dir = tmp_path / "raw"
    source = HttpJsonSource(
        f"{server.url}/feed", cache_dir=str(tmp_path / "cache"), raw_dir=str(raw_dir)
    )
    source.read()
    source.read()
    raw_files = list(raw_dir.glob("*.json"))
    assert len(raw_files) == 1
    assert raw_files[0].read_bytes() == body


# ------------------------------------------------------------------- identity
def test_query_semantics_and_records_key_are_part_of_identity(server, tmp_path):
    server.set("/feed?page=1", body=_body("978-1"), etag='"v1"')
    server.set("/feed?page=2", body=_body("978-2"), etag='"v1"')  # same ETag, different bytes
    cache = str(tmp_path / "cache")

    page1 = HttpJsonSource(f"{server.url}/feed?page=1", cache_dir=cache)
    page2 = HttpJsonSource(f"{server.url}/feed?page=2", cache_dir=cache)
    assert page1.request_identity() != page2.request_identity()
    assert list(page1.read()) == [_record("978-1")]
    assert list(page2.read()) == [_record("978-2")]
    assert server.requests[0]["if_none_match"] is None
    assert server.requests[1]["if_none_match"] is None

    other_key = HttpJsonSource(f"{server.url}/feed?page=1", cache_dir=cache, records_key="records")
    assert other_key.request_identity() != page1.request_identity()
    other_key.read()
    assert server.requests[2]["if_none_match"] is None


def test_access_scope_change_does_not_reuse_validators(server, tmp_path):
    server.set("/feed", body=_body("978-1"), etag='"v1"')
    cache = str(tmp_path / "cache")
    tenant_a = HttpJsonSource(f"{server.url}/feed", cache_dir=cache, access_scope="tenant-a")
    tenant_b = HttpJsonSource(f"{server.url}/feed", cache_dir=cache, access_scope="tenant-b")
    assert tenant_a.request_identity() != tenant_b.request_identity()
    tenant_a.read()
    tenant_b.read()
    assert server.requests[1]["if_none_match"] is None


def test_auth_configuration_switch_does_not_reuse_validators(server, tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_SOURCE_BEARER_TOKEN", "secret-one")
    monkeypatch.setenv("DATA_SOURCE_API_KEY", "")
    server.set("/feed", body=_body("978-1"), etag='"v1"')
    cache = str(tmp_path / "cache")
    monkeypatch.setenv("DATA_SOURCE_BEARER_TOKEN", "secret-one")
    first = HttpJsonSource(f"{server.url}/feed", cache_dir=cache)
    first.read()

    monkeypatch.setenv("DATA_SOURCE_BEARER_TOKEN", "secret-two")
    second = HttpJsonSource(f"{server.url}/feed", cache_dir=cache)
    assert second.request_identity() != first.request_identity()
    second.read()
    assert server.requests[1]["if_none_match"] is None
    index = (tmp_path / "cache" / "index.json").read_text(encoding="utf-8")
    assert "secret-one" not in index and "secret-two" not in index


# ------------------------------------------------------- interleaved requests
def test_304_after_another_writer_advanced_cache_returns_validated_bytes(server, tmp_path):
    cache = str(tmp_path / "cache")
    server.set("/feed", body=_body("978-1"), etag='"v1"')
    a = HttpJsonSource(f"{server.url}/feed", cache_dir=cache)
    a.read()  # A establishes the v1 baseline
    baseline = ConditionalCache(cache).baseline(a.request_identity())
    assert baseline is not None
    assert baseline.validators == {"etag": '"v1"'}
    assert baseline.body == _body("978-1")

    # Hold A's conditional request open, then let B download v2 and commit.
    reached, release = server.hold("/feed", '"v1"', '"v1"')
    outcome: dict = {}

    def run_a():
        outcome["records"] = list(a.read())
        outcome["report"] = a.last_report

    thread = threading.Thread(target=run_a)
    thread.start()
    assert reached.wait(timeout=5)
    held_request = server.requests[-1]
    assert held_request["if_none_match"] == '"v1"'

    server.set("/feed", body=_body("978-2"), etag='"v2"')
    b = HttpJsonSource(f"{server.url}/feed", cache_dir=cache)
    assert b.request_identity() == a.request_identity()
    assert list(b.read()) == [_record("978-2")]
    assert b.last_report.status == "200"

    release.set()
    thread.join(timeout=10)
    assert not thread.is_alive()

    # A returns exactly the bytes its conditional request named.
    assert outcome["records"] == [_record("978-1")]
    assert outcome["report"].status == "304"
    assert outcome["report"].downloaded_bytes == 0
    assert outcome["report"].snapshot_digest == baseline.digest
    assert outcome["report"].conditional_headers == {"If-None-Match": '"v1"'}

    # B's newer entry survives: A's late 304 does not roll the cache back.
    current = ConditionalCache(cache).baseline(a.request_identity())
    assert current is not None
    assert current.validators == {"etag": '"v2"'}
    assert current.body == _body("978-2")


def test_304_for_stale_validator_is_not_rebound_when_validators_changed(server, tmp_path):
    cache = str(tmp_path / "cache")
    server.set("/feed", body=_body("978-1"), etag='"v1"')
    source = HttpJsonSource(f"{server.url}/feed", cache_dir=cache)
    source.read()
    baseline = ConditionalCache(cache).baseline(source.request_identity())

    # Same bytes, different validator: the entry advances without changing the
    # digest, and a late 304 for the old validator must not claim it.
    server.set("/feed", body=_body("978-1"), etag='"v1b"')
    assert list(HttpJsonSource(f"{server.url}/feed", cache_dir=cache).read()) == [_record("978-1")]

    confirmed = ConditionalCache(cache).confirm(
        source.request_identity(), baseline.digest, baseline.validators, {"etag": '"v1"'}
    )
    assert confirmed is False
    assert ConditionalCache(cache).baseline(source.request_identity()).validators == {"etag": '"v1b"'}


# ------------------------------------------------------------- crash / corrupt
def test_interrupted_write_leaves_no_usable_half_cache(server, tmp_path, monkeypatch):
    cache = str(tmp_path / "cache")
    server.set("/feed", body=_body("978-1"), etag='"v1"')

    original = ConditionalCache._atomic_write

    def fail_index_write(path, payload):
        if path.name == "index.json":
            raise OSError("simulated crash before index commit")
        return original(path, payload)

    monkeypatch.setattr(ConditionalCache, "_atomic_write", staticmethod(fail_index_write))
    with pytest.raises(OSError):
        HttpJsonSource(f"{server.url}/feed", cache_dir=cache).read()
    monkeypatch.setattr(ConditionalCache, "_atomic_write", staticmethod(original))

    assert not (Path(cache) / "index.json").exists()
    # No baseline can be derived from the half-committed state, so the next run
    # performs a full read rather than trusting unreferenced bytes.
    source = HttpJsonSource(f"{server.url}/feed", cache_dir=cache)
    assert list(source.read()) == [_record("978-1")]
    assert source.last_report.conditional_headers == {}


def test_corrupt_index_after_restart_falls_back_to_full_read(server, tmp_path):
    cache = tmp_path / "cache"
    server.set("/feed", body=_body("978-1"), etag='"v1"')
    HttpJsonSource(f"{server.url}/feed", cache_dir=str(cache)).read()
    (cache / "index.json").write_text("{ truncated", encoding="utf-8")

    source = HttpJsonSource(f"{server.url}/feed", cache_dir=str(cache))
    assert list(source.read()) == [_record("978-1")]
    assert source.last_report.status == "200"
    assert source.last_report.conditional_headers == {}


def test_corrupt_blob_after_restart_falls_back_to_full_read(server, tmp_path):
    cache = tmp_path / "cache"
    server.set("/feed", body=_body("978-1"), etag='"v1"')
    HttpJsonSource(f"{server.url}/feed", cache_dir=str(cache)).read()
    index = json.loads((cache / "index.json").read_text(encoding="utf-8"))
    digest = next(iter(index["identities"].values()))[0]["digest"]
    (cache / "blobs" / f"{digest}.json").write_bytes(b'{"records": [')

    source = HttpJsonSource(f"{server.url}/feed", cache_dir=str(cache))
    assert list(source.read()) == [_record("978-1")]
    assert source.last_report.status == "200"
    assert source.last_report.conditional_headers == {}


def test_deleted_blob_after_restart_falls_back_to_full_read(server, tmp_path):
    cache = tmp_path / "cache"
    server.set("/feed", body=_body("978-1"), etag='"v1"')
    HttpJsonSource(f"{server.url}/feed", cache_dir=str(cache)).read()
    index = json.loads((cache / "index.json").read_text(encoding="utf-8"))
    digest = next(iter(index["identities"].values()))[0]["digest"]
    (cache / "blobs" / f"{digest}.json").unlink()

    source = HttpJsonSource(f"{server.url}/feed", cache_dir=str(cache))
    assert list(source.read()) == [_record("978-1")]
    assert source.last_report.conditional_headers == {}


def test_corrupt_blob_same_size_after_restart_falls_back_to_full_read(server, tmp_path):
    cache = tmp_path / "cache"
    server.set("/feed", body=_body("978-1"), etag='"v1"')
    HttpJsonSource(f"{server.url}/feed", cache_dir=str(cache)).read()
    index = json.loads((cache / "index.json").read_text(encoding="utf-8"))
    digest = next(iter(index["identities"].values()))[0]["digest"]
    blob = cache / "blobs" / f"{digest}.json"
    tampered = blob.read_bytes().replace(b"978-1", b"978-9")
    assert len(tampered) == len(blob.read_bytes())
    blob.write_bytes(tampered)

    source = HttpJsonSource(f"{server.url}/feed", cache_dir=str(cache))
    assert list(source.read()) == [_record("978-1")]
    assert source.last_report.conditional_headers == {}


def test_304_without_usable_baseline_fails_clearly(server, tmp_path):
    cache = str(tmp_path / "cache")
    server.set("/feed", body=_body("978-1"), etag='"v1"')
    HttpJsonSource(f"{server.url}/feed", cache_dir=cache).read()
    # Simulate the index disappearing while the server answers 304 to a
    # request that carried no validator.
    (Path(cache) / "index.json").write_text("{}", encoding="utf-8")
    server.always_304("/feed", etag='"v1"')

    source = HttpJsonSource(f"{server.url}/feed", cache_dir=cache)
    with pytest.raises(RuntimeError, match="304 without a usable cached baseline"):
        source.read()


def test_confirm_updates_one_identity_without_dropping_others(server, tmp_path):
    cache = ConditionalCache(tmp_path / "cache")
    first_body, second_body = _body("978-1"), _body("978-2")
    cache.record("1" * 64, {"etag": '"a"'}, first_body)
    cache.record("2" * 64, {"etag": '"b"'}, second_body)

    assert cache.confirm(
        "1" * 64, hashlib.sha256(first_body).hexdigest(), {"etag": '"a"'}, {"etag": '"a2"'}
    )
    assert cache.baseline("1" * 64).validators == {"etag": '"a2"'}
    other = cache.baseline("2" * 64)
    assert other is not None
    assert other.validators == {"etag": '"b"'}
    assert other.body == second_body


def test_same_resource_different_container_keeps_independent_snapshots(server, tmp_path):
    payload = {"primary": [_record("978-1")], "mirror": [_record("978-2")]}
    server.set("/feed", body=json.dumps(payload).encode(), etag='"v1"')
    cache = str(tmp_path / "cache")
    primary = HttpJsonSource(
        f"{server.url}/feed", cache_dir=cache, records_key="primary", access_scope="a"
    )
    mirror = HttpJsonSource(
        f"{server.url}/feed", cache_dir=cache, records_key="mirror", access_scope="a"
    )
    assert primary.request_identity() != mirror.request_identity()
    assert list(primary.read()) == [_record("978-1")]
    assert list(mirror.read()) == [_record("978-2")]
    # Each container view still gets its own 304 replay, from its own snapshot.
    assert list(primary.read()) == [_record("978-1")]
    assert list(mirror.read()) == [_record("978-2")]
    assert primary.last_report.status == "304" and mirror.last_report.status == "304"
    assert primary.last_report.conditional_headers == {"If-None-Match": '"v1"'}
    # Both identities remain in the index; neither replay dropped the other.
    index = json.loads((Path(cache) / "index.json").read_text(encoding="utf-8"))
    assert set(index["identities"]) == {primary.request_identity(), mirror.request_identity()}


# ------------------------------------------------------------ strictness checks
def test_invalid_json_never_becomes_a_baseline(server, tmp_path):
    cache = str(tmp_path / "cache")
    raw_dir = str(tmp_path / "raw")
    server.set("/feed", body=b'{"records": [', etag='"v1"')
    with pytest.raises(ValueError, match="not valid"):
        HttpJsonSource(f"{server.url}/feed", cache_dir=cache, raw_dir=raw_dir).read()
    assert not (Path(cache) / "index.json").exists()
    # raw_dir keeps the original bytes even though they were not cacheable.
    assert list(Path(raw_dir).glob("*.json"))

    server.set("/feed", body=_body("978-1"), etag='"v1"')
    source = HttpJsonSource(f"{server.url}/feed", cache_dir=cache)
    assert list(source.read()) == [_record("978-1")]
    assert source.last_report.conditional_headers == {}


def test_metadata_never_stores_credentials_or_url(server, tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_SOURCE_BEARER_TOKEN", "bearer-secret-value")
    monkeypatch.setenv("DATA_SOURCE_API_KEY", "api-key-secret-value")
    server.set("/feed?tenant=7", body=_body("978-1"), etag='"v1"')
    HttpJsonSource(f"{server.url}/feed?tenant=7", cache_dir=str(tmp_path / "cache")).read()

    index = (tmp_path / "cache" / "index.json").read_text(encoding="utf-8")
    assert "bearer-secret-value" not in index
    assert "api-key-secret-value" not in index
    assert server.url not in index  # identity is a one-way digest
    assert "tenant=7" not in index
    blobs = list((tmp_path / "cache" / "blobs").glob("*.json"))
    assert len(blobs) == 1 and blobs[0].read_bytes() == _body("978-1")
