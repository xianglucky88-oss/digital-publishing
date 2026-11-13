from __future__ import annotations

import hashlib
import http.client
import json
import os
import shutil
import tempfile
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator

from .http_cache import BaselineRef, CachedBaseline, ConditionalCache
from .json_stream import ContainerInfo, StreamLimits, analyze_json, iter_records

DEFAULT_CHUNK_SIZE = 64 * 1024
DEFAULT_MAX_BYTES = 64 * 1024 * 1024
DEFAULT_MAX_RECORD_CHARS = 1_000_000
DEFAULT_MAX_DEPTH = 64


class FixtureSource:
    """只负责从本地 fixture 读取原始记录，不做业务解析或持久化。"""

    def read(self, path: str) -> list[Any]:
        value = json.loads(Path(path).read_text(encoding="utf-8"))
        if not isinstance(value, list):
            raise ValueError("fixture must contain a JSON list")
        return value


@dataclass(frozen=True)
class HttpReadReport:
    """一次来源读取的可核对证据：命中哪份正文、付出多少下载字节。"""

    url: str
    identity: str
    status: str
    record_count: int
    snapshot_digest: str
    snapshot_bytes: int
    downloaded_bytes: int
    conditional_headers: dict[str, str] = field(default_factory=dict)
    response_headers: dict[str, str] = field(default_factory=dict)
    cache_hit: bool = False

    def as_dict(self) -> dict[str, Any]:
        return {
            "url": self.url,
            "identity": self.identity,
            "status": self.status,
            "record_count": self.record_count,
            "snapshot_digest": self.snapshot_digest,
            "snapshot_bytes": self.snapshot_bytes,
            "downloaded_bytes": self.downloaded_bytes,
            "conditional_headers": dict(self.conditional_headers),
            "response_headers": dict(self.response_headers),
            "cache_hit": self.cache_hit,
        }


@dataclass(frozen=True)
class _StagedBody:
    """A fully downloaded response body held in a temporary file."""

    path: Path
    digest: str  # sha256 of the body bytes (snapshot identity)
    raw_name: str  # sha256 of url + NUL + body (raw_dir file name)
    size: int


def _read_chunks(path: Path, chunk_size: int) -> Iterator[bytes]:
    with open(path, "rb") as handle:
        while True:
            chunk = handle.read(chunk_size)
            if not chunk:
                return
            yield chunk


class RecordStream:
    """Single-pass, lazily parsed record view over a validated snapshot file.

    Iterating materializes one record at a time, so peak memory stays
    bounded by the record size and parse depth regardless of how many
    records the snapshot holds.  The snapshot file is opened on first
    iteration and released — together with any temporary staging file —
    when iteration completes, when :meth:`close` is called, or when the
    stream is garbage-collected.  A stream that was never iterated still
    releases its staging file on close.
    """

    def __init__(
        self,
        path: Path,
        info: ContainerInfo,
        limits: StreamLimits,
        chunk_size: int,
        cleanup_path: Path | None = None,
    ) -> None:
        self._path = path
        self._info = info
        self._limits = limits
        self._chunk_size = chunk_size
        self._cleanup_path = cleanup_path
        self._generator: Iterator[Any] | None = None

    def __iter__(self) -> Iterator[Any]:
        if self._generator is None:
            self._generator = self._consume()
        return self._generator

    def _consume(self) -> Iterator[Any]:
        try:
            yield from iter_records(
                _read_chunks(self._path, self._chunk_size), self._info, self._limits
            )
        finally:
            self._release()

    def _release(self) -> None:
        if self._cleanup_path is not None:
            try:
                self._cleanup_path.unlink()
            except OSError:
                pass
            except Exception:  # pragma: no cover - interpreter teardown in __del__
                pass
            self._cleanup_path = None

    def close(self) -> None:
        generator, self._generator = self._generator, None
        if generator is not None:
            generator.close()
        self._release()

    def __enter__(self) -> "RecordStream":
        return self

    def __exit__(self, *_exc: object) -> bool:
        self.close()
        return False

    def __del__(self) -> None:  # pragma: no cover - safety net for abandoned streams
        self.close()


class HttpJsonSource:
    """通过标准库 HTTP 客户端分块读取 JSON，并将原始响应落盘。

    The response is consumed in bounded chunks and staged to a temporary
    file (``max_bytes`` enforced while downloading).  The staged bytes are
    then validated in a single streaming pass — UTF-8, JSON grammar,
    container shape and a complete ending — before anything is published:
    only after full validation does the snapshot become the raw/cache
    snapshot and does :attr:`last_report` describe it.  Records are
    materialized one at a time from the validated snapshot when the
    returned :class:`RecordStream` is iterated, so peak memory is bounded
    by ``chunk_size``, one record (``max_record_chars``) and ``max_depth``
    — never by the number of records or by skipped metadata fields.

    当提供 ``cache_dir`` 时，服务端给出的 ``ETag`` / ``Last-Modified`` 会被
    持久化，并在下一次请求中以条件头回放。命中的缓存条目始终绑定到一份
    完整保存且已通过 JSON 容器校验的字节快照；响应缺少验证符时退回完整读取。
    """

    def __init__(
        self,
        url: str,
        *,
        timeout: float = 30.0,
        raw_dir: str | None = None,
        records_key: str | None = None,
        cache_dir: str | None = None,
        access_scope: str | None = None,
        bearer_token_env: str = "DATA_SOURCE_BEARER_TOKEN",
        api_key_env: str = "DATA_SOURCE_API_KEY",
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        max_bytes: int = DEFAULT_MAX_BYTES,
        max_record_chars: int = DEFAULT_MAX_RECORD_CHARS,
        max_depth: int = DEFAULT_MAX_DEPTH,
        staging_dir: str | None = None,
    ) -> None:
        if not url.startswith(("http://", "https://")):
            raise ValueError("url must use http:// or https://")
        if timeout <= 0:
            raise ValueError("timeout must be positive")
        if chunk_size <= 0:
            raise ValueError("chunk_size must be positive")
        if max_bytes <= 0:
            raise ValueError("max_bytes must be positive")
        if max_record_chars <= 0:
            raise ValueError("max_record_chars must be positive")
        if max_depth <= 0:
            raise ValueError("max_depth must be positive")
        self.url = url
        self.timeout = timeout
        self.raw_dir = Path(raw_dir) if raw_dir else None
        self.records_key = records_key
        self.access_scope = access_scope or ""
        self.bearer_token_env = bearer_token_env
        self.api_key_env = api_key_env
        self.cache = ConditionalCache(cache_dir) if cache_dir else None
        self.chunk_size = chunk_size
        self.max_bytes = max_bytes
        self.limits = StreamLimits(max_depth=max_depth, max_record_chars=max_record_chars)
        self.staging_dir = Path(staging_dir) if staging_dir else None
        # Snapshot the effective credentials once.  The identity must describe
        # the headers actually sent, and a later environment change has to
        # produce a new source (and therefore a new identity) rather than
        # silently re-labelling the credentials of an existing one.
        self._auth_snapshot = self._auth_headers()
        self.last_report: HttpReadReport | None = None

    # ------------------------------------------------------------------ headers
    def _auth_headers(self) -> dict[str, str]:
        auth: dict[str, str] = {}
        token = os.getenv(self.bearer_token_env)
        if token:
            auth["Authorization"] = f"Bearer {token}"
        api_key = os.getenv(self.api_key_env)
        if api_key:
            auth["X-API-Key"] = api_key
        return auth

    def _headers(self) -> dict[str, str]:
        headers = {
            "Accept": "application/json",
            "User-Agent": "data-ingestion-adapter/0.1",
        }
        # Use the snapshot so the headers actually sent are the ones the
        # request identity describes.
        headers.update(self._auth_snapshot)
        return headers

    def request_identity(self) -> str:
        """One-way identity over URL, query semantics, record view and scope.

        ``records_key`` and the caller-declared scope are part of the identity,
        so a different container view or a different access scope never reuses
        another identity's validators.  Credentials contribute only a one-way
        fingerprint of the *effective* headers, never their raw values, and a
        change of authentication configuration therefore yields a new identity.
        """
        auth_fingerprint = hashlib.sha256(
            json.dumps(
                sorted(
                    (name, hashlib.sha256(value.encode("utf-8")).hexdigest())
                    for name, value in self._auth_snapshot.items()
                ),
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()
        identity_input = {
            "kind": "http-json",
            "url": self.url,
            "records_key": self.records_key,
            "access_scope": self.access_scope,
            "auth": auth_fingerprint,
        }
        return hashlib.sha256(
            json.dumps(identity_input, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()

    def _conditional_headers(self, baseline: BaselineRef | CachedBaseline | None) -> dict[str, str]:
        if baseline is None:
            return {}
        headers: dict[str, str] = {}
        if "etag" in baseline.validators:
            headers["If-None-Match"] = baseline.validators["etag"]
        if "last_modified" in baseline.validators:
            headers["If-Modified-Since"] = baseline.validators["last_modified"]
        return headers

    # ------------------------------------------------------------------ staging
    def _stage_response(self, response: Any) -> _StagedBody:
        """Stream the response body to a temporary file in bounded chunks.

        ``max_bytes`` is enforced while downloading; a transport failure or
        a byte-limit violation discards the partial file and leaves no
        observable state behind.
        """
        staging_dir = self.raw_dir or self.staging_dir
        if staging_dir is not None:
            staging_dir.mkdir(parents=True, exist_ok=True)
        descriptor, name = tempfile.mkstemp(prefix=".staging-", dir=staging_dir)
        declared = response.headers.get("Content-Length")
        hasher = hashlib.sha256()
        raw_hasher = hashlib.sha256(self.url.encode("utf-8") + b"\0")
        total = 0
        try:
            with os.fdopen(descriptor, "wb") as handle:
                while True:
                    try:
                        chunk = response.read(self.chunk_size)
                    except (http.client.HTTPException, OSError) as exc:
                        raise RuntimeError(f"source request failed: {exc}") from exc
                    if not chunk:
                        break
                    total += len(chunk)
                    if total > self.max_bytes:
                        raise ValueError(
                            f"source response exceeds byte limit of {self.max_bytes}"
                        )
                    hasher.update(chunk)
                    raw_hasher.update(chunk)
                    handle.write(chunk)
                handle.flush()
                os.fsync(handle.fileno())
            if declared is not None:
                try:
                    expected = int(declared)
                except ValueError:
                    expected = -1
                if expected != total:
                    raise RuntimeError(
                        f"source response truncated: expected {expected} bytes, received {total}"
                    )
        except BaseException:
            try:
                os.unlink(name)
            except OSError:
                pass
            raise
        return _StagedBody(Path(name), hasher.hexdigest(), raw_hasher.hexdigest(), total)

    # ------------------------------------------------------------------ raw copies
    def _save_raw_file(self, staged: _StagedBody) -> Path:
        """Publish staged bytes as an immutable raw snapshot; return its path."""
        assert self.raw_dir is not None
        target = self.raw_dir / f"{staged.raw_name}.json"
        if target.exists():
            staged.path.unlink(missing_ok=True)
            return target
        os.replace(staged.path, target)  # staged in the same directory: atomic
        return target

    def _save_raw_copy(self, source_path: Path) -> None:
        """Land a copy of an already-validated snapshot in ``raw_dir``."""
        if self.raw_dir is None:
            return
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        hasher = hashlib.sha256(self.url.encode("utf-8") + b"\0")
        with open(source_path, "rb") as handle:
            for chunk in iter(lambda: handle.read(self.chunk_size), b""):
                hasher.update(chunk)
        target = self.raw_dir / f"{hasher.hexdigest()}.json"
        if target.exists():
            return
        try:
            os.link(source_path, target)
        except FileExistsError:
            return
        except OSError:
            # Cross-device or unsupported link: fall back to an atomic copy.
            descriptor, temporary = tempfile.mkstemp(
                dir=self.raw_dir, prefix=".raw-", suffix=".tmp"
            )
            try:
                with os.fdopen(descriptor, "wb") as out, open(source_path, "rb") as src:
                    shutil.copyfileobj(src, out, self.chunk_size)
                    out.flush()
                    os.fsync(out.fileno())
                os.replace(temporary, target)
            finally:
                if os.path.exists(temporary):
                    os.unlink(temporary)

    # ------------------------------------------------------------------ helpers
    @staticmethod
    def _response_validators(headers: Any) -> dict[str, str]:
        validators: dict[str, str] = {}
        etag = headers.get("ETag")
        if etag:
            validators["etag"] = etag.strip()
        last_modified = headers.get("Last-Modified")
        if last_modified:
            validators["last_modified"] = last_modified.strip()
        return validators

    # ------------------------------------------------------------------ read
    def read(self, _path: str | None = None) -> RecordStream:
        """Fetch, validate and publish the source; return its record stream.

        The download, full-document validation and snapshot publication all
        happen before this returns — an exception here means no record of
        this read was ever exposed.  The returned stream then materializes
        the validated records one at a time; callers that stop early should
        ``close()`` it (or use it as a context manager) to release the
        staging file deterministically.
        """
        identity = self.request_identity()
        baseline = self.cache.baseline_ref(identity) if self.cache is not None else None
        conditional = self._conditional_headers(baseline)
        request = urllib.request.Request(
            self.url, headers={**self._headers(), **conditional}, method="GET"
        )

        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                status = response.status
                response_headers = dict(response.headers.items())
                stream = self._use_download(identity, response, response_headers, conditional)
        except urllib.error.HTTPError as exc:
            if exc.code == 304:
                response_headers = dict(exc.headers.items()) if exc.headers else {}
                exc.close()
                return self._use_not_modified(identity, baseline, conditional, response_headers)
            raise RuntimeError(f"source returned HTTP {exc.code}") from exc
        except urllib.error.URLError as exc:
            raise RuntimeError(f"source request failed: {exc.reason}") from exc

        if status != 200:
            raise RuntimeError(f"source returned HTTP {status}")
        return stream

    def _use_download(
        self,
        identity: str,
        response: Any,
        response_headers: dict[str, str],
        conditional: dict[str, str],
    ) -> RecordStream:
        staged = self._stage_response(response)
        if self.raw_dir is not None:
            # Preserve the original bytes regardless of later validation,
            # matching the previous raw_dir behaviour.  Cacheability is
            # decided below, and only container-validated bytes may become
            # a baseline.
            snapshot_path = self._save_raw_file(staged)
            cleanup: Path | None = None
        else:
            snapshot_path = staged.path
            cleanup = staged.path
        try:
            # Gate: the whole document must validate before anything derived
            # from it is published or returned.
            info = analyze_json(
                _read_chunks(snapshot_path, self.chunk_size), self.records_key, self.limits
            )
            validators = self._response_validators(response_headers)
            if self.cache is not None:
                if validators:
                    self.cache.record_file(identity, validators, snapshot_path)
                else:
                    # The endpoint stopped supplying validators: never keep
                    # replaying old ones for it.
                    self.cache.invalidate(identity)
        except BaseException:
            if cleanup is not None:
                try:
                    cleanup.unlink()
                except OSError:
                    pass
            raise
        self.last_report = HttpReadReport(
            url=self.url,
            identity=identity,
            status="200",
            record_count=info.record_count,
            snapshot_digest=staged.digest,
            snapshot_bytes=staged.size,
            downloaded_bytes=staged.size,
            conditional_headers=dict(conditional),
            response_headers=response_headers,
            cache_hit=False,
        )
        return RecordStream(snapshot_path, info, self.limits, self.chunk_size, cleanup_path=cleanup)

    def _use_not_modified(
        self,
        identity: str,
        baseline: BaselineRef | None,
        conditional: dict[str, str],
        response_headers: dict[str, str],
    ) -> RecordStream:
        if baseline is None:
            # We never send a conditional header without a verified baseline,
            # so a 304 here means the server answered a request we cannot
            # reconstruct.  Fail clearly instead of fabricating records.
            raise RuntimeError("source returned HTTP 304 without a usable cached baseline")
        # The 304 validates exactly the snapshot the conditional request was
        # derived from.  Re-verify its integrity (streamed, bounded memory)
        # and re-validate its structure before any record is replayed.
        info = analyze_json(
            _read_chunks(baseline.path, self.chunk_size),
            self.records_key,
            self.limits,
            expected_digest=baseline.digest,
            expected_size=baseline.size,
        )
        validators = self._response_validators(response_headers)
        if self.cache is not None and validators:
            self.cache.confirm(identity, baseline.digest, baseline.validators, validators)
        self._save_raw_copy(baseline.path)
        self.last_report = HttpReadReport(
            url=self.url,
            identity=identity,
            status="304",
            record_count=info.record_count,
            snapshot_digest=baseline.digest,
            snapshot_bytes=baseline.size,
            downloaded_bytes=0,
            conditional_headers=dict(conditional),
            response_headers=response_headers,
            cache_hit=True,
        )
        return RecordStream(baseline.path, info, self.limits, self.chunk_size)
