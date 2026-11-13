"""Persistent conditional-download cache for JSON HTTP sources.

The cache is deliberately split into two pieces:

``blobs/<sha256>.json``
    Complete, content-addressed copies of response bodies.  A blob is only
    referenced after it has been written and fsynced, so an interrupted write
    can leave an orphan blob but can never leave a half-written blob that an
    index entry points at.

``index.json``
    Metadata binding one or more HTTP validators (``ETag`` /
    ``Last-Modified``) of a request identity to the digest of a fully saved
    blob.  It is replaced atomically with ``os.replace``.  A crash between the
    blob write and the index commit therefore loses the new cache entry (it
    falls back to a full download) instead of exposing a partial one.

The index never stores the URL, the access scope, or credential values.  The
request identity is a one-way SHA-256 over those inputs, and credentials enter
that hash only as one-way fingerprints.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
import threading
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator

try:  # POSIX only; the in-process lock below still works elsewhere.
    import fcntl
except ImportError:  # pragma: no cover - not exercised on the target platform
    fcntl = None  # type: ignore[assignment]

INDEX_VERSION = 1
MAX_ENTRIES_PER_IDENTITY = 8
_DIGEST_RE = re.compile(r"^[0-9a-f]{64}$")

_thread_locks: dict[str, threading.Lock] = {}
_thread_locks_guard = threading.Lock()


def _thread_lock(path: str) -> threading.Lock:
    with _thread_locks_guard:
        return _thread_locks.setdefault(path, threading.Lock())


@dataclass(frozen=True)
class CachedBaseline:
    """A validator set together with the verified bytes it was issued for."""

    digest: str
    size: int
    validators: dict[str, str]
    body: bytes


@dataclass(frozen=True)
class BaselineRef:
    """A validator set together with the on-disk snapshot it was issued for.

    Equivalent to :class:`CachedBaseline` but references the immutable,
    content-addressed blob by path instead of loading it, so callers can
    process large snapshots in bounded memory.  The blob was verified
    (size and SHA-256, streamed) before this reference was handed out.
    """

    digest: str
    size: int
    validators: dict[str, str]
    path: Path


def _clean_validator(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    if not value or any(ch in value for ch in "\r\n"):
        return None
    return value


def _canonical_validators(validators: dict[str, str]) -> dict[str, str] | None:
    canonical: dict[str, str] = {}
    etag = _clean_validator(validators.get("etag"))
    last_modified = _clean_validator(validators.get("last_modified"))
    if etag is not None:
        canonical["etag"] = etag
    if last_modified is not None:
        canonical["last_modified"] = last_modified
    return canonical or None


def _entry_from_raw(raw: Any) -> dict[str, Any] | None:
    if not isinstance(raw, dict):
        return None
    digest = raw.get("digest")
    size = raw.get("size")
    sequence = raw.get("sequence")
    if not isinstance(digest, str) or not _DIGEST_RE.match(digest):
        return None
    if not isinstance(size, int) or isinstance(size, bool) or size < 0:
        return None
    if not isinstance(sequence, int) or isinstance(sequence, bool):
        return None
    etag = _clean_validator(raw.get("etag"))
    last_modified = _clean_validator(raw.get("last_modified"))
    if etag is None and last_modified is None:
        return None
    entry: dict[str, Any] = {"digest": digest, "size": size, "sequence": sequence}
    if etag is not None:
        entry["etag"] = etag
    if last_modified is not None:
        entry["last_modified"] = last_modified
    return entry


class ConditionalCache:
    """Atomic, content-addressed cache of validator-bound response snapshots."""

    def __init__(self, directory: str | os.PathLike[str]) -> None:
        self.root = Path(directory)

    @property
    def index_path(self) -> Path:
        return self.root / "index.json"

    @property
    def blob_dir(self) -> Path:
        return self.root / "blobs"

    def blob_path(self, digest: str) -> Path:
        return self.blob_dir / f"{digest}.json"

    # ------------------------------------------------------------------ locking
    @contextmanager
    def _locked(self) -> Iterator[None]:
        self.root.mkdir(parents=True, exist_ok=True)
        lock = _thread_lock(str(self.root.resolve()))
        with lock:
            lock_file = open(self.root / ".lock", "a+b")
            try:
                if fcntl is not None:
                    fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
                yield
            finally:
                if fcntl is not None:
                    try:
                        fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
                    except OSError:  # pragma: no cover - defensive
                        pass
                lock_file.close()

    # ------------------------------------------------------------------ index
    def _read_index(self) -> dict[str, list[dict[str, Any]]]:
        """Return the usable part of the index, or ``{}`` when it is unusable.

        A missing, truncated or otherwise corrupt index is treated as "no
        baseline": the caller performs a full download and the next commit
        replaces the corrupt file.  Nothing in an unreadable index is trusted.
        """
        try:
            value = json.loads(self.index_path.read_text(encoding="utf-8"))
        except (OSError, ValueError, UnicodeError):
            return {}
        if not isinstance(value, dict) or value.get("version") != INDEX_VERSION:
            return {}
        identities = value.get("identities")
        if not isinstance(identities, dict):
            return {}
        result: dict[str, list[dict[str, Any]]] = {}
        for identity, raw_entries in identities.items():
            if not isinstance(identity, str) or not _DIGEST_RE.match(identity):
                continue
            if not isinstance(raw_entries, list):
                continue
            entries = []
            for raw_entry in raw_entries:
                entry = _entry_from_raw(raw_entry)
                if entry is not None:
                    entries.append(entry)
            if entries:
                result[identity] = entries
        return result

    def _write_index(self, identities: dict[str, list[dict[str, Any]]]) -> None:
        payload = json.dumps(
            {"version": INDEX_VERSION, "identities": identities},
            ensure_ascii=False,
            sort_keys=True,
        ).encode("utf-8")
        ConditionalCache._atomic_write(self.index_path, payload)

    @staticmethod
    def _fsync_dir(directory: Path) -> None:
        # Make a rename durable so a crash cannot resurrect a previous
        # file after the new one was acknowledged.
        try:
            descriptor = os.open(directory, os.O_RDONLY)
        except OSError:  # pragma: no cover - non-POSIX filesystems
            return
        try:
            os.fsync(descriptor)
        except OSError:  # pragma: no cover - some filesystems refuse
            pass
        finally:
            os.close(descriptor)

    @staticmethod
    def _atomic_write(path: Path, payload: bytes) -> None:
        temporary: str | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="wb", dir=path.parent, prefix=f".{path.name}.", delete=False
            ) as handle:
                temporary = handle.name
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, path)
            temporary = None
            ConditionalCache._fsync_dir(path.parent)
        finally:
            if temporary is not None:
                try:
                    os.unlink(temporary)
                except OSError:
                    pass

    # ------------------------------------------------------------------ public
    def baseline(self, identity: str) -> CachedBaseline | None:
        """Return the newest fully verified snapshot for ``identity``.

        Returns ``None`` when there is no entry, when the referenced blob is
        missing, or when its digest/size no longer match.  In those cases the
        caller must not send a conditional request.
        """
        with self._locked():
            entries = self._read_index().get(identity)
            if not entries:
                return None
            entry = max(entries, key=lambda item: item["sequence"])
            body = self._read_verified_blob(entry["digest"], entry["size"])
            if body is None:
                return None
            validators = {
                key: entry[key]
                for key in ("etag", "last_modified")
                if isinstance(entry.get(key), str)
            }
            return CachedBaseline(entry["digest"], entry["size"], validators, body)

    def baseline_ref(self, identity: str) -> BaselineRef | None:
        """Return the newest fully verified snapshot as an on-disk reference.

        Same contract as :meth:`baseline`, but the blob is verified by
        streaming its bytes through SHA-256 instead of loading them, so a
        caller that processes records iteratively never holds the whole
        snapshot in memory.  Returns ``None`` in exactly the cases
        :meth:`baseline` does.
        """
        with self._locked():
            entries = self._read_index().get(identity)
            if not entries:
                return None
            entry = max(entries, key=lambda item: item["sequence"])
            path = self.blob_path(entry["digest"])
            if not self._verify_blob_streaming(path, entry["digest"], entry["size"]):
                return None
            validators = {
                key: entry[key]
                for key in ("etag", "last_modified")
                if isinstance(entry.get(key), str)
            }
            return BaselineRef(entry["digest"], entry["size"], validators, path)

    def record(
        self, identity: str, validators: dict[str, str], body: bytes
    ) -> dict[str, str] | None:
        """Persist ``body`` and bind ``validators`` to it atomically.

        The blob is committed first; only then is the index updated.  Returns
        the canonical validator mapping that was written, or ``None`` if no
        usable validator was supplied.
        """
        canonical = _canonical_validators(validators)
        if canonical is None:
            return None
        digest = hashlib.sha256(body).hexdigest()
        size = len(body)
        with self._locked():
            self.blob_dir.mkdir(parents=True, exist_ok=True)
            self._write_blob(digest, body)
            return self._commit_locked(identity, canonical, digest, size)

    def record_file(
        self, identity: str, validators: dict[str, str], path: str | os.PathLike[str]
    ) -> dict[str, str] | None:
        """Like :meth:`record`, but streams the snapshot from a file.

        The file is copied into the blob store in chunks while its SHA-256
        is computed, so snapshots of any size are cached in bounded memory.
        """
        canonical = _canonical_validators(validators)
        if canonical is None:
            return None
        with self._locked():
            self.blob_dir.mkdir(parents=True, exist_ok=True)
            digest, size = self._write_blob_file(Path(path))
            return self._commit_locked(identity, canonical, digest, size)

    def _commit_locked(
        self, identity: str, canonical: dict[str, str], digest: str, size: int
    ) -> dict[str, str]:
        """Bind ``canonical`` validators to an already-stored blob digest."""
        identities = self._read_index()
        entries = identities.get(identity, [])
        newest = max(entries, key=lambda item: item["sequence"]) if entries else None
        if (
            newest is not None
            and newest["digest"] == digest
            and all(newest.get(key) == value for key, value in canonical.items())
            and set(newest) - {"digest", "size", "sequence"} == set(canonical)
        ):
            return {key: newest[key] for key in canonical}
        sequence = (newest["sequence"] + 1) if newest is not None else 1
        entry = {
            "digest": digest,
            "size": size,
            "sequence": sequence,
            **canonical,
        }
        entries.append(entry)
        entries.sort(key=lambda item: item["sequence"])
        identities[identity] = entries[-MAX_ENTRIES_PER_IDENTITY:]
        self._write_index(identities)
        return dict(canonical)

    def confirm(
        self,
        identity: str,
        expected_digest: str,
        expected_validators: dict[str, str],
        response_validators: dict[str, str],
    ) -> bool:
        """Re-bind a 304 to the snapshot it was issued for, atomically.

        A ``304`` may only be trusted when the snapshot it validated still is
        the one a conditional request was derived from: the newest entry must
        carry both the expected digest and the exact validators that were sent.
        If another writer has already advanced ``identity`` (new bytes *or* a
        different validator for the same bytes), this returns ``False`` and
        leaves the newer entry untouched.  The caller still returns its own
        in-memory baseline bytes, which are exactly the bytes the server
        validated.
        """
        with self._locked():
            identities = self._read_index()
            entries = identities.get(identity)
            if not entries:
                return False
            newest = max(entries, key=lambda item: item["sequence"])
            if newest["digest"] != expected_digest:
                return False
            if any(newest.get(key) != value for key, value in expected_validators.items()):
                return False
            if self._read_verified_blob(expected_digest, newest["size"]) is None:
                return False
            canonical = {
                key: _clean_validator(response_validators.get(key))
                for key in ("etag", "last_modified")
            }
            canonical = {key: value for key, value in canonical.items() if value is not None}
            changed = any(newest.get(key) != value for key, value in canonical.items())
            if changed:
                newest.update(canonical)
                self._write_index(identities)
            return True

    def prune_orphans(self) -> int:
        """Remove blobs no live index entry references (e.g. after a crash).

        Only call this when no writer is active; it reports how many orphan
        blobs it deleted.  Orphans are otherwise harmless: they are never
        referenced, so they cannot be mistaken for a cache hit.
        """
        with self._locked():
            identities = self._read_index()
            referenced = {
                entry["digest"]
                for entries in identities.values()
                for entry in entries
            }
            removed = 0
            if not self.blob_dir.exists():
                return 0
            for blob in self.blob_dir.glob("*.json"):
                if blob.stem not in referenced:
                    try:
                        blob.unlink()
                        removed += 1
                    except OSError:
                        pass
            return removed

    def invalidate(self, identity: str) -> None:
        """Forget an identity whose server stopped supplying validators."""
        with self._locked():
            identities = self._read_index()
            if identity in identities:
                del identities[identity]
                self._write_index(identities)

    # ------------------------------------------------------------------ blobs
    @staticmethod
    def _verify_blob_streaming(path: Path, digest: str, expected_size: int) -> bool:
        """Verify a blob's size and SHA-256 without loading it into memory."""
        hasher = hashlib.sha256()
        total = 0
        try:
            with open(path, "rb") as handle:
                while True:
                    chunk = handle.read(1024 * 256)
                    if not chunk:
                        break
                    total += len(chunk)
                    if total > expected_size:
                        return False
                    hasher.update(chunk)
        except OSError:
            return False
        return total == expected_size and hasher.hexdigest() == digest

    def _read_verified_blob(self, digest: str, expected_size: int) -> bytes | None:
        try:
            body = self.blob_path(digest).read_bytes()
        except OSError:
            return None
        if len(body) != expected_size:
            return None
        if hashlib.sha256(body).hexdigest() != digest:
            return None
        return body

    def _write_blob(self, digest: str, body: bytes) -> None:
        target = self.blob_path(digest)
        existing = self._read_verified_blob(digest, len(body))
        if existing is not None:
            return
        ConditionalCache._atomic_write(target, body)

    def _write_blob_file(self, source: Path) -> tuple[str, int]:
        """Stream ``source`` into the blob store; return ``(digest, size)``."""
        hasher = hashlib.sha256()
        total = 0
        temporary: str | None = None
        try:
            with open(source, "rb") as src:
                with tempfile.NamedTemporaryFile(
                    mode="wb", dir=self.blob_dir, prefix=".incoming-", delete=False
                ) as handle:
                    temporary = handle.name
                    while True:
                        chunk = src.read(1024 * 256)
                        if not chunk:
                            break
                        hasher.update(chunk)
                        total += len(chunk)
                        handle.write(chunk)
                    handle.flush()
                    os.fsync(handle.fileno())
            digest = hasher.hexdigest()
            target = self.blob_path(digest)
            if not self._verify_blob_streaming(target, digest, total):
                os.replace(temporary, target)
                ConditionalCache._fsync_dir(target.parent)
            temporary = None
            return digest, total
        finally:
            if temporary is not None:
                try:
                    os.unlink(temporary)
                except OSError:
                    pass
