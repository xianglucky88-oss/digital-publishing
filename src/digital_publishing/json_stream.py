"""Bounded-memory streaming JSON extraction for HTTP source responses.

A response body is consumed as an iterable of byte chunks.  Bytes are
decoded as UTF-8 incrementally and characters are dropped as soon as they
are consumed, so the resident memory of a pass is bounded by the chunk
size, the single record currently being read and the parse depth -- never
by the number of records in the document or by the length of skipped
strings (for example irrelevant metadata fields).

Two passes are offered, to be used as "stage -> validate -> import":

``analyze_json``
    Validates the whole document (UTF-8, JSON grammar, container shape and
    a complete, garbage-free ending), locates the record array and counts
    the records.  Nothing is materialized except the current token;
    skipped metadata is never built into strings.

``iter_records``
    Re-reads a *previously validated* document and materializes the
    records one at a time.  It may only run after ``analyze_json`` has
    succeeded: until UTF-8, structure and the ending of the target
    response have been fully confirmed, no parsed prefix may be treated
    as a successful source batch.

Supported container shapes (unchanged from the previous ``json.loads``
implementation): a top-level array, the first list-valued of the default
keys ``records``/``data``/``items``/``results`` (priority order, not
document order), or a nested array named by a dot-separated
``records_key``.  Content after the container is validated as strictly as
the container itself.  Duplicate object keys resolve "last wins", exactly
like ``json.loads``.
"""

from __future__ import annotations

import codecs
import hashlib
import re
from dataclasses import dataclass
from typing import Any, Iterable, Iterator


class JsonStreamError(ValueError):
    """A structural, encoding or limit violation in a streamed JSON document."""


@dataclass(frozen=True)
class StreamLimits:
    """Hard bounds applied while a JSON document is consumed.

    ``max_depth``
        Maximum nesting of arrays/objects, counting the root value as 1.
        Exceeding it raises :class:`JsonStreamError` before the parser
        descends further.
    ``max_record_chars``
        Maximum decoded characters a single record may span.  Checked
        while the record is consumed, so an oversized record is rejected
        before it has been fully materialized.
    """

    max_depth: int = 64
    max_record_chars: int = 1_000_000


@dataclass(frozen=True)
class ContainerInfo:
    """Location of the record array inside a validated document.

    ``key_path`` is empty for a top-level array; otherwise it holds the
    member names to descend.  ``occurrences`` says which same-named member
    to enter at each level (duplicate keys: the last one wins, matching
    ``json.loads``).  ``record_count`` is the validated element count.
    """

    key_path: tuple[str, ...]
    occurrences: tuple[int, ...]
    record_count: int


DEFAULT_CONTAINER_KEYS = ("records", "data", "items", "results")

_DIGITS_RUN = re.compile(r"[0-9]+")
_STRING_RUN = re.compile(r'[^"\\\x00-\x1f]+')
_HEX = frozenset("0123456789abcdefABCDEF")
_WHITESPACE = frozenset(" \t\n\r")


class _Cursor:
    """Incremental UTF-8 character stream over byte chunks.

    Decoded characters are dropped as soon as they are consumed; the
    buffer never holds more than one undelivered chunk plus the current
    token, so memory stays bounded by the chunk size no matter how large
    the document is.
    """

    __slots__ = (
        "_decoder",
        "_chunks",
        "_buf",
        "_idx",
        "_eof",
        "offset",
        "_record_start",
        "_max_record_chars",
    )

    def __init__(self, chunks: Iterable[bytes], max_record_chars: int) -> None:
        self._decoder = codecs.getincrementaldecoder("utf-8")("strict")
        self._chunks = iter(chunks)
        self._buf = ""
        self._idx = 0
        self._eof = False
        self.offset = 0  # decoded characters consumed so far
        self._record_start: int | None = None
        self._max_record_chars = max_record_chars

    # ------------------------------------------------------------ buffering
    def _fill(self) -> None:
        if self._idx:
            self._buf = self._buf[self._idx :]
            self._idx = 0
        if self._eof:
            return
        chunk = next(self._chunks, None)
        if chunk is None:
            self._eof = True
            chunk = b""
        try:
            text = self._decoder.decode(chunk, final=self._eof)
        except UnicodeDecodeError as exc:
            raise JsonStreamError("source response is not valid UTF-8 JSON") from exc
        if text:
            self._buf = self._buf + text if self._buf else text

    # ------------------------------------------------------------ primitives
    def peek(self) -> str:
        """Return the current character, or ``""`` at end of input."""
        while self._idx >= len(self._buf) and not self._eof:
            self._fill()
        if self._idx >= len(self._buf):
            return ""
        return self._buf[self._idx]

    def _advance(self, count: int) -> None:
        self._idx += count
        self.offset += count
        if (
            self._record_start is not None
            and self.offset - self._record_start > self._max_record_chars
        ):
            raise JsonStreamError(
                f"record exceeds size limit of {self._max_record_chars} characters"
            )

    def next(self) -> str:
        """Consume and return the current character (``""`` at end of input)."""
        ch = self.peek()
        if ch:
            self._advance(1)
        return ch

    def lookahead(self, count: int) -> str:
        """Return up to ``count`` upcoming characters without consuming them."""
        while len(self._buf) - self._idx < count and not self._eof:
            self._fill()
        return self._buf[self._idx : self._idx + count]

    def consume_run(self, pattern: re.Pattern[str], keep: bool) -> tuple[str, int]:
        """Consume a maximal ``pattern`` run; return ``(text, char_count)``.

        When ``keep`` is false the consumed text is dropped chunk by chunk
        and the returned text is empty, so skipped runs (huge metadata
        strings, long numbers) never materialize in memory.
        """
        parts: list[str] | None = [] if keep else None
        total = 0
        while True:
            while self._idx >= len(self._buf):
                if self._eof:
                    return ("".join(parts) if keep else ""), total
                self._fill()
            match = pattern.match(self._buf, self._idx)
            if match is None:
                return ("".join(parts) if keep else ""), total
            end = match.end()
            if keep:
                parts.append(self._buf[self._idx : end])
            total += end - self._idx
            self._advance(end - self._idx)
            if end < len(self._buf) or self._eof:
                return ("".join(parts) if keep else ""), total
            # The run reached the end of the buffer; it may continue after
            # the next fill, so loop instead of returning.

    # ------------------------------------------------------------ record budget
    def begin_record(self) -> None:
        self._record_start = self.offset

    def end_record(self) -> None:
        self._record_start = None


class _Parser:
    """Recursive-descent JSON parser over a :class:`_Cursor`.

    ``materialize=False`` validates a value without building it; both
    modes apply exactly the same grammar, so skipped content is validated
    as strictly as content that is kept.
    """

    def __init__(self, cursor: _Cursor, max_depth: int) -> None:
        self._c = cursor
        self._max_depth = max_depth

    def error(self, message: str) -> JsonStreamError:
        return JsonStreamError(
            f"source response is not valid JSON: {message} at character {self._c.offset}"
        )

    def enter(self, depth: int) -> None:
        if depth > self._max_depth:
            raise JsonStreamError(f"nesting exceeds depth limit of {self._max_depth}")

    def skip_ws(self) -> None:
        c = self._c
        while c.peek() in _WHITESPACE:
            c.next()

    def expect_literal(self, word: str) -> None:
        for expected in word:
            if self._c.next() != expected:
                raise self.error(f"malformed literal, expected {word!r}")

    # ---------------------------------------------------------------- strings
    def parse_string(self, materialize: bool) -> str | None:
        c = self._c
        c.next()  # opening quote
        out: list[str] | None = [] if materialize else None
        while True:
            run, _ = c.consume_run(_STRING_RUN, materialize)
            if materialize:
                out.append(run)
            ch = c.next()
            if ch == "":
                raise self.error("unterminated string")
            if ch == '"':
                return "".join(out) if materialize else None
            if ch == "\\":
                piece = self._escape()
                if materialize:
                    out.append(piece)
            else:
                raise self.error("invalid control character in string")

    def _escape(self) -> str:
        c = self._c
        esc = c.next()
        if esc in ('"', "\\", "/"):
            return esc
        if esc == "b":
            return "\b"
        if esc == "f":
            return "\f"
        if esc == "n":
            return "\n"
        if esc == "r":
            return "\r"
        if esc == "t":
            return "\t"
        if esc == "u":
            return self._unicode_escape()
        raise self.error(f"invalid escape \\{esc}")

    def _unicode_escape(self) -> str:
        c = self._c
        digits = ""
        for _ in range(4):
            ch = c.next()
            if ch not in _HEX:
                raise self.error("invalid \\u escape")
            digits += ch
        code = int(digits, 16)
        if 0xD800 <= code <= 0xDBFF:
            # A high surrogate combines with a directly following low
            # surrogate escape; anything else stays a lone surrogate,
            # matching json.loads.
            ahead = c.lookahead(6)
            if (
                len(ahead) == 6
                and ahead[0] == "\\"
                and ahead[1] == "u"
                and all(h in _HEX for h in ahead[2:6])
            ):
                low = int(ahead[2:6], 16)
                if 0xDC00 <= low <= 0xDFFF:
                    for _ in range(6):
                        c.next()
                    return chr(0x10000 + ((code - 0xD800) << 10) + (low - 0xDC00))
        return chr(code)

    # ---------------------------------------------------------------- numbers
    def parse_number(self, materialize: bool) -> Any:
        c = self._c
        out: list[str] = []
        is_float = False
        first = c.peek()
        if first == "-":
            c.next()
            if materialize:
                out.append("-")
            first = c.peek()
        head_text, head_len = c.consume_run(_DIGITS_RUN, materialize)
        if head_len == 0:
            raise self.error("malformed number")
        if head_len > 1 and first == "0":
            raise self.error("malformed number: leading zero")
        if materialize:
            out.append(head_text)
        if c.peek() == ".":
            is_float = True
            c.next()
            if materialize:
                out.append(".")
            frac_text, frac_len = c.consume_run(_DIGITS_RUN, materialize)
            if frac_len == 0:
                raise self.error("malformed number: fraction needs digits")
            if materialize:
                out.append(frac_text)
        ch = c.peek()
        if ch in ("e", "E"):
            is_float = True
            c.next()
            if materialize:
                out.append(ch)
            sign = c.peek()
            if sign in ("+", "-"):
                c.next()
                if materialize:
                    out.append(sign)
            exp_text, exp_len = c.consume_run(_DIGITS_RUN, materialize)
            if exp_len == 0:
                raise self.error("malformed number: exponent needs digits")
            if materialize:
                out.append(exp_text)
        if not materialize:
            return None
        text = "".join(out)
        try:
            return float(text) if is_float else int(text)
        except ValueError as exc:  # e.g. Python's int-string digit ceiling
            raise self.error("malformed number") from exc

    # ---------------------------------------------------------------- values
    def parse_value(self, depth: int, materialize: bool) -> Any:
        ch = self._c.peek()
        if ch == "{":
            return self.parse_object(depth, materialize)
        if ch == "[":
            return self.parse_array(depth, materialize)
        if ch == '"':
            return self.parse_string(materialize)
        if ch == "t":
            self.expect_literal("true")
            return True if materialize else None
        if ch == "f":
            self.expect_literal("false")
            return False if materialize else None
        if ch == "n":
            self.expect_literal("null")
            return None
        if ch == "-" or ("0" <= ch <= "9"):
            return self.parse_number(materialize)
        if ch == "":
            raise self.error("unexpected end of document")
        raise self.error(f"unexpected character {ch!r}")

    def parse_object(self, depth: int, materialize: bool) -> Any:
        self.enter(depth)
        c = self._c
        c.next()  # {
        obj: dict[str, Any] | None = {} if materialize else None
        self.skip_ws()
        if c.peek() == "}":
            c.next()
            return obj
        while True:
            self.skip_ws()
            if c.peek() != '"':
                raise self.error("object keys must be strings")
            key = self.parse_string(materialize=True)
            self.skip_ws()
            if c.next() != ":":
                raise self.error("expected ':'")
            self.skip_ws()
            value = self.parse_value(depth + 1, materialize)
            if materialize:
                obj[key] = value
            self.skip_ws()
            ch = c.next()
            if ch == "}":
                return obj
            if ch != ",":
                raise self.error("expected ',' or '}'")

    def parse_array(self, depth: int, materialize: bool) -> Any:
        self.enter(depth)
        c = self._c
        c.next()  # [
        arr: list[Any] | None = [] if materialize else None
        self.skip_ws()
        if c.peek() == "]":
            c.next()
            return arr
        while True:
            self.skip_ws()
            value = self.parse_value(depth + 1, materialize)
            if materialize:
                arr.append(value)
            self.skip_ws()
            ch = c.next()
            if ch == "]":
                return arr
            if ch != ",":
                raise self.error("expected ',' or ']'")


# ------------------------------------------------------------------ analysis
def _stream_array_elements(parser: _Parser, cursor: _Cursor, depth: int) -> int:
    """Consume a ``[`` array in skip mode and return its element count."""
    parser.enter(depth)
    cursor.next()  # [
    count = 0
    parser.skip_ws()
    if cursor.peek() == "]":
        cursor.next()
        return 0
    while True:
        parser.skip_ws()
        cursor.begin_record()
        parser.parse_value(depth + 1, materialize=False)
        cursor.end_record()
        count += 1
        parser.skip_ws()
        ch = cursor.next()
        if ch == "]":
            return count
        if ch != ",":
            raise parser.error("expected ',' or ']'")


def _analyze_default_container(parser: _Parser, cursor: _Cursor) -> ContainerInfo | None:
    """Consume the root object and pick the record container by priority.

    The first of ``records``/``data``/``items``/``results`` (in that
    priority order, independent of document order) whose last occurrence
    holds a list becomes the container -- the same rule the previous
    ``json.loads``-based selection applied.
    """
    cursor.next()  # {
    candidates: dict[str, tuple[int, int]] = {}
    seen: dict[str, int] = {}
    parser.skip_ws()
    if cursor.peek() == "}":
        cursor.next()
    else:
        while True:
            parser.skip_ws()
            if cursor.peek() != '"':
                raise parser.error("object keys must be strings")
            key = parser.parse_string(materialize=True)
            parser.skip_ws()
            if cursor.next() != ":":
                raise parser.error("expected ':'")
            parser.skip_ws()
            occurrence = seen.get(key, 0)
            seen[key] = occurrence + 1
            if key in DEFAULT_CONTAINER_KEYS:
                # The last occurrence of a key decides, like json.loads.
                candidates.pop(key, None)
                if cursor.peek() == "[":
                    count = _stream_array_elements(parser, cursor, 2)
                    candidates[key] = (occurrence, count)
                else:
                    parser.parse_value(2, materialize=False)
            else:
                parser.parse_value(2, materialize=False)
            parser.skip_ws()
            ch = cursor.next()
            if ch == "}":
                break
            if ch != ",":
                raise parser.error("expected ',' or '}'")
    for key in DEFAULT_CONTAINER_KEYS:
        if key in candidates:
            occurrence, count = candidates[key]
            return ContainerInfo((key,), (occurrence,), count)
    return None


def _walk_path(
    parser: _Parser, cursor: _Cursor, parts: tuple[str, ...], depth: int
) -> tuple[str, tuple[int, ...], int]:
    """Consume the object at ``depth`` hunting for ``parts``.

    Returns ``(status, occurrences, record_count)`` where status is
    ``"ok"``, ``"not_found"`` (path missing or not an object midway) or
    ``"not_a_list"`` (path resolves to a non-array value).  The whole
    object is always consumed and validated, even after the target is
    found, so structural errors anywhere in the document take precedence
    over selection errors -- as they did with ``json.loads``.
    """
    cursor.next()  # {
    target = parts[0]
    seen = 0
    found: tuple[str, tuple[int, ...], int] = ("not_found", (), 0)
    parser.skip_ws()
    if cursor.peek() == "}":
        cursor.next()
        return found
    while True:
        parser.skip_ws()
        if cursor.peek() != '"':
            raise parser.error("object keys must be strings")
        key = parser.parse_string(materialize=True)
        parser.skip_ws()
        if cursor.next() != ":":
            raise parser.error("expected ':'")
        parser.skip_ws()
        if key == target:
            occurrence = seen
            seen += 1
            if len(parts) == 1:
                if cursor.peek() == "[":
                    count = _stream_array_elements(parser, cursor, depth + 1)
                    found = ("ok", (occurrence,), count)
                else:
                    parser.parse_value(depth + 1, materialize=False)
                    found = ("not_a_list", (), 0)
            elif cursor.peek() == "{":
                status, sub_occurrences, count = _walk_path(parser, cursor, parts[1:], depth + 1)
                found = (status, (occurrence,) + sub_occurrences, count)
            else:
                parser.parse_value(depth + 1, materialize=False)
                found = ("not_found", (), 0)
        else:
            parser.parse_value(depth + 1, materialize=False)
        parser.skip_ws()
        ch = cursor.next()
        if ch == "}":
            return found
        if ch != ",":
            raise parser.error("expected ',' or '}'")


def _finish_document(parser: _Parser, cursor: _Cursor) -> None:
    parser.skip_ws()
    if cursor.peek() != "":
        raise parser.error("unexpected trailing data")


def _analyze_document(
    parser: _Parser, cursor: _Cursor, records_key: str | None
) -> ContainerInfo:
    parser.skip_ws()
    ch = cursor.peek()
    if ch == "":
        raise parser.error("empty document")
    if ch == "[":
        if records_key is not None:
            raise JsonStreamError(f"records key not found: {records_key}")
        count = _stream_array_elements(parser, cursor, 1)
        _finish_document(parser, cursor)
        return ContainerInfo((), (), count)
    if ch == "{":
        if records_key is None:
            info = _analyze_default_container(parser, cursor)
            _finish_document(parser, cursor)
            if info is None:
                raise JsonStreamError("source response must contain a JSON list of records")
            return info
        parts = tuple(records_key.split("."))
        status, occurrences, count = _walk_path(parser, cursor, parts, 1)
        _finish_document(parser, cursor)
        if status == "not_found":
            raise JsonStreamError(f"records key not found: {records_key}")
        if status == "not_a_list":
            raise JsonStreamError("source response must contain a JSON list of records")
        return ContainerInfo(parts, occurrences, count)
    if records_key is not None:
        raise JsonStreamError(f"records key not found: {records_key}")
    raise JsonStreamError("source response must contain a JSON list of records")


def analyze_json(
    chunks: Iterable[bytes],
    records_key: str | None,
    limits: StreamLimits,
    *,
    expected_digest: str | None = None,
    expected_size: int | None = None,
) -> ContainerInfo:
    """Validate a complete JSON document and locate its record array.

    The whole document is consumed exactly once: UTF-8 decoding, JSON
    grammar, the container shape and a complete, garbage-free ending are
    all confirmed before this returns.  When ``expected_digest`` /
    ``expected_size`` are given, the raw bytes are additionally hashed and
    measured as they flow by, and any mismatch fails the analysis.
    """
    hasher = hashlib.sha256() if expected_digest is not None else None
    total = 0

    def measured() -> Iterator[bytes]:
        nonlocal total
        for chunk in chunks:
            total += len(chunk)
            if hasher is not None:
                hasher.update(chunk)
            yield chunk

    cursor = _Cursor(measured(), limits.max_record_chars)
    parser = _Parser(cursor, limits.max_depth)
    info = _analyze_document(parser, cursor, records_key)
    if expected_size is not None and total != expected_size:
        raise JsonStreamError("cached snapshot size mismatch")
    if hasher is not None and hasher.hexdigest() != expected_digest:
        raise JsonStreamError("cached snapshot failed integrity check")
    return info


# ------------------------------------------------------------------ extraction
def _seek_member(
    parser: _Parser, cursor: _Cursor, target: str, occurrence: int, depth: int
) -> bool:
    """Scan the current object for the ``occurrence``-th member ``target``.

    Other members are validated and skipped.  On success the cursor sits
    at the member's value; returns ``False`` when the object ends first.
    """
    parser.skip_ws()
    if cursor.peek() == "}":
        cursor.next()
        return False
    seen = 0
    while True:
        parser.skip_ws()
        if cursor.peek() != '"':
            raise parser.error("object keys must be strings")
        key = parser.parse_string(materialize=True)
        parser.skip_ws()
        if cursor.next() != ":":
            raise parser.error("expected ':'")
        parser.skip_ws()
        if key == target:
            if seen == occurrence:
                return True
            seen += 1
        parser.parse_value(depth + 1, materialize=False)
        parser.skip_ws()
        ch = cursor.next()
        if ch == "}":
            return False
        if ch != ",":
            raise parser.error("expected ',' or '}'")


def _iter_array_elements(parser: _Parser, cursor: _Cursor, depth: int) -> Iterator[Any]:
    """Yield the elements of a ``[`` array, one materialized record at a time."""
    parser.enter(depth)
    cursor.next()  # [
    parser.skip_ws()
    if cursor.peek() == "]":
        cursor.next()
        return
    while True:
        parser.skip_ws()
        cursor.begin_record()
        value = parser.parse_value(depth + 1, materialize=True)
        cursor.end_record()
        yield value
        parser.skip_ws()
        ch = cursor.next()
        if ch == "]":
            return
        if ch != ",":
            raise parser.error("expected ',' or ']'")


def iter_records(
    chunks: Iterable[bytes], info: ContainerInfo, limits: StreamLimits
) -> Iterator[Any]:
    """Materialize records one at a time from a *validated* document.

    ``info`` must come from a successful :func:`analyze_json` pass over
    the same bytes.  Everything outside the record array is validated and
    skipped; only one record is ever held in memory.
    """
    cursor = _Cursor(chunks, limits.max_record_chars)
    parser = _Parser(cursor, limits.max_depth)
    parser.skip_ws()
    if not info.key_path:
        yield from _iter_array_elements(parser, cursor, 1)
        return
    depth = 1
    for level, (part, occurrence) in enumerate(zip(info.key_path, info.occurrences)):
        if cursor.peek() != "{":
            raise JsonStreamError("validated snapshot no longer matches its analysis")
        cursor.next()  # {
        if not _seek_member(parser, cursor, part, occurrence, depth):
            raise JsonStreamError("validated snapshot no longer matches its analysis")
        if level == len(info.key_path) - 1:
            if cursor.peek() != "[":
                raise JsonStreamError("validated snapshot no longer matches its analysis")
            yield from _iter_array_elements(parser, cursor, depth + 1)
            return
        depth += 1
