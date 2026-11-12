from __future__ import annotations

import hashlib
import json
import os
import tempfile
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any


class FixtureSource:
    """只负责从本地 fixture 读取原始记录，不做业务解析或持久化。"""

    def read(self, path: str) -> list[Any]:
        value = json.loads(Path(path).read_text(encoding="utf-8"))
        if not isinstance(value, list):
            raise ValueError("fixture must contain a JSON list")
        return value


class HttpJsonSource:
    """通过标准库 HTTP 客户端读取 JSON，并可将原始响应落盘。"""

    def __init__(
        self,
        url: str,
        *,
        timeout: float = 30.0,
        raw_dir: str | None = None,
        records_key: str | None = None,
        bearer_token_env: str = "DATA_SOURCE_BEARER_TOKEN",
        api_key_env: str = "DATA_SOURCE_API_KEY",
    ) -> None:
        if not url.startswith(("http://", "https://")):
            raise ValueError("url must use http:// or https://")
        if timeout <= 0:
            raise ValueError("timeout must be positive")
        self.url = url
        self.timeout = timeout
        self.raw_dir = Path(raw_dir) if raw_dir else None
        self.records_key = records_key
        self.bearer_token_env = bearer_token_env
        self.api_key_env = api_key_env

    def _headers(self) -> dict[str, str]:
        headers = {
            "Accept": "application/json",
            "User-Agent": "data-ingestion-adapter/0.1",
        }
        token = os.getenv(self.bearer_token_env)
        if token:
            headers["Authorization"] = f"Bearer {token}"
        api_key = os.getenv(self.api_key_env)
        if api_key:
            headers["X-API-Key"] = api_key
        return headers

    def _save_raw(self, body: bytes) -> None:
        if self.raw_dir is None:
            return
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        digest = hashlib.sha256(self.url.encode("utf-8") + b"\0" + body).hexdigest()
        target = self.raw_dir / f"{digest}.json"
        if target.exists():
            return
        with tempfile.NamedTemporaryFile(
            mode="wb", dir=self.raw_dir, prefix=f".{digest}.", delete=False
        ) as handle:
            temporary = Path(handle.name)
            handle.write(body)
        temporary.replace(target)

    @staticmethod
    def _select_records(value: Any, records_key: str | None) -> list[Any]:
        selected = value
        if records_key:
            for part in records_key.split("."):
                if not isinstance(selected, dict) or part not in selected:
                    raise ValueError(f"records key not found: {records_key}")
                selected = selected[part]
        elif isinstance(selected, dict):
            for key in ("records", "data", "items", "results"):
                candidate = selected.get(key)
                if isinstance(candidate, list):
                    selected = candidate
                    break
        if not isinstance(selected, list):
            raise ValueError("source response must contain a JSON list of records")
        return selected

    def read(self, _path: str | None = None) -> list[Any]:
        request = urllib.request.Request(self.url, headers=self._headers(), method="GET")
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                body = response.read()
        except urllib.error.HTTPError as exc:
            raise RuntimeError(f"source returned HTTP {exc.code}") from exc
        except urllib.error.URLError as exc:
            raise RuntimeError(f"source request failed: {exc.reason}") from exc
        self._save_raw(body)
        try:
            value = json.loads(body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("source response is not valid UTF-8 JSON") from exc
        return self._select_records(value, self.records_key)
