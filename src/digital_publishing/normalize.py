import hashlib
import json
from typing import Any
from .models import Observation

FIELD_MAP = {'isbn': 'isbn', 'title': 'title', 'publisher': 'publisher', 'edition': 'edition', 'language': 'language', 'availability': 'availability'}
KEY_FIELDS = ['isbn', 'edition']
REQUIRED_FIELDS = ['isbn', 'title', 'publisher']


def normalize(raw: Any) -> Observation:
    """只负责把一条来源记录转换为领域对象并执行字段级校验。"""
    if not isinstance(raw, dict):
        raise ValueError("record must be an object")
    source = str(raw.get("source", "")).strip()
    observed_at = str(raw.get("observed_at", "")).strip()
    payload = raw.get("payload")
    if not source or not observed_at or not isinstance(payload, dict):
        raise ValueError("source, observed_at and object payload are required")
    normalized = {}
    for target, source_field in FIELD_MAP.items():
        value = payload.get(source_field)
        if isinstance(value, str):
            value = " ".join(value.split())
        normalized[target] = value
    missing = [field for field in REQUIRED_FIELDS if normalized.get(field) in (None, "")]
    if missing:
        raise ValueError("missing required fields: " + ",".join(missing))
    key_values = [str(normalized.get(field, "")).strip().lower() for field in KEY_FIELDS]
    if any(not value for value in key_values):
        raise ValueError("deduplication key is incomplete")
    key = "|".join(key_values)
    canonical = json.dumps({"source": source, "observed_at": observed_at, "payload": normalized}, sort_keys=True, ensure_ascii=False)
    raw_hash = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    return Observation(source, key, observed_at, normalized, raw_hash)
