from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class Observation:
    source: str
    key: str
    observed_at: str
    payload: dict[str, Any]
    raw_hash: str
