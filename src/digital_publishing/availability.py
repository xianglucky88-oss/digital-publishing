"""供货状态声明的本地语义与抽取。

本模块只定义本地词表语义和从原始记录抽取声明依据的规则：

* 状态只有三种本地取值：``available``（可供货，对应原始词 ``active``）、
  ``withdrawn``（已撤回）、``unknown``（未知）。缺失、空串、空值或无法
  识别的取值一律归入 ``unknown``，绝不默认为可供货，也不做销售订单或
  版权许可层面的推断。
* 声明时刻沿用记录中的 ``observed_at``。能解析为带时区的时间点时归一化为
  UTC（``observed_at_utc``）；没有时区偏移或无法解析时 ``observed_at_utc``
  为 None，该声明留作依据但不能进入任何时间段裁决。
* 演示用 ISBN（如 ``978-1``）按普通字符串处理，不做真实 ISBN 校验位校验。
* 版次缺失的声明仍然留存，但不得自动作用到该 ISBN 的其他版次。
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any

from .models import AvailabilityClaim

AVAILABLE = "available"
WITHDRAWN = "withdrawn"
UNKNOWN = "unknown"

STATUSES = (AVAILABLE, WITHDRAWN, UNKNOWN)

# 原始词到本地状态的显式映射；未列出的取值一律 unknown。
_STATUS_WORDS = {
    "active": AVAILABLE,
    "available": AVAILABLE,
    "inprint": AVAILABLE,
    "in-print": AVAILABLE,
    "in_print": AVAILABLE,
    "withdrawn": WITHDRAWN,
    "recalled": WITHDRAWN,
    "outofprint": WITHDRAWN,
    "out-of-print": WITHDRAWN,
    "out_of_print": WITHDRAWN,
    "delisted": WITHDRAWN,
}


def classify_status(raw: Any) -> tuple[str, str | None, str | None]:
    """把原始 availability 值翻译成本地三态。

    返回 ``(status, status_raw, note)``：``status`` 必为三态之一；
    无法判定时给出说明性 note，调用方不得把 unknown 当可供货处理。
    """
    if raw is None:
        return UNKNOWN, None, "availability 缺失，按未知处理，不默认可供货"
    if not isinstance(raw, str):
        return UNKNOWN, str(raw), "availability 非字符串，按未知处理"
    text = " ".join(raw.split())
    if not text:
        return UNKNOWN, "", "availability 为空串，按未知处理，不默认可供货"
    word = text.lower()
    if word in _STATUS_WORDS:
        return _STATUS_WORDS[word], text, None
    return UNKNOWN, text, f"无法识别的 availability 取值 {text!r}，按未知处理"


def parse_observed_at(raw: Any) -> tuple[str | None, str]:
    """把 observed_at 归一化为 UTC 字符串。

    返回 ``(observed_at_utc, observed_at_raw)``。无时区偏移或无法解析时
    第一个元素为 None——此类声明保留依据，但不能进入时间线裁决。
    """
    text = "" if raw is None else str(raw).strip()
    if not text:
        return None, text
    candidate = text
    # datetime.fromisoformat 只在 3.11+ 接受 'Z'，这里显式兼容。
    if candidate.endswith(("Z", "z")):
        candidate = candidate[:-1] + "+00:00"
    try:
        moment = datetime.fromisoformat(candidate)
    except ValueError:
        return None, text
    if moment.tzinfo is None or moment.utcoffset() is None:
        return None, text
    utc = moment.astimezone(timezone.utc)
    return utc.strftime("%Y-%m-%dT%H:%M:%SZ"), text


def _clean(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    text = " ".join(value.split())
    return text or None


def _claim_hash(parts: dict[str, Any]) -> str:
    canonical = json.dumps(parts, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def extract_claim(raw: Any) -> AvailabilityClaim | None:
    """从一条原始 JSON 记录抽取追加式声明依据。

    与规范化管道相互独立：即使记录缺少 title/publisher 等入库必填字段、
    或缺少 edition，也尽量保留其供货声明。只有连来源、ISBN、observed_at
    都无法取得时才返回 None（材料完全不足）。
    """
    if not isinstance(raw, dict):
        return None
    source = _clean(raw.get("source"))
    observed_raw_value = raw.get("observed_at")
    payload = raw.get("payload")
    if not source or observed_raw_value is None or not isinstance(payload, dict):
        return None
    isbn = _clean(payload.get("isbn"))
    if not isbn:
        return None

    observed_utc, observed_raw = parse_observed_at(observed_raw_value)
    status, status_raw, status_note = classify_status(payload.get("availability"))
    edition = _clean(payload.get("edition"))

    notes = [note for note in (status_note,) if note]
    if observed_utc is None:
        notes.append("observed_at 无时区偏移或无法解析，该声明不进入时间线裁决")
    if edition is None:
        notes.append("版次缺失，该声明不作用于该 ISBN 的任何具体版次")

    identity = {
        "source": source,
        "isbn": isbn,
        "edition": edition,
        "observed_at_raw": observed_raw,
        "observed_at_utc": observed_utc,
        "status": status,
        "status_raw": status_raw,
    }
    return AvailabilityClaim(
        claim_id=_claim_hash(identity),
        source=source,
        isbn=isbn,
        edition=edition,
        observed_at_raw=observed_raw,
        observed_at_utc=observed_utc,
        status=status,
        status_raw=status_raw,
        note="；".join(notes) if notes else None,
        title=_clean(payload.get("title")),
        language=_clean(payload.get("language")),
        publisher=_clean(payload.get("publisher")),
    )
