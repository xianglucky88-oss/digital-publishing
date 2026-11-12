from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class Observation:
    source: str
    key: str
    observed_at: str
    payload: dict[str, Any]
    raw_hash: str


@dataclass(frozen=True)
class AvailabilityClaim:
    """追加式供货状态声明依据，一经保存不再修改或删除。

    本地语义（不依赖外部 ONIX 词表）：
    - status 仅取 ``available`` / ``withdrawn`` / ``unknown``；
    - ``observed_at_utc`` 为 None 表示原始时刻缺少时区偏移或无法解析，
      该声明不能被放上时间线，也不会被默认为可供货；
    - ``edition`` 为 None 的声明只按“缺版次材料”留存，不自动作用于该 ISBN
      的任何具体版次。
    """

    claim_id: str
    source: str
    isbn: str
    edition: str | None
    observed_at_raw: str
    observed_at_utc: str | None
    status: str
    status_raw: str | None
    note: str | None
    title: str | None
    language: str | None
    publisher: str | None
