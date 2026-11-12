"""旧观测迁入追加式声明表。

旧 ``observations`` 表的主键是 ``(source, record_key, observed_at)``，同刻不同
内容在历史 upsert 中已被覆盖，迁入时无法恢复被抹去的一方。本迁移只如实承认
材料不足：按现存行抽取声明，不臆造冲突；对缺少版次或时刻无偏移的材料按
``availability.extract_claim`` 的规则标注并保留。
"""

from __future__ import annotations

import json
import sqlite3

from .availability import extract_claim
from .claim_storage import ClaimStore


def migrate_observations(
    source_conn: sqlite3.Connection,
    claim_store: ClaimStore,
) -> dict[str, int]:
    """把旧库 observations 中的现存行迁入声明表，返回迁移/跳过计数。"""
    rows = source_conn.execute(
        "SELECT source, observed_at, payload_json FROM observations"
    ).fetchall()
    migrated = skipped = 0
    for source, observed_at, payload_json in rows:
        try:
            payload = json.loads(payload_json)
        except (TypeError, json.JSONDecodeError):
            skipped += 1
            continue
        raw = {"source": source, "observed_at": observed_at, "payload": payload}
        claim = extract_claim(raw)
        if claim is None:
            skipped += 1
            continue
        if claim_store.append(claim):
            migrated += 1
        else:
            skipped += 1
    return {"migrated": migrated, "skipped": skipped}
