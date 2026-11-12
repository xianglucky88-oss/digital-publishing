"""供货状态裁决测试。

场景主线（题目要求）：A 源撤回、B 源活跃、A 源恢复三条声明，到达顺序
故意反过来，并补入一条更早声明；随后加入同刻冲突与缺版次材料，让调用者
分清“确实撤回”与“暂时无法判断”。演示 ISBN（978-1 等）不做真实校验位
校验。
"""

from __future__ import annotations

import json

import pytest

from digital_publishing.adjudication import (
    RULE_LATEST_PER_SOURCE,
    RULE_PRIORITY_WITHDRAWN_HELD,
    RULE_RANK_ADOPTED,
    RULE_SAME_RANK_CONFLICT,
    RULE_SAME_TIME_CONFLICT,
)
from digital_publishing.application import (
    adjudicate_availability,
    availability_timeline,
    migrate_legacy_observations,
)
from digital_publishing.availability import (
    AVAILABLE,
    UNKNOWN,
    WITHDRAWN,
    classify_status,
    extract_claim,
    parse_observed_at,
)
from digital_publishing.claim_storage import ClaimStore
from digital_publishing.models import AvailabilityClaim

ISBN = "978-1"
EDITION = "2"
PRIORITY = ["source-a", "source-b"]


def _record(source, observed_at, availability, *, isbn=ISBN, edition=EDITION, **extra):
    payload = {"isbn": isbn, "edition": edition, "title": "Data Pipelines",
               "publisher": "Example Press", "language": "en"}
    if availability is not None:
        payload["availability"] = availability
    payload.update(extra)
    return {"source": source, "observed_at": observed_at, "payload": payload}


# 声明时刻顺序：t0 早声明 -> t1 A 撤回 -> t2 B 活跃 -> t3 A 恢复
T0 = "2026-11-01T00:00:00Z"
T1 = "2026-11-02T00:00:00Z"
T2 = "2026-11-03T00:00:00Z"
T3 = "2026-11-04T00:00:00Z"


def _main_story_records():
    # 到达顺序与声明时刻顺序故意相反，且补入最早的 t0。
    return [
        _record("source-a", T3, "active"),       # A 恢复，最先到达
        _record("source-b", T2, "active"),       # B 活跃
        _record("source-a", T1, "withdrawn"),    # A 撤回
        _record("source-a", T0, "active"),       # 更早的声明，最晚到达
    ]


def _store_with(records, tmp_path, name="claims.sqlite3"):
    store = ClaimStore(str(tmp_path / name))
    for raw in records:
        claim = extract_claim(raw)
        assert claim is not None
        store.append(claim)
    return store


# ---------- 本地语义 ----------

def test_status_vocabulary_is_three_valued_and_unknown_is_default():
    assert classify_status("active") == (AVAILABLE, "active", None)
    assert classify_status("  Active ") == (AVAILABLE, "Active", None)
    assert classify_status("withdrawn") == (WITHDRAWN, "withdrawn", None)
    assert classify_status(None)[0] == UNKNOWN
    assert classify_status("")[0] == UNKNOWN
    assert classify_status("temporarily-delisted")[0] == UNKNOWN
    assert classify_status(123)[0] == UNKNOWN
    # 未知只是未知，绝不等于可供货
    assert classify_status(None)[0] != AVAILABLE


def test_observed_at_requires_offset_otherwise_not_on_timeline():
    utc, raw = parse_observed_at("2026-11-02T08:00:00+08:00")
    assert utc == "2026-11-02T00:00:00Z"
    assert parse_observed_at("2026-11-02T00:00:00")[0] is None
    assert parse_observed_at("not-a-time")[0] is None

    claim = extract_claim(_record("source-a", "2026-11-02T00:00:00", "active"))
    assert claim.observed_at_utc is None
    assert claim.status == AVAILABLE
    assert "无时区偏移" in claim.note


def test_demo_isbn_accepted_without_checksum_validation():
    claim = extract_claim(_record("source-a", T1, "withdrawn", isbn="978-1"))
    assert claim.isbn == "978-1"


def test_missing_edition_is_kept_but_not_attached_to_any_edition():
    claim = extract_claim(_record("source-a", T1, "withdrawn", edition=None))
    assert claim.edition is None
    assert claim.status == WITHDRAWN
    assert "版次缺失" in claim.note


# ---------- 追加式存储与同刻冲突 ----------

def test_claims_are_append_only_and_keep_same_time_conflicts(tmp_path):
    store = ClaimStore(str(tmp_path / "c.sqlite3"))
    first = extract_claim(_record("source-a", T1, "withdrawn"))
    second = extract_claim(_record("source-a", T1, "active"))
    assert first.claim_id != second.claim_id
    assert store.append(first) is True
    assert store.append(first) is False  # 内容完全相同才幂等跳过
    assert store.append(second) is True  # 同刻不同内容必须双方保留
    rows = store.claims_for_product(ISBN, EDITION)
    assert {row.status for row in rows} == {WITHDRAWN, AVAILABLE}

    # 缺版次材料不进入具体版次查询
    missing_edition = extract_claim(_record("source-a", T1, "withdrawn", edition=None))
    store.append(missing_edition)
    assert store.claims_for_product(ISBN, EDITION) == rows
    assert len(store.claims_for_isbn(ISBN)) == 3
    store.close()


# ---------- 主线：优先级、恢复、晚到声明 ----------

def test_reversed_arrival_order_still_adjudicates_by_observed_time(tmp_path):
    store = _store_with(_main_story_records(), tmp_path)

    # t1 截止：A 已撤回（t0 的 active 是该来源更早状态，已被自己的撤回取代）
    at_t1 = adjudicate_availability(
        str(tmp_path / "claims.sqlite3"), ISBN, EDITION, T1, PRIORITY
    )
    assert at_t1.final_status == WITHDRAWN
    states = {s.source: s for s in at_t1.source_states}
    assert states["source-a"].status == WITHDRAWN
    assert states["source-a"].latest_observed_at == T1


def test_lower_priority_active_cannot_override_higher_priority_withdrawal(tmp_path):
    _store_with(_main_story_records(), tmp_path)
    decision = adjudicate_availability(
        str(tmp_path / "claims.sqlite3"), ISBN, EDITION, T2, PRIORITY
    )
    # B 的新 active 声明不能盖过 A 仍有效的 withdrawn
    assert decision.final_status == WITHDRAWN
    states = {s.source: s for s in decision.source_states}
    assert states["source-a"].status == WITHDRAWN
    assert states["source-b"].status == AVAILABLE
    assert any(entry.rule == RULE_PRIORITY_WITHDRAWN_HELD for entry in decision.basis)
    assert any(entry.rule == RULE_RANK_ADOPTED and WITHDRAWN in entry.detail
               for entry in decision.basis)


def test_same_source_explicit_recovery_changes_its_own_state(tmp_path):
    _store_with(_main_story_records(), tmp_path)
    decision = adjudicate_availability(
        str(tmp_path / "claims.sqlite3"), ISBN, EDITION, T3, PRIORITY
    )
    assert decision.final_status == AVAILABLE
    states = {s.source: s for s in decision.source_states}
    assert states["source-a"].status == AVAILABLE
    assert states["source-a"].latest_observed_at == T3
    # 时间线上能看到 A 自己 active -> withdrawn -> active 的变化依据
    assert any(
        entry.rule == RULE_LATEST_PER_SOURCE and T3 in entry.detail
        for entry in decision.basis
    )


def test_late_arriving_earlier_claim_lands_in_its_own_segment(tmp_path):
    db = str(tmp_path / "claims.sqlite3")
    _store_with(_main_story_records(), tmp_path)
    segments = availability_timeline(db, ISBN, EDITION, PRIORITY)
    assert [segment.valid_from for segment in segments] == [T0, T1, T2, T3]
    # 晚到的 t0 声明只作用于最早段，不把当前状态倒退
    assert [segment.status for segment in segments] == [
        AVAILABLE,   # t0: A 早先 active
        WITHDRAWN,   # t1: A 撤回
        WITHDRAWN,   # t2: B active 无法盖过 A 撤回
        AVAILABLE,   # t3: A 恢复
    ]


def test_timeline_identical_regardless_of_ingestion_order(tmp_path):
    forward = [
        _record("source-a", T0, "active"),
        _record("source-a", T1, "withdrawn"),
        _record("source-b", T2, "active"),
        _record("source-a", T3, "active"),
    ]
    db_a = str(tmp_path / "a.sqlite3")
    db_b = str(tmp_path / "b.sqlite3")
    _store_with(forward, tmp_path, "a.sqlite3")
    _store_with(list(reversed(forward)), tmp_path, "b.sqlite3")
    ta = availability_timeline(db_a, ISBN, EDITION, PRIORITY)
    tb = availability_timeline(db_b, ISBN, EDITION, PRIORITY)
    assert [(s.valid_from, s.status) for s in ta] == [(s.valid_from, s.status) for s in tb]


def test_cutoff_excludes_future_claims(tmp_path):
    _store_with(_main_story_records(), tmp_path)
    decision = adjudicate_availability(
        str(tmp_path / "claims.sqlite3"), ISBN, EDITION, T0, PRIORITY
    )
    assert decision.final_status == AVAILABLE
    # t1/t2/t3 的材料被列为截止时刻之后的证据，而不是直接丢弃
    assert len(decision.excluded_evidence) == 3
    assert all("截止时刻" in item.reason for item in decision.excluded_evidence)


# ---------- 同级矛盾与同刻冲突：确实撤回 vs 暂时无法判断 ----------

def test_same_rank_contradiction_is_unknown_not_an_arbitrary_pick(tmp_path):
    records = [
        _record("source-a", T1, "withdrawn", isbn="978-7", edition="1"),
        _record("source-b", T2, "active", isbn="978-7", edition="1"),
    ]
    db = str(tmp_path / "same-rank.sqlite3")
    _store_with(records, tmp_path, "same-rank.sqlite3")
    decision = adjudicate_availability(db, "978-7", "1", T2, [])  # 未配置优先级 => 同级
    assert decision.final_status == UNKNOWN
    assert any(entry.rule == RULE_SAME_RANK_CONFLICT for entry in decision.basis)


def test_same_time_conflict_is_retained_and_renders_unknown(tmp_path):
    records = [
        _record("source-a", T1, "withdrawn", isbn="978-8", edition="1"),
        _record("source-a", T1, "active", isbn="978-8", edition="1"),
    ]
    db = str(tmp_path / "conflict.sqlite3")
    _store_with(records, tmp_path, "conflict.sqlite3")
    decision = adjudicate_availability(db, "978-8", "1", T1, PRIORITY)
    assert decision.final_status == UNKNOWN
    state = decision.source_states[0]
    assert state.conflicted is True
    assert len(state.latest_claim_ids) == 2
    assert any(entry.rule == RULE_SAME_TIME_CONFLICT for entry in decision.basis)


def test_conflicted_top_source_does_not_block_clear_same_rank_source(tmp_path):
    # A 同刻冲突无法自证，但同级 B 明确撤回 => 可以确定为撤回
    records = [
        _record("source-a", T1, "withdrawn", isbn="978-9", edition="1"),
        _record("source-a", T1, "active", isbn="978-9", edition="1"),
        _record("source-b", T1, "withdrawn", isbn="978-9", edition="1"),
    ]
    db = str(tmp_path / "mixed.sqlite3")
    _store_with(records, tmp_path, "mixed.sqlite3")
    decision = adjudicate_availability(db, "978-9", "1", T1, [])
    assert decision.final_status == WITHDRAWN


def test_missing_edition_material_does_not_apply_to_named_edition(tmp_path):
    records = [
        _record("source-a", T1, "withdrawn", isbn="978-10", edition=None),
        _record("source-b", T1, "active", isbn="978-10", edition="1"),
    ]
    db = str(tmp_path / "missing-edition.sqlite3")
    _store_with(records, tmp_path, "missing-edition.sqlite3")
    # 对具体版次 1：只剩 B 的 active；缺版次撤回不得自动作用过来
    decision = adjudicate_availability(db, "978-10", "1", T1, PRIORITY)
    assert decision.final_status == AVAILABLE
    # 缺版次材料仍然能在 ISBN 维度查到，作为证据留存
    store = ClaimStore(db)
    try:
        loose = [c for c in store.claims_for_isbn("978-10") if c.edition is None]
        assert len(loose) == 1 and loose[0].status == WITHDRAWN
    finally:
        store.close()


def test_unknown_availability_is_not_defaulted_to_available(tmp_path):
    records = [
        _record("source-a", T1, None, isbn="978-11", edition="1"),
        _record("source-b", T1, "weird-word", isbn="978-11", edition="1"),
    ]
    db = str(tmp_path / "unknown.sqlite3")
    _store_with(records, tmp_path, "unknown.sqlite3")
    decision = adjudicate_availability(db, "978-11", "1", T1, PRIORITY)
    assert decision.final_status == UNKNOWN


def test_no_claims_and_offsetless_cutoff_behavior(tmp_path):
    db = str(tmp_path / "empty.sqlite3")
    ClaimStore(db).close()
    decision = adjudicate_availability(db, "978-100", "9", T1, PRIORITY)
    assert decision.final_status == UNKNOWN
    assert decision.source_states == []

    # 无时区偏移的截止时刻本身不合法
    with pytest.raises(ValueError):
        adjudicate_availability(db, "978-100", "9", "2026-11-02T00:00:00", PRIORITY)


def test_offsetless_claim_is_evidence_but_never_adjudicated(tmp_path):
    records = [_record("source-a", "2026-11-02T00:00:00", "withdrawn",
                       isbn="978-12", edition="1")]
    db = str(tmp_path / "offsetless.sqlite3")
    _store_with(records, tmp_path, "offsetless.sqlite3")
    decision = adjudicate_availability(db, "978-12", "1", T3, PRIORITY)
    assert decision.final_status == UNKNOWN
    assert len(decision.excluded_evidence) == 1
    assert decision.source_states == []
    assert availability_timeline(db, "978-12", "1", PRIORITY) == []


# ---------- 旧观测迁移：只承认材料不足 ----------

def test_legacy_migration_recovers_existing_rows_but_not_lost_conflicts(tmp_path):
    db = str(tmp_path / "legacy.sqlite3")
    from digital_publishing.storage import ObservationStore
    from digital_publishing.normalize import normalize

    store = ObservationStore(db)
    raw = _record("source-a", T1, "withdrawn")
    store.upsert(normalize(raw))
    # 模拟旧覆盖行为：同刻不同内容 upsert 后只剩一方
    overwritten = _record("source-a", T1, "active")
    store.upsert(normalize(overwritten))
    store.close()

    result = migrate_legacy_observations(db)
    assert result["migrated"] == 1  # 被覆盖的冲突无法恢复，只承认现存材料
    decision = adjudicate_availability(db, ISBN, EDITION, T1, PRIORITY)
    # 现存行是后写入的 active；材料不足，裁决依据中没有同刻冲突
    assert decision.final_status == AVAILABLE
    assert not any(entry.rule == RULE_SAME_TIME_CONFLICT for entry in decision.basis)


# ---------- 端到端：ingest 管道同时写两种存储 ----------

def test_ingestion_pipeline_records_claims_even_for_rejected_observations(tmp_path):
    from digital_publishing.application import run_fixture

    fixture = tmp_path / "input.json"
    fixture.write_text(json.dumps([
        _record("source-a", T1, "withdrawn"),
        # 缺 title/publisher：观测入库会被拒绝，但声明依据必须留存
        {"source": "source-b", "observed_at": T2,
         "payload": {"isbn": ISBN, "edition": EDITION, "availability": "active"}},
    ], ensure_ascii=False), encoding="utf-8")
    db = str(tmp_path / "e2e.sqlite3")
    summary = run_fixture(str(fixture), db)
    assert summary["rejected"] == 1
    assert summary["claims"] == 2

    decision = adjudicate_availability(db, ISBN, EDITION, T2, PRIORITY)
    assert decision.final_status == WITHDRAWN
    assert {state.source for state in decision.source_states} == {"source-a", "source-b"}
