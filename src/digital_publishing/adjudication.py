"""独立的供货状态时间线裁决。

调用者以 ``isbn``、``edition`` 和声明截止时刻 ``as_of`` 查询，得到：

1. 各来源在截止时刻之前的最新声明（按 ``observed_at_utc`` 选取，与入库
   到达顺序无关）；
2. 实际应用的来源优先规则及每一步裁决依据（可审计的 basis）；
3. 最终结论：``available``（可供货）、``withdrawn``（已撤回）或
   ``unknown``（无法确定）。

关键规则：

* 低优先来源的新声明不能盖过高优先来源仍有效的撤回；
* 同级来源矛盾不能任取一条，结论为无法确定；
* 同一来源后续明确恢复可以改变自己的状态；
* 晚收到但声明时刻更早的材料进入它所属的时间段，不会把当前状态倒退；
* 同刻冲突（同来源、同产品版次、同 observed_at 内容不同）双方都保留，
  该来源该时刻无法自证，按冲突处理；
* 缺版次声明、无时区偏移/无法解析时刻的声明只作证据展示，不参与裁决，
  且未知状态绝不默认为可供货。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone

from .claim_storage import ClaimStore
from .models import AvailabilityClaim
from .availability import AVAILABLE, UNKNOWN, WITHDRAWN

# 依据规则码，供调用方程序化引用；detail 为人类可读说明。
RULE_CUTOFF = "cutoff"
RULE_EXCLUDED_NOTE = "excluded-note"
RULE_LATEST_PER_SOURCE = "latest-per-source"
RULE_SAME_TIME_CONFLICT = "same-time-conflict"
RULE_PRIORITY_ORDER = "priority-order"
RULE_PRIORITY_WITHDRAWN_HELD = "priority-withdrawn-held"
RULE_SAME_RANK_CONFLICT = "same-rank-conflict"
RULE_RANK_ADOPTED = "rank-adopted"
RULE_NO_DETERMINATE = "no-determinate"
RULE_NO_CLAIMS = "no-claims"

_DETERMINATE = frozenset((AVAILABLE, WITHDRAWN))


@dataclass(frozen=True)
class BasisEntry:
    rule: str
    detail: str
    claim_ids: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class SourceState:
    source: str
    rank: int
    latest_observed_at: str | None
    latest_claim_ids: list[str]
    status: str
    conflicted: bool
    conflict_detail: str | None
    considered_claim_ids: list[str]


@dataclass(frozen=True)
class Evidence:
    """不进入裁决但需要向调用者交代的材料。"""

    claim: AvailabilityClaim
    reason: str


@dataclass(frozen=True)
class Adjudication:
    isbn: str
    edition: str
    as_of: str
    final_status: str
    priority: list[str]
    source_states: list[SourceState]
    basis: list[BasisEntry]
    excluded_evidence: list[Evidence]


@dataclass(frozen=True)
class TimelineSegment:
    valid_from: str
    valid_to: str | None
    status: str
    basis: list[BasisEntry]
    deciding_claim_ids: list[str]


def _as_utc(value: str) -> str:
    text = value.strip()
    if text.endswith(("Z", "z")):
        text = text[:-1] + "+00:00"
    moment = datetime.fromisoformat(text)
    if moment.tzinfo is None or moment.utcoffset() is None:
        raise ValueError("as_of 必须带时区偏移，无法确定的时刻不能作为截止时刻")
    return moment.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def build_ranks(priority: list[str]) -> dict[str, int]:
    """把显式配置的来源顺序转成排名：位置越靠前优先级越高（rank 越小）。
    未配置的来源统一落到配置之后的同一最低档。"""
    ranks: dict[str, int] = {}
    for index, source in enumerate(priority):
        if source not in ranks:
            ranks[source] = index
    return ranks


def _group_latest_by_source(
    claims: list[AvailabilityClaim],
) -> dict[str, tuple[str, list[AvailabilityClaim]]]:
    latest: dict[str, tuple[str, list[AvailabilityClaim]]] = {}
    for claim in claims:
        current = latest.get(claim.source)
        if current is None or claim.observed_at_utc > current[0]:
            latest[claim.source] = (claim.observed_at_utc, [claim])
        elif claim.observed_at_utc == current[0]:
            current[1].append(claim)
    return latest


def adjudicate(
    store: ClaimStore,
    isbn: str,
    edition: str,
    as_of: str,
    priority: list[str] | None = None,
) -> Adjudication:
    """按截止时刻裁决某 ISBN 具体版次的供货状态。"""
    priority = list(priority or [])
    cutoff = _as_utc(as_of)
    ranks = build_ranks(priority)
    fallback_rank = len(ranks)

    all_claims = store.claims_for_product(isbn, edition)

    excluded: list[Evidence] = []
    timed: list[AvailabilityClaim] = []
    for claim in all_claims:
        if claim.observed_at_utc is None:
            excluded.append(Evidence(claim, "observed_at 无时区偏移或无法解析，不进入时间线"))
        elif claim.observed_at_utc > cutoff:
            excluded.append(Evidence(claim, f"声明时刻晚于截止时刻 {cutoff}"))
        else:
            timed.append(claim)

    basis: list[BasisEntry] = [
        BasisEntry(
            RULE_CUTOFF,
            f"截止时刻 {cutoff}（含）之前的声明才参与裁决，按 observed_at 归位而非入库到达顺序",
            [claim.claim_id for claim in timed],
        )
    ]
    for item in excluded:
        basis.append(
            BasisEntry(RULE_EXCLUDED_NOTE, item.reason, [item.claim.claim_id])
        )

    latest_groups = _group_latest_by_source(timed)
    source_states: list[SourceState] = []
    for source, (moment, claims_at_latest) in sorted(
        latest_groups.items(), key=lambda item: ranks.get(item[0], fallback_rank)
    ):
        considered = [
            claim.claim_id
            for claim in timed
            if claim.source == source and claim.observed_at_utc <= moment
        ]
        statuses = {claim.status for claim in claims_at_latest}
        conflicted = len(claims_at_latest) > 1
        if conflicted:
            status = UNKNOWN
            detail = (
                f"来源 {source} 在 {moment} 的同刻声明存在冲突"
                f"（{sorted(statuses)}），双方均保留，该来源此时刻无法自证"
            )
            basis.append(
                BasisEntry(
                    RULE_SAME_TIME_CONFLICT,
                    detail,
                    [claim.claim_id for claim in claims_at_latest],
                )
            )
        else:
            detail = None
            only = claims_at_latest[0]
            basis.append(
                BasisEntry(
                    RULE_LATEST_PER_SOURCE,
                    f"来源 {source} 截止前最新声明时刻为 {moment}，状态 {only.status}"
                    + ("（原始取值未知，按 unknown 处理，不默认可供货）" if only.status == UNKNOWN else ""),
                    [only.claim_id],
                )
            )
            status = only.status
        source_states.append(
            SourceState(
                source=source,
                rank=ranks.get(source, fallback_rank),
                latest_observed_at=moment,
                latest_claim_ids=[claim.claim_id for claim in claims_at_latest],
                status=status,
                conflicted=conflicted,
                conflict_detail=detail,
                considered_claim_ids=considered,
            )
        )

    final_status, deciding, rank_basis = _apply_priority(
        source_states, ranks, fallback_rank, priority
    )
    basis.extend(rank_basis)

    return Adjudication(
        isbn=isbn,
        edition=edition,
        as_of=cutoff,
        final_status=final_status,
        priority=list(priority),
        source_states=source_states,
        basis=basis,
        excluded_evidence=excluded,
    )


def _apply_priority(
    states: list[SourceState],
    ranks: dict[str, int],
    fallback_rank: int,
    priority: list[str],
) -> tuple[str, list[str], list[BasisEntry]]:
    if not states:
        return UNKNOWN, [], [BasisEntry(RULE_NO_CLAIMS, "截止时刻前没有可用于该版次的声明，无法确定")]

    order_text = ", ".join(priority) if priority else "（未显式配置，所有来源同级）"
    basis = [
        BasisEntry(
            RULE_PRIORITY_ORDER,
            f"来源优先规则（靠前优先，未配置来源同列最低档 {fallback_rank}）：{order_text}",
        )
    ]

    highest = min(state.rank for state in states)
    ordered = sorted(states, key=lambda state: (state.rank, state.source))
    for state in ordered:
        if state.rank != highest:
            basis.append(
                BasisEntry(
                    RULE_PRIORITY_WITHDRAWN_HELD,
                    f"来源 {state.source}（优先级 {state.rank}）不得盖过更高优先级来源仍有效的结论，其声明不改变裁决",
                    state.latest_claim_ids,
                )
            )
            continue
        if state.conflicted:
            basis.append(
                BasisEntry(
                    RULE_SAME_TIME_CONFLICT,
                    f"最高优先级来源 {state.source} 同刻冲突，双方均保留，该来源无法自证",
                    state.latest_claim_ids,
                )
            )
        elif state.status not in _DETERMINATE:
            basis.append(
                BasisEntry(
                    RULE_NO_DETERMINATE,
                    f"最高优先级来源 {state.source} 的最新状态为未知，未知不默认可供货，跳过该来源",
                    state.latest_claim_ids,
                )
            )

    top = [state for state in ordered if state.rank == highest]
    determinate = [
        state
        for state in top
        if not state.conflicted and state.status in _DETERMINATE
    ]
    determinate_statuses = {state.status for state in determinate}
    if len(determinate_statuses) > 1:
        basis.append(
            BasisEntry(
                RULE_SAME_RANK_CONFLICT,
                f"同级来源在截止时刻前给出矛盾结论 {sorted(determinate_statuses)}，不能任取一条，无法确定",
                [claim_id for state in top for claim_id in state.latest_claim_ids],
            )
        )
        return UNKNOWN, [], basis
    if not determinate_statuses:
        if any(state.conflicted for state in top):
            basis.append(
                BasisEntry(
                    RULE_SAME_TIME_CONFLICT,
                    "最高优先级来源存在同刻冲突且无明确状态可采用，无法确定",
                )
            )
        else:
            basis.append(
                BasisEntry(RULE_NO_DETERMINATE, "最高优先级各来源均无明确状态，未知不默认可供货，无法确定")
            )
        return UNKNOWN, [], basis
    chosen = next(iter(determinate_statuses))
    deciding = [
        claim_id
        for state in determinate
        if state.status == chosen
        for claim_id in state.latest_claim_ids
    ]
    label = "可供货" if chosen == AVAILABLE else "已撤回"
    basis.append(
        BasisEntry(
            RULE_RANK_ADOPTED,
            f"采用最高优先级（rank {highest}）来源的明确状态：{chosen}（{label}）",
            deciding,
        )
    )
    return chosen, deciding, basis


def timeline(
    store: ClaimStore,
    isbn: str,
    edition: str,
    priority: list[str] | None = None,
) -> list[TimelineSegment]:
    """按声明时刻切分的完整状态时间线。

    每个段以一个可进入时间线的声明时刻为起点，复用同一套截止时刻裁决，
    因此晚收到的早时刻声明只会落在自己的时间段内，不会倒退当前状态。
    """
    cutoffs = store.distinct_cutoffs(isbn, edition)
    segments: list[TimelineSegment] = []
    for index, moment in enumerate(cutoffs):
        decision = adjudicate(store, isbn, edition, moment, priority)
        valid_to = cutoffs[index + 1] if index + 1 < len(cutoffs) else None
        deciding = [
            claim_id
            for state in decision.source_states
            if state.status == decision.final_status
            for claim_id in state.latest_claim_ids
        ]
        segments.append(
            TimelineSegment(
                valid_from=moment,
                valid_to=valid_to,
                status=decision.final_status,
                basis=decision.basis,
                deciding_claim_ids=deciding,
            )
        )
    return segments
