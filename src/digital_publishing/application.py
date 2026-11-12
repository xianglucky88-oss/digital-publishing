from collections.abc import Iterable
from typing import Any, Callable

from .adjudication import Adjudication, TimelineSegment, adjudicate, timeline
from .claim_storage import ClaimStore
from .migration import migrate_observations
from .normalize import normalize
from .pipeline import IngestionPipeline
from .sources import FixtureSource, HttpJsonSource
from .storage import ObservationStore


def _run_records(
    records: Iterable[Any],
    database: str,
    claim_database: str | None = None,
) -> dict[str, int]:
    """应用组合根：组装 source、pipeline 和 store，并管理资源生命周期。"""
    store = ObservationStore(database)
    claim_store = ClaimStore(claim_database or database)
    try:
        return IngestionPipeline(records, normalize, store, claim_store).run()
    finally:
        claim_store.close()
        store.close()


def run_fixture(
    fixture: str, database: str, claim_database: str | None = None
) -> dict[str, int]:
    return _run_records(FixtureSource().read(fixture), database, claim_database)


def run_url(
    url: str,
    database: str,
    *,
    timeout: float = 30.0,
    raw_dir: str | None = None,
    records_key: str | None = None,
    claim_database: str | None = None,
) -> dict[str, int]:
    source = HttpJsonSource(url, timeout=timeout, raw_dir=raw_dir, records_key=records_key)
    return _run_records(source.read(), database, claim_database)


def adjudicate_availability(
    database: str,
    isbn: str,
    edition: str,
    as_of: str,
    priority: list[str] | None = None,
) -> Adjudication:
    """供货状态裁决查询入口：返回各来源最新声明、优先规则依据与最终结论。"""
    claim_store = ClaimStore(database)
    try:
        return adjudicate(claim_store, isbn, edition, as_of, priority)
    finally:
        claim_store.close()


def availability_timeline(
    database: str,
    isbn: str,
    edition: str,
    priority: list[str] | None = None,
) -> list[TimelineSegment]:
    """独立的状态时间线查询：逐段给出状态与裁决依据。"""
    claim_store = ClaimStore(database)
    try:
        return timeline(claim_store, isbn, edition, priority)
    finally:
        claim_store.close()


def migrate_legacy_observations(
    database: str,
) -> dict[str, int]:
    """把同库旧 observations 表的现存材料迁入追加式声明表。"""
    claim_store = ClaimStore(database)
    try:
        return migrate_observations(claim_store.conn, claim_store)
    finally:
        claim_store.close()
