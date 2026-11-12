from collections.abc import Iterable
from typing import Any, Callable

from .normalize import normalize
from .pipeline import IngestionPipeline
from .sources import FixtureSource, HttpJsonSource
from .storage import ObservationStore


def _run_records(records: Iterable[Any], database: str) -> dict[str, int]:
    """应用组合根：组装 source、pipeline 和 store，并管理资源生命周期。"""
    store = ObservationStore(database)
    try:
        return IngestionPipeline(records, normalize, store).run()
    finally:
        store.close()


def run_fixture(fixture: str, database: str) -> dict[str, int]:
    return _run_records(FixtureSource().read(fixture), database)


def run_url(
    url: str,
    database: str,
    *,
    timeout: float = 30.0,
    raw_dir: str | None = None,
    records_key: str | None = None,
) -> dict[str, int]:
    source = HttpJsonSource(url, timeout=timeout, raw_dir=raw_dir, records_key=records_key)
    return _run_records(source.read(), database)
