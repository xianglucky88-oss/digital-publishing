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
    cache_dir: str | None = None,
    access_scope: str | None = None,
    report: list[dict[str, Any]] | None = None,
    source: HttpJsonSource | None = None,
    max_bytes: int | None = None,
    max_record_chars: int | None = None,
    max_depth: int | None = None,
) -> dict[str, int]:
    """Read one URL through the ingestion pipeline.

    The source stages and fully validates the response before any record
    reaches the pipeline; records are then streamed into the pipeline one
    at a time and the staging resources are released when consumption
    finishes or is abandoned.  ``report`` (when supplied) receives the read
    evidence: status, snapshot digest, record count and downloaded bytes.
    Callers that need to rehearse "request issued, another writer
    intervenes, then the 304 arrives" can pass their own ``source`` and
    call ``source.read()`` at the moment they choose.
    """
    limit_kwargs = {
        key: value
        for key, value in (
            ("max_bytes", max_bytes),
            ("max_record_chars", max_record_chars),
            ("max_depth", max_depth),
        )
        if value is not None
    }
    source = source or HttpJsonSource(
        url,
        timeout=timeout,
        raw_dir=raw_dir,
        records_key=records_key,
        cache_dir=cache_dir,
        access_scope=access_scope,
        **limit_kwargs,
    )
    try:
        records = source.read()
        try:
            return _run_records(records, database)
        finally:
            close = getattr(records, "close", None)
            if callable(close):
                close()
    finally:
        if report is not None and source.last_report is not None:
            report.append(source.last_report.as_dict())
