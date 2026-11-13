from collections.abc import Iterable
from pathlib import Path
from typing import Any, Callable

from .normalize import normalize
from .onix_service import OnixService
from .onix_storage import OnixStore
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


def _run_onix(
    database: str, action: Callable[[OnixService], dict[str, Any]]
) -> dict[str, Any]:
    """ONIX 组合根：打开存储、执行服务动作并管理资源生命周期。"""
    store = OnixStore(database)
    try:
        return action(OnixService(store))
    finally:
        store.close()


def run_onix_import(path: str, database: str, *, message_key: str | None = None) -> dict[str, Any]:
    xml_text = Path(path).read_text(encoding="utf-8-sig")
    return _run_onix(
        database,
        lambda svc: svc.import_message(xml_text, message_key=message_key),
    )


def run_onix_plan(
    message_key: str,
    database: str,
    *,
    field: str,
    product_ref: str | None = None,
    product_path: str | None = None,
) -> dict[str, Any]:
    return _run_onix(
        database,
        lambda svc: svc.plan_correction(
            message_key,
            field=field,
            record_reference=product_ref,
            product_path=product_path,
        ),
    )


def run_onix_correct(
    message_key: str,
    database: str,
    *,
    field: str,
    value: str,
    base_fingerprint: str,
    product_ref: str | None = None,
    product_path: str | None = None,
    node_path: str | None = None,
    rationale: str = "",
) -> dict[str, Any]:
    return _run_onix(
        database,
        lambda svc: svc.commit_correction(
            message_key,
            field=field,
            new_value=value,
            base_fingerprint=base_fingerprint,
            record_reference=product_ref,
            product_path=product_path,
            node_path=node_path,
            rationale=rationale,
        ),
    )


def run_onix_export(message_key: str, database: str, output: str) -> dict[str, Any]:
    result = _run_onix(database, lambda svc: svc.export_message(message_key))
    Path(output).write_text(result.pop("xml_text"), encoding="utf-8")
    result.update({"status": "exported", "output": output})
    return result
