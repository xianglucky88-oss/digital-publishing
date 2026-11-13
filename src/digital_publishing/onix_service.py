"""ONIX 消息的导入、订正与导出编排。

要点：

- 投影复用现有 ``normalize`` 契约，能唯一解释的字段才进入观测；
- 订正提交必须携带基准消息指纹；指纹不匹配说明接入的新消息已改变基准，
  记录冲突并拒绝用旧映射改新版；
- 订正落库是一个事务：新消息版本、全部产品的重投影、目录观测、订正状态
  一起提交——不会只更新目录却交付未改或半改的消息；
- 中途失败时回滚全部写入，另起事务把失败与完整依据记入订正表。
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from . import onix
from .normalize import normalize
from .onix_storage import OnixStore


class OnixService:
    def __init__(self, store: OnixStore):
        self.store = store

    def import_message(self, xml_text: str, message_key: str | None = None) -> dict[str, Any]:
        doc = onix.parse(xml_text)
        projections = onix.project_message(doc)
        header = onix.header_info(doc)

        key = message_key if message_key else header["sender"]
        if not key:
            raise ValueError(
                "message key required: pass message_key or include Header/Sender/SenderName"
            )
        sender = header["sender"] if header["sender"] else key
        fp = onix.fingerprint(xml_text)
        observed_at = header["sent_at"] or datetime.now(timezone.utc).isoformat()
        source = f"onix:{sender}"

        current = self.store.current_message(key)
        self.store.begin()
        try:
            if current and current["fingerprint"] == fp:
                version = current["version"]
                status = "unchanged"
            else:
                version = self.store.insert_version(key, sender, xml_text, fp, "import")
                status = "imported"

            rows: list[tuple[onix.ProductProjection, str | None]] = []
            product_results = []
            changed = 0
            for projection in projections:
                observation_key, upsert_changed = self._project_to_observation(
                    projection, source, observed_at
                )
                changed += int(upsert_changed)
                rows.append((projection, observation_key))
                product_results.append(self._product_result(projection, observation_key))

            self.store.replace_projections(key, version, rows)
            self.store.commit()
        except Exception:
            self.store.rollback()
            raise

        return {
            "status": status,
            "message_key": key,
            "version": version,
            "fingerprint": fp,
            "sender": sender,
            "products": product_results,
            "projected": sum(1 for p in product_results if p["status"] == "projected"),
            "rejected": sum(1 for p in product_results if p["status"] == "rejected"),
            "observations_changed": changed,
        }

    def _project_to_observation(
        self, projection: onix.ProductProjection, source: str, observed_at: str
    ) -> tuple[str | None, bool]:
        """把可投影产品写入目录观测，返回 (观测键, 是否有变化)；不可投影返回 (None, False)。"""
        if not projection.ok:
            return None, False
        record = {
            "source": source,
            "observed_at": observed_at,
            "payload": projection.payload(),
        }
        try:
            observation = normalize(record)
        except ValueError as exc:
            projection.reasons.append(f"normalization: {exc}")
            return None, False
        return observation.key, self.store.upsert(observation, commit=False)

    @staticmethod
    def _product_result(
        projection: onix.ProductProjection, observation_key: str | None
    ) -> dict[str, Any]:
        result = {
            "index": projection.product_index,
            "path": projection.product_path,
            "record_reference": projection.record_reference,
            "status": "projected" if observation_key else "rejected",
        }
        if observation_key:
            result["observation_key"] = observation_key
            result["issues"] = list(projection.issues)
        else:
            result["reasons"] = list(projection.reasons)
        return result

    def plan_correction(
        self,
        message_key: str,
        *,
        field: str,
        record_reference: str | None = None,
        product_path: str | None = None,
    ) -> dict[str, Any]:
        """列出字段在当前消息中的候选源节点与已保存的映射，不改动任何内容。"""
        if field not in onix.CORRECTABLE_FIELDS:
            raise ValueError(
                f"field must be one of {onix.CORRECTABLE_FIELDS}, got {field!r}"
            )
        current = self._require_message(message_key)
        doc = onix.parse(current["xml_text"])
        product, path, _index = onix.locate_product(
            doc, record_reference=record_reference, product_path=product_path
        )
        candidates = onix.correction_candidates(product, path, field)
        mapped = self._saved_mapping(message_key, current["version"], path, field)
        return {
            "status": "ok",
            "message_key": message_key,
            "version": current["version"],
            "fingerprint": current["fingerprint"],
            "product_path": path,
            "record_reference": onix.child_text(product, "RecordReference"),
            "field": field,
            "candidates": [c.to_dict() for c in candidates],
            "mapped": mapped,
        }

    def commit_correction(
        self,
        message_key: str,
        *,
        field: str,
        new_value: str,
        base_fingerprint: str,
        record_reference: str | None = None,
        product_path: str | None = None,
        node_path: str | None = None,
        rationale: str = "",
    ) -> dict[str, Any]:
        if field not in onix.CORRECTABLE_FIELDS:
            raise ValueError(
                f"field must be one of {onix.CORRECTABLE_FIELDS}, got {field!r}"
            )
        value = " ".join(str(new_value).split())
        if not value:
            raise ValueError("new value must be non-empty")

        current = self._require_message(message_key)

        if current["fingerprint"] != base_fingerprint:
            detail = {
                "current_fingerprint": current["fingerprint"],
                "current_version": current["version"],
            }
            correction_id = self._record_correction(
                message_key=message_key,
                base_fingerprint=base_fingerprint,
                product_path=product_path or "",
                record_reference=record_reference,
                field=field,
                old_value=None,
                new_value=value,
                node_path=node_path,
                rationale=rationale,
                status="conflict",
                detail=detail,
            )
            return {
                "status": "conflict",
                "correction_id": correction_id,
                "message_key": message_key,
                "base_fingerprint": base_fingerprint,
                **detail,
            }

        doc = onix.parse(current["xml_text"])
        try:
            product, resolved_path, _index = onix.locate_product(
                doc, record_reference=record_reference, product_path=product_path
            )
        except ValueError as exc:
            correction_id = self._record_correction(
                message_key=message_key,
                base_fingerprint=base_fingerprint,
                product_path=product_path or "",
                record_reference=record_reference,
                field=field,
                old_value=None,
                new_value=value,
                node_path=node_path,
                rationale=rationale,
                status="failed",
                detail={"error": str(exc)},
            )
            return {"status": "failed", "correction_id": correction_id, "error": str(exc)}

        reference = onix.child_text(product, "RecordReference")
        candidates = onix.correction_candidates(product, resolved_path, field)

        if not candidates:
            error = f"no {field} candidate nodes in product {resolved_path}"
            correction_id = self._record_correction(
                message_key=message_key,
                base_fingerprint=base_fingerprint,
                product_path=resolved_path,
                record_reference=reference,
                field=field,
                old_value=None,
                new_value=value,
                node_path=node_path,
                rationale=rationale,
                status="failed",
                detail={"error": error},
            )
            return {"status": "failed", "correction_id": correction_id, "error": error}

        try:
            chosen = onix.choose_candidate(candidates, field, node_path)
        except ValueError as exc:
            detail = {"error": str(exc), "candidates": [c.to_dict() for c in candidates]}
            correction_id = self._record_correction(
                message_key=message_key,
                base_fingerprint=base_fingerprint,
                product_path=resolved_path,
                record_reference=reference,
                field=field,
                old_value=None,
                new_value=value,
                node_path=node_path,
                rationale=rationale,
                status="failed",
                detail=detail,
            )
            return {"status": "failed", "correction_id": correction_id, **detail}

        if chosen is None:
            detail = {
                "expected_role": onix.ROLE_EXPECTED[field],
                "candidates": [c.to_dict() for c in candidates],
            }
            correction_id = self._record_correction(
                message_key=message_key,
                base_fingerprint=base_fingerprint,
                product_path=resolved_path,
                record_reference=reference,
                field=field,
                old_value=None,
                new_value=value,
                node_path=node_path,
                rationale=rationale,
                status="ambiguous",
                detail=detail,
            )
            return {"status": "ambiguous", "correction_id": correction_id, **detail}

        correction_id = self._record_correction(
            message_key=message_key,
            base_fingerprint=base_fingerprint,
            product_path=resolved_path,
            record_reference=reference,
            field=field,
            old_value=chosen.value,
            new_value=value,
            node_path=chosen.path,
            rationale=rationale,
            status="pending",
            detail={},
        )

        try:
            onix.apply_text_correction(doc, chosen, value)
            new_xml = onix.serialize(doc)
            new_fingerprint = onix.fingerprint(new_xml)
            projections = onix.project_message(doc)
            header = onix.header_info(doc)
            observed_at = header["sent_at"] or datetime.now(timezone.utc).isoformat()
            source = f"onix:{header['sender'] or current['sender']}"

            self.store.begin()
            version = self.store.insert_version(
                message_key,
                current["sender"],
                new_xml,
                new_fingerprint,
                "correction",
                correction_id=correction_id,
            )

            rows: list[tuple[onix.ProductProjection, str | None]] = []
            changed = 0
            corrected_key: str | None = None
            for projection in projections:
                observation_key, upsert_changed = self._project_to_observation(
                    projection, source, observed_at
                )
                changed += int(upsert_changed)
                if projection.product_path == resolved_path:
                    corrected_key = observation_key
                rows.append((projection, observation_key))

            self.store.replace_projections(message_key, version, rows)
            self.store.finish_correction(
                correction_id,
                "applied",
                {"result_fingerprint": new_fingerprint, "result_version": version},
            )
            self.store.commit()
        except Exception as exc:
            self.store.rollback()
            self.store.finish_correction(
                correction_id,
                "failed",
                {"error": f"{type(exc).__name__}: {exc}"},
            )
            self.store.commit()
            raise

        return {
            "status": "applied",
            "correction_id": correction_id,
            "message_key": message_key,
            "version": version,
            "fingerprint": new_fingerprint,
            "base_fingerprint": base_fingerprint,
            "field": field,
            "old_value": chosen.value,
            "new_value": value,
            "node_path": chosen.path,
            "observation_key": corrected_key,
            "observations_changed": changed,
        }

    def export_message(self, message_key: str) -> dict[str, Any]:
        current = self._require_message(message_key)
        return {
            "message_key": message_key,
            "version": current["version"],
            "fingerprint": current["fingerprint"],
            "xml_text": current["xml_text"],
        }

    def _require_message(self, message_key: str) -> dict[str, Any]:
        current = self.store.current_message(message_key)
        if current is None:
            raise KeyError(f"unknown ONIX message: {message_key!r}")
        return current

    def _record_correction(self, **kwargs: Any) -> int:
        correction_id = self.store.insert_correction(**kwargs)
        self.store.commit()
        return correction_id

    def _saved_mapping(
        self, message_key: str, version: int, product_path: str, field: str
    ) -> dict[str, Any] | None:
        for product in self.store.products(message_key, version):
            if product["product_path"] != product_path:
                continue
            for mapping in self.store.mappings(product["id"]):
                if mapping["field"] == field:
                    return mapping
        return None
