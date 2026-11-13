"""ONIX 消息版本、投影映射与订正依据的持久化。

继承 ObservationStore 复用观测表与拒绝记录表，并新增：

- ``onix_messages`` / ``onix_message_versions``：原消息全文与版本指纹；
- ``onix_products`` / ``onix_mappings``：每个产品的投影结果、拒绝原因，
  以及字段到源节点（路径 + 行号）的关系；
- ``onix_corrections``：订正依据（基准指纹、目标节点、旧值/新值、理由、
  结果或失败/冲突细节）。

写方法不自行提交，由服务层在一个事务内统一 commit/rollback，
保证目录观测与消息版本不会只改一半。
"""

from __future__ import annotations

import json
from typing import Any

from .onix import ProductProjection
from .storage import ObservationStore


class OnixStore(ObservationStore):
    def __init__(self, database: str):
        super().__init__(database)
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS onix_messages (
                message_key TEXT PRIMARY KEY,
                sender TEXT NOT NULL,
                current_version INTEGER NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS onix_message_versions (
                message_key TEXT NOT NULL,
                version INTEGER NOT NULL,
                fingerprint TEXT NOT NULL,
                xml_text TEXT NOT NULL,
                source_kind TEXT NOT NULL,
                correction_id INTEGER,
                imported_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (message_key, version)
            );
            CREATE TABLE IF NOT EXISTS onix_products (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                message_key TEXT NOT NULL,
                version INTEGER NOT NULL,
                product_index INTEGER NOT NULL,
                product_path TEXT NOT NULL,
                record_reference TEXT,
                status TEXT NOT NULL,
                observation_key TEXT,
                reasons_json TEXT NOT NULL DEFAULT '[]',
                issues_json TEXT NOT NULL DEFAULT '[]'
            );
            CREATE TABLE IF NOT EXISTS onix_mappings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_id INTEGER NOT NULL REFERENCES onix_products(id),
                field TEXT NOT NULL,
                value TEXT,
                node_path TEXT,
                node_line INTEGER,
                status TEXT NOT NULL,
                candidates_json TEXT NOT NULL DEFAULT '[]'
            );
            CREATE TABLE IF NOT EXISTS onix_corrections (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                message_key TEXT NOT NULL,
                base_fingerprint TEXT NOT NULL,
                product_path TEXT NOT NULL,
                record_reference TEXT,
                field TEXT NOT NULL,
                old_value TEXT,
                new_value TEXT NOT NULL,
                node_path TEXT,
                rationale TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL,
                detail_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
        """)
        self.conn.commit()

    def begin(self) -> None:
        self.conn.execute("BEGIN")

    def commit(self) -> None:
        self.conn.commit()

    def rollback(self) -> None:
        self.conn.rollback()

    def current_message(self, message_key: str) -> dict[str, Any] | None:
        row = self.conn.execute("""
            SELECT m.message_key, m.sender, m.current_version, v.fingerprint, v.xml_text
            FROM onix_messages m
            JOIN onix_message_versions v
              ON v.message_key = m.message_key AND v.version = m.current_version
            WHERE m.message_key = ?
        """, (message_key,)).fetchone()
        if row is None:
            return None
        return {
            "message_key": row[0],
            "sender": row[1],
            "version": row[2],
            "fingerprint": row[3],
            "xml_text": row[4],
        }

    def insert_version(
        self,
        message_key: str,
        sender: str,
        xml_text: str,
        fingerprint: str,
        source_kind: str,
        correction_id: int | None = None,
    ) -> int:
        version = self.conn.execute(
            "SELECT COALESCE(MAX(version), 0) + 1 FROM onix_message_versions WHERE message_key = ?",
            (message_key,),
        ).fetchone()[0]
        self.conn.execute("""
            INSERT INTO onix_messages(message_key, sender, current_version) VALUES (?, ?, ?)
            ON CONFLICT(message_key) DO UPDATE SET
                sender = excluded.sender,
                current_version = excluded.current_version
        """, (message_key, sender, version))
        self.conn.execute("""
            INSERT INTO onix_message_versions(message_key, version, fingerprint, xml_text, source_kind, correction_id)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (message_key, version, fingerprint, xml_text, source_kind, correction_id))
        return version

    def replace_projections(
        self,
        message_key: str,
        version: int,
        rows: list[tuple[ProductProjection, str | None]],
    ) -> None:
        self.conn.execute("""
            DELETE FROM onix_mappings WHERE product_id IN
                (SELECT id FROM onix_products WHERE message_key = ? AND version = ?)
        """, (message_key, version))
        self.conn.execute(
            "DELETE FROM onix_products WHERE message_key = ? AND version = ?",
            (message_key, version),
        )
        for projection, observation_key in rows:
            cursor = self.conn.execute("""
                INSERT INTO onix_products(
                    message_key, version, product_index, product_path, record_reference,
                    status, observation_key, reasons_json, issues_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                message_key,
                version,
                projection.product_index,
                projection.product_path,
                projection.record_reference,
                "projected" if observation_key else "rejected",
                observation_key,
                json.dumps(projection.reasons, ensure_ascii=False),
                json.dumps(projection.issues, ensure_ascii=False),
            ))
            product_id = cursor.lastrowid
            for name, field_projection in projection.fields.items():
                self.conn.execute("""
                    INSERT INTO onix_mappings(product_id, field, value, node_path, node_line, status, candidates_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    product_id,
                    name,
                    field_projection.value,
                    field_projection.node_path,
                    field_projection.node_line,
                    field_projection.status,
                    json.dumps([c.to_dict() for c in field_projection.candidates], ensure_ascii=False),
                ))

    def products(self, message_key: str, version: int) -> list[dict[str, Any]]:
        rows = self.conn.execute("""
            SELECT id, product_index, product_path, record_reference, status,
                   observation_key, reasons_json, issues_json
            FROM onix_products WHERE message_key = ? AND version = ?
            ORDER BY product_index
        """, (message_key, version)).fetchall()
        return [
            {
                "id": row[0],
                "product_index": row[1],
                "product_path": row[2],
                "record_reference": row[3],
                "status": row[4],
                "observation_key": row[5],
                "reasons": json.loads(row[6]),
                "issues": json.loads(row[7]),
            }
            for row in rows
        ]

    def mappings(self, product_id: int) -> list[dict[str, Any]]:
        rows = self.conn.execute("""
            SELECT field, value, node_path, node_line, status, candidates_json
            FROM onix_mappings WHERE product_id = ? ORDER BY id
        """, (product_id,)).fetchall()
        return [
            {
                "field": row[0],
                "value": row[1],
                "node_path": row[2],
                "node_line": row[3],
                "status": row[4],
                "candidates": json.loads(row[5]),
            }
            for row in rows
        ]

    def insert_correction(
        self,
        *,
        message_key: str,
        base_fingerprint: str,
        product_path: str,
        record_reference: str | None,
        field: str,
        old_value: str | None,
        new_value: str,
        node_path: str | None,
        rationale: str,
        status: str,
        detail: dict[str, Any],
    ) -> int:
        cursor = self.conn.execute("""
            INSERT INTO onix_corrections(
                message_key, base_fingerprint, product_path, record_reference,
                field, old_value, new_value, node_path, rationale, status, detail_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            message_key,
            base_fingerprint,
            product_path,
            record_reference,
            field,
            old_value,
            new_value,
            node_path,
            rationale,
            status,
            json.dumps(detail, ensure_ascii=False),
        ))
        return cursor.lastrowid

    def finish_correction(self, correction_id: int, status: str, detail: dict[str, Any]) -> None:
        self.conn.execute(
            "UPDATE onix_corrections SET status = ?, detail_json = ? WHERE id = ?",
            (status, json.dumps(detail, ensure_ascii=False), correction_id),
        )

    def corrections(self, message_key: str) -> list[dict[str, Any]]:
        rows = self.conn.execute("""
            SELECT id, message_key, base_fingerprint, product_path, record_reference,
                   field, old_value, new_value, node_path, rationale, status, detail_json, created_at
            FROM onix_corrections WHERE message_key = ? ORDER BY id
        """, (message_key,)).fetchall()
        columns = [
            "id", "message_key", "base_fingerprint", "product_path", "record_reference",
            "field", "old_value", "new_value", "node_path", "rationale", "status",
            "detail_json", "created_at",
        ]
        return [dict(zip(columns, row)) for row in rows]
