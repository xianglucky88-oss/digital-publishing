import json
import sqlite3
from .models import Observation


class ObservationStore:
    """只负责数据库 schema、幂等写入、拒绝记录和统计查询。"""

    def __init__(self, database: str):
        self.conn = sqlite3.connect(database)
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS observations (
                source TEXT NOT NULL,
                record_key TEXT NOT NULL,
                observed_at TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                raw_hash TEXT NOT NULL,
                first_seen_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (source, record_key, observed_at)
            );
            CREATE TABLE IF NOT EXISTS rejected_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT,
                observed_at TEXT,
                error TEXT NOT NULL,
                raw_json TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
        """)
        self.conn.commit()

    def upsert(self, observation: Observation) -> bool:
        before = self.conn.total_changes
        self.conn.execute("""
            INSERT INTO observations(source, record_key, observed_at, payload_json, raw_hash)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(source, record_key, observed_at) DO UPDATE SET
                payload_json = excluded.payload_json,
                raw_hash = excluded.raw_hash
            WHERE observations.raw_hash <> excluded.raw_hash
        """, (observation.source, observation.key, observation.observed_at,
              json.dumps(observation.payload, ensure_ascii=False, sort_keys=True), observation.raw_hash))
        self.conn.commit()
        return self.conn.total_changes > before

    def reject(self, raw: object, error: str) -> None:
        record = raw if isinstance(raw, dict) else {"value": raw}
        self.conn.execute("INSERT INTO rejected_records(source, observed_at, error, raw_json) VALUES (?, ?, ?, ?)",
                          (record.get("source"), record.get("observed_at"), error,
                           json.dumps(record, ensure_ascii=False, sort_keys=True)))
        self.conn.commit()

    def counts(self) -> dict[str, int]:
        return {
            "observations": self.conn.execute("SELECT COUNT(*) FROM observations").fetchone()[0],
            "rejected": self.conn.execute("SELECT COUNT(*) FROM rejected_records").fetchone()[0],
        }

    def close(self) -> None:
        self.conn.close()
