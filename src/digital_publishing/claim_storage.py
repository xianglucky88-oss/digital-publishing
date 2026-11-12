"""供货声明依据的 SQLite 追加式存储。

与 ``ObservationStore`` 的覆盖式 upsert 不同，这里只有 INSERT，没有 UPDATE/
DELETE：同来源、同产品版次、同一声明时刻出现不同内容时，因 claim_id（内容
哈希）不同会各自保留，冲突由裁决层识别，存储层绝不抹去其中一方。
"""

from __future__ import annotations

import sqlite3

from .models import AvailabilityClaim

_FIELDS = (
    "claim_id",
    "source",
    "isbn",
    "edition",
    "observed_at_raw",
    "observed_at_utc",
    "status",
    "status_raw",
    "note",
    "title",
    "language",
    "publisher",
)


class ClaimStore:
    """只负责声明依据的建表、追加写入和只读查询。"""

    def __init__(self, database: str):
        self.conn = sqlite3.connect(database)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS availability_claims (
                claim_id TEXT NOT NULL PRIMARY KEY,
                source TEXT NOT NULL,
                isbn TEXT NOT NULL COLLATE NOCASE,
                edition TEXT COLLATE NOCASE,
                observed_at_raw TEXT NOT NULL,
                observed_at_utc TEXT,
                status TEXT NOT NULL,
                status_raw TEXT,
                note TEXT,
                title TEXT,
                language TEXT,
                publisher TEXT,
                recorded_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_claims_product
                ON availability_claims(isbn, edition, observed_at_utc);
            """
        )
        self.conn.commit()

    def append(self, claim: AvailabilityClaim) -> bool:
        """追加一条声明。内容完全相同（同 claim_id）时跳过并返回 False；
        同刻不同内容因哈希不同会作为新行保留，不发生覆盖。"""
        placeholders = ", ".join("?" for _ in _FIELDS)
        columns = ", ".join(_FIELDS)
        before = self.conn.total_changes
        self.conn.execute(
            f"INSERT OR IGNORE INTO availability_claims({columns}) "
            f"VALUES ({placeholders})",
            tuple(getattr(claim, field) for field in _FIELDS),
        )
        self.conn.commit()
        return self.conn.total_changes > before

    def _hydrate(self, rows: list[sqlite3.Row]) -> list[AvailabilityClaim]:
        return [AvailabilityClaim(**{field: row[field] for field in _FIELDS}) for row in rows]

    def claims_for_product(self, isbn: str, edition: str) -> list[AvailabilityClaim]:
        """取某 ISBN 具体版次的声明：缺版次材料不会被自动并入。"""
        rows = self.conn.execute(
            """
            SELECT * FROM availability_claims
            WHERE isbn = ? AND edition = ? AND edition IS NOT NULL
            ORDER BY observed_at_utc ASC, recorded_at ASC, claim_id ASC
            """,
            (isbn, edition),
        ).fetchall()
        return self._hydrate(rows)

    def claims_for_isbn(self, isbn: str) -> list[AvailabilityClaim]:
        """取该 ISBN 下全部材料，包含缺版次声明，供证据展示使用。"""
        rows = self.conn.execute(
            """
            SELECT * FROM availability_claims
            WHERE isbn = ?
            ORDER BY observed_at_utc ASC, recorded_at ASC, claim_id ASC
            """,
            (isbn,),
        ).fetchall()
        return self._hydrate(rows)

    def distinct_cutoffs(self, isbn: str, edition: str) -> list[str]:
        """该产品版次上可进入时间线的全部声明时刻（UTC，升序）。"""
        rows = self.conn.execute(
            """
            SELECT DISTINCT observed_at_utc FROM availability_claims
            WHERE isbn = ? AND edition = ? AND edition IS NOT NULL
                  AND observed_at_utc IS NOT NULL
            ORDER BY observed_at_utc ASC
            """,
            (isbn, edition),
        ).fetchall()
        return [row[0] for row in rows]

    def counts(self) -> dict[str, int]:
        return {
            "claims": self.conn.execute(
                "SELECT COUNT(*) FROM availability_claims"
            ).fetchone()[0],
        }

    def close(self) -> None:
        self.conn.close()
