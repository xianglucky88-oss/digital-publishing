import json
import sqlite3
from pathlib import Path

import pytest

from digital_publishing.application import (
    run_fixture,
    run_onix_correct,
    run_onix_export,
    run_onix_import,
    run_onix_plan,
)
from digital_publishing.onix_service import OnixService
from digital_publishing.onix_storage import OnixStore

FIXTURES = Path(__file__).parents[1] / "fixtures"
ONIX = FIXTURES / "onix"


def _observations(database):
    conn = sqlite3.connect(database)
    try:
        rows = conn.execute(
            "SELECT source, record_key, payload_json FROM observations"
        ).fetchall()
    finally:
        conn.close()
    return {key: json.loads(payload) for _source, key, payload in rows}


def test_import_projects_subset_and_is_idempotent(tmp_path):
    db = str(tmp_path / "onix.sqlite3")
    result = run_onix_import(str(ONIX / "catalog_message.xml"), db)

    assert result["status"] == "imported"
    assert result["message_key"] == "Example Publisher"
    assert result["projected"] == 2
    assert result["rejected"] == 0

    observations = _observations(db)
    first = observations["9780000000001|2"]
    assert first["title"] == "Data Pipelines"
    assert first["language"] == "eng"
    assert first["publisher"] == "Example Press"
    assert first["edition"] == "2"
    assert first["availability"] == "20"
    assert observations["9780000000002|1"]["title"] == "采集工程"

    again = run_onix_import(str(ONIX / "catalog_message.xml"), db)
    assert again["status"] == "unchanged"
    assert again["observations_changed"] == 0
    assert again["version"] == result["version"]


def test_missing_edition_is_rejected_not_defaulted(tmp_path):
    db = str(tmp_path / "edition.sqlite3")
    result = run_onix_import(str(ONIX / "missing_edition.xml"), db)

    assert result["projected"] == 1
    assert result["rejected"] == 1

    by_ref = {p["record_reference"]: p for p in result["products"]}
    rejected = by_ref["book-2001"]
    assert rejected["status"] == "rejected"
    assert any("edition" in reason and "not defaulted" in reason
               for reason in rejected["reasons"])
    assert by_ref["book-2002"]["status"] == "projected"

    keys = set(_observations(db))
    assert "9780000002001|1" not in keys
    assert not any(key.startswith("9780000002001|") for key in keys)
    assert "9780000002002|4" in keys

    store = OnixStore(db)
    try:
        products = {
            p["record_reference"]: p
            for p in store.products("Sparse Metadata Books", result["version"])
        }
        assert products["book-2001"]["status"] == "rejected"
        assert any("edition" in reason for reason in products["book-2001"]["reasons"])
        assert products["book-2002"]["status"] == "projected"
    finally:
        store.close()


def test_duplicate_roles_are_not_merged_and_require_explicit_choice(tmp_path):
    db = str(tmp_path / "roles.sqlite3")
    result = run_onix_import(str(ONIX / "duplicate_roles.xml"), db)

    by_ref = {p["record_reference"]: p for p in result["products"]}
    assert by_ref["book-1001"]["status"] == "projected"
    assert any("language" in issue for issue in by_ref["book-1001"]["issues"])
    assert by_ref["book-1002"]["status"] == "rejected"
    assert any("publisher" in reason for reason in by_ref["book-1002"]["reasons"])

    assert _observations(db)["9780000001001|1"]["language"] is None

    fingerprint = result["fingerprint"]
    ambiguous = run_onix_correct(
        "Role Clash Books",
        db,
        field="language",
        value="spa",
        base_fingerprint=fingerprint,
        product_ref="book-1001",
    )
    assert ambiguous["status"] == "ambiguous"
    assert len(ambiguous["candidates"]) == 2
    assert {c["value"] for c in ambiguous["candidates"]} == {"eng", "fre"}
    assert all(c["path"] and c["line"] > 0 for c in ambiguous["candidates"])

    store = OnixStore(db)
    try:
        assert store.current_message("Role Clash Books")["fingerprint"] == fingerprint
        recorded = store.corrections("Role Clash Books")
        assert recorded[-1]["status"] == "ambiguous"
        assert len(json.loads(recorded[-1]["detail_json"])["candidates"]) == 2
    finally:
        store.close()

    chosen = ambiguous["candidates"][0]
    applied = run_onix_correct(
        "Role Clash Books",
        db,
        field="language",
        value="spa",
        base_fingerprint=fingerprint,
        product_ref="book-1001",
        node_path=chosen["path"],
    )
    assert applied["status"] == "applied"

    exported = run_onix_export("Role Clash Books", db, str(tmp_path / "roles_out.xml"))
    text = Path(exported["output"]).read_text(encoding="utf-8")
    assert "<LanguageCode>spa</LanguageCode>" in text
    assert "<LanguageCode>fre</LanguageCode>" in text

    plan = run_onix_correct(
        "Role Clash Books",
        db,
        field="publisher",
        value="Unified Press",
        base_fingerprint=applied["fingerprint"],
        product_ref="book-1002",
    )
    assert plan["status"] == "ambiguous"
    assert {c["value"] for c in plan["candidates"]} == {"Role Clash Books", "Mirror Press"}


def test_correction_export_reimport_round_trip(tmp_path):
    db = str(tmp_path / "round.sqlite3")
    imported = run_onix_import(str(ONIX / "catalog_message.xml"), db)
    fingerprint = imported["fingerprint"]

    plan = run_onix_plan(
        "Example Publisher",
        db,
        field="title",
        product_ref="book-0001",
    )
    assert plan["status"] == "ok"
    assert plan["fingerprint"] == fingerprint
    assert len(plan["candidates"]) == 2
    assert plan["mapped"]["node_path"].endswith("TitleText[1]")
    assert plan["mapped"]["value"] == "Data Pipelines"

    corrected_title = run_onix_correct(
        "Example Publisher",
        db,
        field="title",
        value="Data Pipelines (Revised)",
        base_fingerprint=fingerprint,
        product_ref="book-0001",
        rationale="编辑订正：与出版社 2026-11-10 邮件一致",
    )
    assert corrected_title["status"] == "applied"
    assert corrected_title["old_value"] == "Data Pipelines"

    corrected_publisher = run_onix_correct(
        "Example Publisher",
        db,
        field="publisher",
        value="Example Press International",
        base_fingerprint=corrected_title["fingerprint"],
        product_ref="book-0002",
    )
    assert corrected_publisher["status"] == "applied"

    corrected_language = run_onix_correct(
        "Example Publisher",
        db,
        field="language",
        value="spa",
        base_fingerprint=corrected_publisher["fingerprint"],
        product_ref="book-0001",
    )
    assert corrected_language["status"] == "applied"

    observations = _observations(db)
    assert observations["9780000000001|2"]["title"] == "Data Pipelines (Revised)"
    assert observations["9780000000001|2"]["language"] == "spa"
    assert observations["9780000000002|1"]["publisher"] == "Example Press International"

    output = tmp_path / "corrected.xml"
    run_onix_export("Example Publisher", db, str(output))
    text = output.read_text(encoding="utf-8")
    assert "Data Pipelines (Revised)" in text
    assert "<TitleText>数据管道</TitleText>" in text
    assert "PersonNameInverted" in text
    assert "<ExtentValue>320</ExtentValue>" in text
    assert 'x:origin="publisher-portal"' in text
    assert "InternalSku" in text
    assert "editor note" in text
    assert '<?render mode="full"?>' in text

    db2 = str(tmp_path / "round2.sqlite3")
    reimported = run_onix_import(str(output), db2)
    assert reimported["status"] == "imported"
    observations2 = _observations(db2)
    assert observations2["9780000000001|2"]["title"] == "Data Pipelines (Revised)"
    assert observations2["9780000000001|2"]["language"] == "spa"
    assert observations2["9780000000002|1"]["publisher"] == "Example Press International"
    assert run_onix_import(str(output), db2)["observations_changed"] == 0

    store = OnixStore(db)
    try:
        corrections = store.corrections("Example Publisher")
        assert [c["status"] for c in corrections] == ["applied"] * 3
        assert corrections[0]["rationale"] == "编辑订正：与出版社 2026-11-10 邮件一致"
        assert corrections[0]["base_fingerprint"] == fingerprint
        assert corrections[0]["node_path"].endswith("TitleText[1]")
        detail = json.loads(corrections[2]["detail_json"])
        assert detail["result_fingerprint"] == corrected_language["fingerprint"]
    finally:
        store.close()


def test_stale_base_fingerprint_conflicts_after_source_update(tmp_path):
    db = str(tmp_path / "conflict.sqlite3")
    v1 = run_onix_import(str(ONIX / "catalog_message.xml"), db)
    v2 = run_onix_import(str(ONIX / "publisher_update.xml"), db)
    assert v2["version"] == 2
    assert v2["fingerprint"] != v1["fingerprint"]
    assert _observations(db)["9780000000001|2"]["title"] == "Data Pipelines Deluxe"

    stale = run_onix_correct(
        "Example Publisher",
        db,
        field="title",
        value="Hacked Title",
        base_fingerprint=v1["fingerprint"],
        product_ref="book-0001",
    )
    assert stale["status"] == "conflict"
    assert stale["current_fingerprint"] == v2["fingerprint"]
    assert stale["current_version"] == 2

    store = OnixStore(db)
    try:
        current = store.current_message("Example Publisher")
        assert current["version"] == 2
        assert current["fingerprint"] == v2["fingerprint"]
        assert "Hacked Title" not in current["xml_text"]
        conflicts = [c for c in store.corrections("Example Publisher")
                     if c["status"] == "conflict"]
        assert len(conflicts) == 1
        assert conflicts[0]["base_fingerprint"] == v1["fingerprint"]
    finally:
        store.close()

    assert _observations(db)["9780000000001|2"]["title"] == "Data Pipelines Deluxe"

    applied = run_onix_correct(
        "Example Publisher",
        db,
        field="title",
        value="Data Pipelines Deluxe (Revised)",
        base_fingerprint=v2["fingerprint"],
        product_ref="book-0001",
    )
    assert applied["status"] == "applied"
    assert _observations(db)["9780000000001|2"]["title"] == \
        "Data Pipelines Deluxe (Revised)"


def test_failed_commit_rolls_back_and_preserves_evidence(tmp_path, monkeypatch):
    db = str(tmp_path / "failure.sqlite3")
    imported = run_onix_import(str(ONIX / "catalog_message.xml"), db)
    fingerprint = imported["fingerprint"]

    store = OnixStore(db)
    service = OnixService(store)

    def boom(*_args, **_kwargs):
        raise RuntimeError("simulated sink failure")

    monkeypatch.setattr(store, "upsert", boom)

    with pytest.raises(RuntimeError, match="simulated sink failure"):
        service.commit_correction(
            "Example Publisher",
            field="title",
            new_value="Nope",
            base_fingerprint=fingerprint,
            record_reference="book-0001",
            rationale="中途失败演练",
        )

    monkeypatch.undo()

    current = store.current_message("Example Publisher")
    assert current["version"] == 1
    assert current["fingerprint"] == fingerprint
    assert "Nope" not in current["xml_text"]

    observations = {
        key: json.loads(payload)
        for key, payload in store.conn.execute(
            "SELECT record_key, payload_json FROM observations"
        )
    }
    assert observations["9780000000001|2"]["title"] == "Data Pipelines"

    corrections = store.corrections("Example Publisher")
    assert len(corrections) == 1
    failed = corrections[0]
    assert failed["status"] == "failed"
    assert failed["rationale"] == "中途失败演练"
    assert failed["old_value"] == "Data Pipelines"
    assert failed["new_value"] == "Nope"
    assert failed["node_path"].endswith("TitleText[1]")
    assert failed["base_fingerprint"] == fingerprint
    assert "simulated sink failure" in json.loads(failed["detail_json"])["error"]
    store.close()


def test_json_fixture_ingestion_coexists_with_onix(tmp_path):
    db = str(tmp_path / "mixed.sqlite3")
    run_onix_import(str(ONIX / "catalog_message.xml"), db)
    result = run_fixture(str(FIXTURES / "sample.json"), db)
    assert result["processed"] == 3
    assert result["observations"] == 4
    assert result["rejected"] == 1
