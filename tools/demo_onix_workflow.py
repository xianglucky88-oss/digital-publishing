"""端到端演示：ONIX 3.0 消息导入 → 指定节点订正 → 导出 → 再导入。

只用标准库，直接复用应用组合根；在临时目录中运行，不写仓库文件。

    python3 -I tools/demo_onix_workflow.py
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
import sys

sys.path.insert(0, str(ROOT / "src"))

from digital_publishing.application import (  # noqa: E402
    run_onix_correct,
    run_onix_export,
    run_onix_import,
    run_onix_plan,
)

ONIX = ROOT / "fixtures" / "onix"


def show(title: str, result: dict) -> None:
    print(f"\n=== {title} ===")
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2))


def main() -> None:
    work = Path(tempfile.mkdtemp(prefix="onix-demo-"))
    db = str(work / "demo.sqlite3")

    imported = run_onix_import(str(ONIX / "catalog_message.xml"), db)
    show("1. 导入多产品/多语种/含未映射扩展的消息", {
        "status": imported["status"],
        "projected": imported["projected"],
        "rejected": imported["rejected"],
        "fingerprint": imported["fingerprint"],
    })
    fingerprint = imported["fingerprint"]

    plan = run_onix_plan(
        "Example Publisher", db, field="title", product_ref="book-0001"
    )
    show("2. 订正计划：返回候选原文位置与已保存映射，不改动消息", {
        "status": plan["status"],
        "candidates": plan["candidates"],
        "mapped_node_path": plan["mapped"]["node_path"],
    })

    corrected = run_onix_correct(
        "Example Publisher",
        db,
        field="title",
        value="Data Pipelines (Revised)",
        base_fingerprint=fingerprint,
        product_ref="book-0001",
        rationale="演示订正：编辑指定 01 角色标题节点",
    )
    show("3. 提交订正（带基准指纹，单事务落库新版本）", {
        "status": corrected["status"],
        "old_value": corrected["old_value"],
        "new_value": corrected["new_value"],
        "node_path": corrected["node_path"],
        "version": corrected["version"],
    })

    exported = run_onix_export(
        "Example Publisher", db, str(work / "corrected.xml")
    )
    text = Path(exported["output"]).read_text(encoding="utf-8")
    show("4. 导出消息（未映射内容按语义保留）", {
        "status": exported["status"],
        "output": exported["output"],
        "revised_title_present": "Data Pipelines (Revised)" in text,
        "untranslated_title_present": "<TitleText>数据管道</TitleText>" in text,
        "extension_present": 'x:origin="publisher-portal"' in text,
        "comment_present": "editor note" in text,
        "processing_instruction_present": '<?render mode="full"?>' in text,
    })

    reimported = run_onix_import(exported["output"], str(work / "reimport.sqlite3"))
    show("5. 导出消息再导入：对方仍可识别，目标字段为订正后的值", {
        "status": reimported["status"],
        "projected": reimported["projected"],
        "products": [
            {"ref": p["record_reference"], "status": p["status"]}
            for p in reimported["products"]
        ],
    })

    print(f"\n演示产物目录：{work}")


if __name__ == "__main__":
    main()
