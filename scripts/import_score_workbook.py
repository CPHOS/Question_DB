from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from question_bank.config import DEFAULT_DB_PATH, RAW_DIR
from question_bank.workbooks import upsert_score_workbook


def main() -> None:
    parser = argparse.ArgumentParser(description="把 xlsx 统计工作簿写入数据库。")
    parser.add_argument("workbook_path", type=Path, help="xlsx 文件路径。")
    parser.add_argument("paper_id", help="所属试卷 ID。")
    parser.add_argument("exam_session", help="场次标签。")
    parser.add_argument("workbook_kind", help="工作簿类型，例如 score_table 或 paper_registry。")
    parser.add_argument("workbook_id", help="工作簿唯一 ID。")
    parser.add_argument("--db-path", default=str(DEFAULT_DB_PATH), help="SQLite 数据库路径。")
    parser.add_argument("--notes", default="", help="备注。")
    args = parser.parse_args()

    bundle_root = args.workbook_path.resolve().parent
    workbook = {
        "workbook_id": args.workbook_id,
        "exam_session": args.exam_session,
        "workbook_kind": args.workbook_kind,
        "file_path": args.workbook_path.name,
        "notes": args.notes,
    }
    storage_root = RAW_DIR if str(args.workbook_path.resolve()).startswith(str(RAW_DIR.resolve())) else None
    upsert_score_workbook(
        Path(args.db_path),
        paper_id=args.paper_id,
        workbook=workbook,
        bundle_path=bundle_root,
        storage_root=storage_root,
    )
    print(f"已写入 workbook: {args.workbook_id}")


if __name__ == "__main__":
    main()
