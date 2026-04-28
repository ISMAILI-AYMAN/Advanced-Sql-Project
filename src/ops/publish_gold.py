from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.common.db import get_connection


def execute_sql_file(path: Path) -> None:
    sql_text = path.read_text(encoding="utf-8")
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql_text)
            if "post_publish_checks" in path.name:
                for row in cur.fetchall():
                    check_name, status = row
                    if status != "pass":
                        raise RuntimeError(f"Post publish check failed: {check_name}")


if __name__ == "__main__":
    execute_sql_file(PROJECT_ROOT / "sql" / "publish" / "publish_gold.sql")
    execute_sql_file(PROJECT_ROOT / "sql" / "publish" / "post_publish_checks.sql")
    print("Gold publish complete.")
