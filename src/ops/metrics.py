from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.common.db import get_connection


def table_row_count(table_name: str) -> int:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"select count(*) from {table_name}")
            value = cur.fetchone()
            return int(value[0]) if value is not None else 0
