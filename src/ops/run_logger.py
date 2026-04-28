from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.common.db import get_connection
from src.common.settings import load_settings


def log_pipeline_task(
    run_id: str,
    task_name: str,
    status: str,
    rows_in: int | None = None,
    rows_out: int | None = None,
    metadata: dict[str, Any] | None = None,
) -> None:
    settings = load_settings()
    now = datetime.now(timezone.utc)
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                insert into ops.pipeline_run_log (
                    run_id, pipeline_version, task_name, status,
                    rows_in, rows_out, metadata, started_at_utc, finished_at_utc
                ) values (%s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s)
                """,
                (
                    run_id,
                    settings.pipeline_version,
                    task_name,
                    status,
                    rows_in,
                    rows_out,
                    "{}" if metadata is None else str(metadata).replace("'", '"'),
                    now,
                    now,
                ),
            )
