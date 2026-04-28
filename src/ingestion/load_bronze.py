from __future__ import annotations

import csv
import sys
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Final, Iterable

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.common.db import get_connection
from src.ingestion.validate_layout import validate_project_layout

EXPECTED_TELEMETRY_COLS: Final[int] = 26


@dataclass(frozen=True)
class IngestionTarget:
    split: str
    file_glob: str
    table_name: str


TELEMETRY_TARGETS: Final[tuple[IngestionTarget, ...]] = (
    IngestionTarget("train", "train_*.txt", "bronze.raw_train_data"),
    IngestionTarget("test", "test_*.txt", "bronze.raw_test_data"),
)


def parse_telemetry_row(row_text: str) -> list[float]:
    parts = [token for token in row_text.strip().split(" ") if token]
    if len(parts) < EXPECTED_TELEMETRY_COLS:
        raise ValueError(
            f"Malformed telemetry row. Expected {EXPECTED_TELEMETRY_COLS}, got {len(parts)}"
        )
    return [float(parts[i]) for i in range(EXPECTED_TELEMETRY_COLS)]


def parse_truth_rows(file_path: Path) -> Iterable[tuple[int, int]]:
    with file_path.open("r", encoding="utf-8", errors="ignore") as handle:
        for index, line in enumerate(handle, start=1):
            parts = [token for token in line.strip().split(" ") if token]
            if not parts:
                continue
            yield index, int(float(parts[0]))


def insert_audit_log(
    run_id: str,
    ingestion_id: str,
    split: str,
    source_file_name: str,
    status: str,
    rows_loaded: int,
    started_at: datetime,
    finished_at: datetime | None,
    error_message: str | None = None,
) -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                insert into ops.ingestion_audit_log (
                    run_id, ingestion_id, split, source_file_name, status, rows_loaded,
                    error_message, started_at_utc, finished_at_utc
                ) values (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    run_id,
                    ingestion_id,
                    split,
                    source_file_name,
                    status,
                    rows_loaded,
                    error_message,
                    started_at,
                    finished_at,
                ),
            )


def load_telemetry_file(target: IngestionTarget, file_path: Path, run_id: str) -> None:
    ingestion_id = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc)
    rows_loaded = 0
    try:
        with get_connection() as conn:
            with conn.cursor() as cur, file_path.open(
                "r", encoding="utf-8", errors="ignore"
            ) as handle:
                for row_num, line in enumerate(handle, start=1):
                    values = parse_telemetry_row(line)
                    cur.execute(
                        f"""
                        insert into {target.table_name} values (
                            %s, %s, %s, %s,
                            %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                        """,
                        (
                            ingestion_id,
                            started_at,
                            file_path.name,
                            row_num,
                            *values,
                        ),
                    )
                    rows_loaded += 1
        insert_audit_log(
            run_id=run_id,
            ingestion_id=ingestion_id,
            split=target.split,
            source_file_name=file_path.name,
            status="success",
            rows_loaded=rows_loaded,
            started_at=started_at,
            finished_at=datetime.now(timezone.utc),
        )
    except Exception as exc:
        insert_audit_log(
            run_id=run_id,
            ingestion_id=ingestion_id,
            split=target.split,
            source_file_name=file_path.name,
            status="failed",
            rows_loaded=rows_loaded,
            started_at=started_at,
            finished_at=datetime.now(timezone.utc),
            error_message=str(exc),
        )
        raise


def load_truth_file(file_path: Path, run_id: str) -> None:
    ingestion_id = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc)
    rows_loaded = 0
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                for unit_id, rul_value in parse_truth_rows(file_path):
                    cur.execute(
                        """
                        insert into bronze.raw_rul_truth (
                            ingestion_id, ingestion_ts_utc, source_file_name,
                            source_row_num, unit_id, rul_truth
                        ) values (%s, %s, %s, %s, %s, %s)
                        """,
                        (
                            ingestion_id,
                            started_at,
                            file_path.name,
                            unit_id,
                            unit_id,
                            rul_value,
                        ),
                    )
                    rows_loaded += 1
        insert_audit_log(
            run_id=run_id,
            ingestion_id=ingestion_id,
            split="truth",
            source_file_name=file_path.name,
            status="success",
            rows_loaded=rows_loaded,
            started_at=started_at,
            finished_at=datetime.now(timezone.utc),
        )
    except Exception as exc:
        insert_audit_log(
            run_id=run_id,
            ingestion_id=ingestion_id,
            split="truth",
            source_file_name=file_path.name,
            status="failed",
            rows_loaded=rows_loaded,
            started_at=started_at,
            finished_at=datetime.now(timezone.utc),
            error_message=str(exc),
        )
        raise


def check_manifest_gate() -> bool:
    manifest_path = PROJECT_ROOT / "data" / "manifest" / "data_file_manifest.csv"
    if not manifest_path.exists():
        return False
    with manifest_path.open("r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        statuses = [row.get("status", "").strip().lower() for row in reader]
    return bool(statuses) and all(status == "approved" for status in statuses)


def main() -> None:
    layout = validate_project_layout(PROJECT_ROOT)
    if not layout.ok:
        raise RuntimeError("Layout validation failed before ingestion.")
    if not check_manifest_gate():
        raise RuntimeError("Manifest gate failed: all files must be approved.")

    run_id = str(uuid.uuid4())
    for target in TELEMETRY_TARGETS:
        directory = PROJECT_ROOT / "data" / "raw" / target.split
        for file_path in sorted(directory.glob(target.file_glob)):
            load_telemetry_file(target, file_path, run_id)

    truth_dir = PROJECT_ROOT / "data" / "raw" / "truth"
    for file_path in sorted(truth_dir.glob("RUL_*.txt")):
        load_truth_file(file_path, run_id)

    print(f"Bronze ingestion completed. run_id={run_id}")


if __name__ == "__main__":
    main()
