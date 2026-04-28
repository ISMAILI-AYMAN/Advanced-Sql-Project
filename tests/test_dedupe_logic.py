from __future__ import annotations


def dedupe_key(unit_id: int, cycle: int, ingestion_ts: int, source_row_num: int) -> tuple[int, int, int, int]:
    return (unit_id, cycle, -ingestion_ts, -source_row_num)


def test_dedupe_prefers_latest_ingestion_then_source_row_num() -> None:
    rows = [
        (1, 5, 100, 8),
        (1, 5, 101, 1),
        (1, 5, 101, 9),
    ]
    winner = sorted(rows, key=lambda r: dedupe_key(*r))[0]
    assert winner == (1, 5, 101, 9)
