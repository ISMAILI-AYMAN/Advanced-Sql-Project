from __future__ import annotations

from src.ingestion.load_bronze import parse_telemetry_row


def test_parse_telemetry_row_expected_column_count() -> None:
    row = " ".join(str(float(i)) for i in range(26))
    parsed = parse_telemetry_row(row)
    assert len(parsed) == 26


def test_parse_telemetry_row_invalid_short_row() -> None:
    short_row = " ".join(str(float(i)) for i in range(20))
    try:
        parse_telemetry_row(short_row)
    except ValueError:
        assert True
        return
    raise AssertionError("Expected ValueError for malformed row.")
