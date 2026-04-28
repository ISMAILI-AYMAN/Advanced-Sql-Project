from __future__ import annotations

import csv
from pathlib import Path


def test_manifest_exists() -> None:
    path = Path("data/manifest/data_file_manifest.csv")
    assert path.exists()


def test_manifest_has_required_columns() -> None:
    path = Path("data/manifest/data_file_manifest.csv")
    with path.open("r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        expected = {
            "file_id",
            "file_path",
            "split",
            "checksum_sha256",
            "record_count",
            "size_bytes",
            "status",
        }
        assert reader.fieldnames is not None
        assert expected.issubset(set(reader.fieldnames))
