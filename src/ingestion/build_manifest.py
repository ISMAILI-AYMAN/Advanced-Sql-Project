from __future__ import annotations

import csv
import sys
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Final

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.ingestion.validate_layout import file_checksum, validate_project_layout


SPLIT_DIRS: Final[dict[str, str]] = {
    "train": "data/raw/train",
    "test": "data/raw/test",
    "truth": "data/raw/truth",
}

MANIFEST_HEADERS: Final[tuple[str, ...]] = (
    "file_id",
    "file_path",
    "split",
    "checksum_sha256",
    "record_count",
    "size_bytes",
    "status",
)


@dataclass(frozen=True)
class ManifestRecord:
    file_id: str
    file_path: str
    split: str
    checksum_sha256: str
    record_count: int
    size_bytes: int
    status: str


def count_records(path: Path) -> int:
    with path.open("r", encoding="utf-8", errors="ignore") as handle:
        return sum(1 for _ in handle)


def build_records(root: Path) -> list[ManifestRecord]:
    records: list[ManifestRecord] = []
    for split, rel_dir in SPLIT_DIRS.items():
        for file_path in sorted((root / rel_dir).glob("*.txt")):
            records.append(
                ManifestRecord(
                    file_id=str(uuid.uuid4()),
                    file_path=str(file_path.relative_to(root)).replace("\\", "/"),
                    split=split,
                    checksum_sha256=file_checksum(file_path),
                    record_count=count_records(file_path),
                    size_bytes=file_path.stat().st_size,
                    status="approved",
                )
            )
    return records


def write_manifest(root: Path, records: list[ManifestRecord]) -> Path:
    output_dir = root / "data" / "manifest"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "data_file_manifest.csv"
    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(MANIFEST_HEADERS)
        for record in records:
            writer.writerow(
                (
                    record.file_id,
                    record.file_path,
                    record.split,
                    record.checksum_sha256,
                    record.record_count,
                    record.size_bytes,
                    record.status,
                )
            )
    return output_path


if __name__ == "__main__":
    project_root = PROJECT_ROOT
    validation = validate_project_layout(project_root)
    if not validation.ok:
        for message in validation.errors:
            print(message)
        raise SystemExit(1)
    records = build_records(project_root)
    manifest_path = write_manifest(project_root, records)
    print(f"Manifest generated at {manifest_path}")
