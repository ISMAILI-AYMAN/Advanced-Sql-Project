from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Final


REQUIRED_DIRS: Final[tuple[str, ...]] = (
    "data/raw/train",
    "data/raw/test",
    "data/raw/truth",
    "data/staging",
    "data/archive",
)

REQUIRED_GLOBS: Final[dict[str, str]] = {
    "train": "train_*.txt",
    "test": "test_*.txt",
    "truth": "RUL_*.txt",
}


@dataclass(frozen=True)
class LayoutValidationResult:
    ok: bool
    errors: list[str]


def file_checksum(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8192), b""):
            digest.update(chunk)
    return digest.hexdigest()


def validate_project_layout(root: Path) -> LayoutValidationResult:
    errors: list[str] = []
    for relative in REQUIRED_DIRS:
        if not (root / relative).exists():
            errors.append(f"Missing required directory: {relative}")

    for split, pattern in REQUIRED_GLOBS.items():
        split_dir = root / "data" / "raw" / split
        files = sorted(split_dir.glob(pattern)) if split_dir.exists() else []
        if not files:
            errors.append(f"No files found for split '{split}' with pattern '{pattern}'")

    checksums: dict[str, str] = {}
    for split in ("train", "test", "truth"):
        for path in sorted((root / "data" / "raw" / split).glob("*.txt")):
            checksum = file_checksum(path)
            if checksum in checksums:
                errors.append(
                    f"Duplicate checksum detected: {path.name} duplicates {checksums[checksum]}"
                )
            else:
                checksums[checksum] = path.name

    return LayoutValidationResult(ok=not errors, errors=errors)


if __name__ == "__main__":
    project_root = Path(__file__).resolve().parents[2]
    result = validate_project_layout(project_root)
    if not result.ok:
        for message in result.errors:
            print(message)
        raise SystemExit(1)
    print("Layout validation successful.")
