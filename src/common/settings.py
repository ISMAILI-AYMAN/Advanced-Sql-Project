from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Final


@dataclass(frozen=True)
class AppSettings:
    pg_host: str
    pg_port: int
    pg_database: str
    pg_user: str
    pg_password: str
    pipeline_version: str


DEFAULT_PIPELINE_VERSION: Final[str] = "dev-local"


def load_settings() -> AppSettings:
    return AppSettings(
        pg_host=os.getenv("PGHOST", "localhost"),
        pg_port=int(os.getenv("PGPORT", "5432")),
        pg_database=os.getenv("PGDATABASE", "cmapss"),
        pg_user=os.getenv("PGUSER", "cmapss"),
        pg_password=os.getenv("PGPASSWORD", "cmapss"),
        pipeline_version=os.getenv("PIPELINE_VERSION", DEFAULT_PIPELINE_VERSION),
    )
