from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

from src.common.settings import load_settings


@contextmanager
def get_connection() -> Iterator[object]:
    import psycopg2

    settings = load_settings()
    conn = psycopg2.connect(
        host=settings.pg_host,
        port=settings.pg_port,
        dbname=settings.pg_database,
        user=settings.pg_user,
        password=settings.pg_password,
    )
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
