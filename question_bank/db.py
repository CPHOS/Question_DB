from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Iterable

from .schema import MIGRATION_COLUMNS, SCHEMA_STATEMENTS


def connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def _existing_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {row[1] for row in rows}


def initialize_database(db_path: Path) -> None:
    with connect(db_path) as conn:
        for statement in SCHEMA_STATEMENTS:
            conn.execute(statement)
        for table_name, columns in MIGRATION_COLUMNS.items():
            existing = _existing_columns(conn, table_name)
            for column_name, definition in columns.items():
                if column_name not in existing:
                    conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {definition}")
        conn.commit()


def fetch_all(conn: sqlite3.Connection, query: str, params: Iterable[object] | None = None) -> list[sqlite3.Row]:
    cursor = conn.execute(query, tuple(params or ()))
    return cursor.fetchall()
