import sqlite3

import pandas as pd


def to_sqlite_type(series: pd.Series) -> str:
    if pd.api.types.is_integer_dtype(series):
        return "INTEGER"
    if pd.api.types.is_float_dtype(series):
        return "REAL"
    return "TEXT"


def create_table(
    conn: sqlite3.Connection,
    table_name: str,
    df: pd.DataFrame,
    column_types: dict[str, str] | None = None,
) -> None:
    if "record_key" not in df.columns:
        raise ValueError("`record_key` column is required for upsert ingestion.")

    existing_types = _get_existing_column_types(conn, table_name)
    existing_columns = set(existing_types.keys())
    expected_types = {
        col: _resolve_column_type(col, df[col], column_types)
        for col in df.columns
    }

    if existing_columns and "record_key" not in existing_columns:
        # One-time migration path from old insert-only schema.
        conn.execute(f'DROP TABLE IF EXISTS "{table_name}"')
        existing_columns = set()
        existing_types = {}
    elif existing_columns and _has_schema_mismatch(existing_types, expected_types):
        # Existing schema has incompatible column types; rebuild to enforce canonical types.
        conn.execute(f'DROP TABLE IF EXISTS "{table_name}"')
        existing_columns = set()
        existing_types = {}

    if not existing_columns:
        cols_with_types = [f'"{col}" {expected_types[col]}' for col in df.columns]
        conn.execute(f'CREATE TABLE IF NOT EXISTS "{table_name}" ({", ".join(cols_with_types)})')
    else:
        for col in df.columns:
            if col in existing_columns:
                continue
            col_type = expected_types[col]
            conn.execute(f'ALTER TABLE "{table_name}" ADD COLUMN "{col}" {col_type}')

    conn.execute(
        f'CREATE UNIQUE INDEX IF NOT EXISTS "idx_{table_name}_record_key" '
        f'ON "{table_name}" ("record_key")'
    )
    conn.commit()


def upsert_dataframe(conn: sqlite3.Connection, table_name: str, df: pd.DataFrame) -> None:
    if "record_key" not in df.columns:
        raise ValueError("`record_key` column is required for upsert ingestion.")

    placeholders = ", ".join(["?"] * len(df.columns))
    col_names = ", ".join([f'"{c}"' for c in df.columns])
    update_cols = [col for col in df.columns if col != "record_key"]
    if update_cols:
        updates = ", ".join([f'"{col}"=excluded."{col}"' for col in update_cols])
        query = (
            f'INSERT INTO "{table_name}" ({col_names}) VALUES ({placeholders}) '
            f'ON CONFLICT("record_key") DO UPDATE SET {updates}'
        )
    else:
        query = (
            f'INSERT INTO "{table_name}" ({col_names}) VALUES ({placeholders}) '
            f'ON CONFLICT("record_key") DO NOTHING'
        )
    conn.executemany(query, df.where(pd.notna(df), None).itertuples(index=False, name=None))
    conn.commit()


def _get_existing_column_types(conn: sqlite3.Connection, table_name: str) -> dict[str, str]:
    cursor = conn.execute(f"PRAGMA table_info('{table_name}')")
    rows = cursor.fetchall()
    return {row[1]: _normalize_sqlite_type(row[2]) for row in rows}


def _resolve_column_type(col: str, series: pd.Series, column_types: dict[str, str] | None) -> str:
    if column_types and col in column_types:
        return _normalize_sqlite_type(column_types[col])
    if column_types:
        return "TEXT"
    return to_sqlite_type(series)


def _normalize_sqlite_type(type_name: str) -> str:
    normalized = (type_name or "").strip().upper()
    if normalized.startswith("INT"):
        return "INTEGER"
    if normalized.startswith("REAL") or normalized.startswith("FLOA") or normalized.startswith("DOUB"):
        return "REAL"
    return "TEXT"


def _has_schema_mismatch(existing_types: dict[str, str], expected_types: dict[str, str]) -> bool:
    for col, expected_type in expected_types.items():
        if col not in existing_types:
            continue
        if existing_types[col] != expected_type:
            return True
    return False
