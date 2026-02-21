import sqlite3

import pandas as pd


def to_sqlite_type(series: pd.Series) -> str:
    if pd.api.types.is_integer_dtype(series):
        return "INTEGER"
    if pd.api.types.is_float_dtype(series):
        return "REAL"
    return "TEXT"


def create_table(conn: sqlite3.Connection, table_name: str, df: pd.DataFrame) -> None:
    cols_with_types = [f'"{col}" {to_sqlite_type(df[col])}' for col in df.columns]
    conn.execute(f'DROP TABLE IF EXISTS "{table_name}"')
    conn.execute(f'CREATE TABLE "{table_name}" ({", ".join(cols_with_types)})')


def insert_dataframe(conn: sqlite3.Connection, table_name: str, df: pd.DataFrame) -> None:
    placeholders = ", ".join(["?"] * len(df.columns))
    col_names = ", ".join([f'"{c}"' for c in df.columns])
    query = f'INSERT INTO "{table_name}" ({col_names}) VALUES ({placeholders})'
    conn.executemany(query, df.where(pd.notna(df), None).itertuples(index=False, name=None))
    conn.commit()
