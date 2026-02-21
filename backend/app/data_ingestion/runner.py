import argparse
import hashlib
import sqlite3

import pandas as pd

from app.core.config import DATA_DIR
from app.data_ingestion.dataset_config import DATASET_CONFIG
from app.data_ingestion.sqlite_writer import create_table, upsert_dataframe
from app.data_ingestion.transform import load_hf_to_dataframe, transform_dataframe


UPSERT_KEY_CANDIDATES: dict[str, list[list[str]]] = {
    "institutions": [
        ["id"],
        ["code"],
        ["institution_id"],
        ["name", "location", "district"],
        ["name", "district", "upazila"],
        ["name", "location"],
    ],
    "hospitals": [
        ["id"],
        ["code"],
        ["hospital_id"],
        ["name", "location", "district"],
        ["name", "district", "upazila"],
        ["name", "location"],
    ],
    "restaurants": [
        ["id"],
        ["code"],
        ["restaurant_id"],
        ["name", "location", "district"],
        ["name", "district", "city"],
        ["name", "location"],
    ],
}


def ingest_dataset(hf_id: str, db_path, table_name: str) -> None:
    print(f"Ingesting {hf_id} -> {db_path}")
    df = load_hf_to_dataframe(hf_id)
    column_types = _get_column_types(table_name)
    df = transform_dataframe(df, table_name=table_name, column_types=column_types)
    df = _attach_record_key(df, table_name=table_name)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        create_table(conn, table_name, df, column_types=column_types)
        upsert_dataframe(conn, table_name, df)
    print(f"Completed {table_name}: {len(df)} rows, {len(df.columns)} columns")


def run_ingestion(only: str | None = None) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    for item in DATASET_CONFIG:
        if only and item["table_name"] != only:
            continue
        ingest_dataset(item["hf_id"], item["db_path"], item["table_name"])


def cli() -> None:
    parser = argparse.ArgumentParser(description="Create SQLite DBs from Bangladesh HuggingFace datasets.")
    parser.add_argument(
        "--only",
        choices=["institutions", "hospitals", "restaurants"],
        help="Ingest only one dataset by table name.",
    )
    args = parser.parse_args()
    run_ingestion(only=args.only)


def _attach_record_key(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    out = df.copy()
    selected_key_cols = _select_key_columns(out, table_name)

    full_row_key = _row_signature(out, columns=list(out.columns))
    if selected_key_cols:
        natural_key = _row_signature(out, columns=selected_key_cols)
    else:
        natural_key = full_row_key

    use_fallback = natural_key.str.replace("|", "", regex=False).str.len() == 0
    merged_key = natural_key.where(~use_fallback, full_row_key)

    out["record_key"] = merged_key.map(_sha256)

    duplicate_mask = out["record_key"].duplicated(keep=False)
    if duplicate_mask.any():
        # Use full-row signature for collisions so distinct records are preserved.
        out.loc[duplicate_mask, "record_key"] = full_row_key.loc[duplicate_mask].map(_sha256)

    out = out.drop_duplicates(subset=["record_key"], keep="last")
    return out


def _select_key_columns(df: pd.DataFrame, table_name: str) -> list[str]:
    candidates = UPSERT_KEY_CANDIDATES.get(table_name, []) + [["uid"], ["uuid"]]
    for cols in candidates:
        if not all(col in df.columns for col in cols):
            continue
        signature = _row_signature(df, columns=cols)
        non_empty_ratio = (signature.str.replace("|", "", regex=False).str.len() > 0).mean()
        if non_empty_ratio >= 0.8:
            return cols
    return []


def _row_signature(df: pd.DataFrame, columns: list[str]) -> pd.Series:
    frame = df[columns].copy()
    for col in frame.columns:
        frame[col] = frame[col].fillna("").astype(str).str.strip().str.lower()
    return frame.agg("|".join, axis=1)


def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _get_column_types(table_name: str) -> dict[str, str]:
    for item in DATASET_CONFIG:
        if item["table_name"] == table_name:
            return item.get("column_types", {})
    return {}
