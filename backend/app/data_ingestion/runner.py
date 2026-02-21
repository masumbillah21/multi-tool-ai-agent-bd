import argparse
import sqlite3

from app.core.config import DATA_DIR
from app.data_ingestion.dataset_config import DATASET_CONFIG
from app.data_ingestion.sqlite_writer import create_table, insert_dataframe
from app.data_ingestion.transform import load_hf_to_dataframe, transform_dataframe


def ingest_dataset(hf_id: str, db_path, table_name: str) -> None:
    print(f"Ingesting {hf_id} -> {db_path}")
    df = load_hf_to_dataframe(hf_id)
    df = transform_dataframe(df, table_name=table_name)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        create_table(conn, table_name, df)
        insert_dataframe(conn, table_name, df)
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
