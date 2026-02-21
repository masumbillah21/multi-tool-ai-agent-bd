from app.core.config import HOSPITALS_DB_PATH, INSTITUTIONS_DB_PATH, RESTAURANTS_DB_PATH


DATASET_CONFIG = [
    {
        "hf_id": "Mahadih534/Institutional-Information-of-Bangladesh",
        "db_path": INSTITUTIONS_DB_PATH,
        "table_name": "institutions",
    },
    {
        "hf_id": "Mahadih534/all-bangladeshi-hospitals",
        "db_path": HOSPITALS_DB_PATH,
        "table_name": "hospitals",
    },
    {
        "hf_id": "Mahadih534/Bangladeshi-Restaurant-Data",
        "db_path": RESTAURANTS_DB_PATH,
        "table_name": "restaurants",
    },
]
