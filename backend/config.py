import os
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"

INSTITUTIONS_DB_PATH = DATA_DIR / "institutions.db"
HOSPITALS_DB_PATH = DATA_DIR / "hospitals.db"
RESTAURANTS_DB_PATH = DATA_DIR / "restaurants.db"

GROQ_MODEL = os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile")
MAX_SQL_ROWS = int(os.getenv("MAX_SQL_ROWS", "50"))
