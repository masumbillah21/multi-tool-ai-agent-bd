import os
from pathlib import Path


BACKEND_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = BACKEND_DIR / "data"

INSTITUTIONS_DB_PATH = DATA_DIR / "institutions.db"
HOSPITALS_DB_PATH = DATA_DIR / "hospitals.db"
RESTAURANTS_DB_PATH = DATA_DIR / "restaurants.db"

GROQ_MODEL = os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile")
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
TAVILY_API_KEY = os.getenv("TAVILY_API_KEY", "")
MAX_SQL_ROWS = int(os.getenv("MAX_SQL_ROWS", "50"))
INGEST_API_TOKEN = os.getenv("INGEST_API_TOKEN", "")
