from pathlib import Path
from typing import Any

from app.core.config import HOSPITALS_DB_PATH, INSTITUTIONS_DB_PATH, RESTAURANTS_DB_PATH
from app.tools.sqlite_question_tool import SQLiteQuestionTool


class InstitutionsDBTool(SQLiteQuestionTool):
    name: str = "institutions_db_tool"
    description: str = (
        "Use for queries about Bangladeshi universities, colleges, schools, and government institutions."
    )
    db_path: Path = INSTITUTIONS_DB_PATH
    table_name: str = "institutions"
    llm: Any


class HospitalsDBTool(SQLiteQuestionTool):
    name: str = "hospitals_db_tool"
    description: str = (
        "Use for queries about Bangladeshi hospitals, clinics, beds, doctors, and facilities."
    )
    db_path: Path = HOSPITALS_DB_PATH
    table_name: str = "hospitals"
    llm: Any


class RestaurantsDBTool(SQLiteQuestionTool):
    name: str = "restaurants_db_tool"
    description: str = (
        "Use for queries about Bangladeshi restaurants, cuisine, ratings, and locations."
    )
    db_path: Path = RESTAURANTS_DB_PATH
    table_name: str = "restaurants"
    llm: Any


def build_db_tools(llm: Any) -> list[SQLiteQuestionTool]:
    return [
        InstitutionsDBTool(llm=llm),
        HospitalsDBTool(llm=llm),
        RestaurantsDBTool(llm=llm),
    ]
