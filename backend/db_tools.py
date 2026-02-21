import sqlite3
from pathlib import Path
from typing import Any

from langchain_core.tools import BaseTool
from pydantic import BaseModel, Field

from config import MAX_SQL_ROWS


class SQLQueryInput(BaseModel):
    question: str = Field(..., description="Natural language question to answer from this database.")


class SQLiteQuestionTool(BaseTool):
    args_schema = SQLQueryInput
    db_path: Path
    table_name: str
    llm: Any
    row_limit: int = MAX_SQL_ROWS

    def _run(self, question: str) -> str:
        if not self.db_path.exists():
            return (
                f"Database not found at {self.db_path}. "
                "Run `python ingest_datasets.py` in backend/ first."
            )

        schema = self._get_schema()
        sql_query = self._generate_sql(question=question, schema=schema)
        if not self._is_safe_query(sql_query):
            return (
                "I could not generate a safe SELECT query for that request. "
                "Please rephrase your question with clear filtering details."
            )

        rows, column_names = self._execute_query(sql_query)
        return self._summarize_results(
            question=question,
            sql_query=sql_query,
            column_names=column_names,
            rows=rows,
        )

    def _get_schema(self) -> str:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(f"PRAGMA table_info({self.table_name});")
            fields = cursor.fetchall()
        if not fields:
            return f"Table `{self.table_name}` has no columns."
        lines = [f"- {name} ({field_type})" for _, name, field_type, *_ in fields]
        return f"Table `{self.table_name}` columns:\n" + "\n".join(lines)

    def _generate_sql(self, question: str, schema: str) -> str:
        prompt = f"""
You write SQLite SELECT queries.
Return only a single SQL query, no markdown, no explanation.
Use only the table `{self.table_name}`.
Always include LIMIT {self.row_limit} unless the user asks for a count/aggregation.
Never use INSERT, UPDATE, DELETE, DROP, ALTER, CREATE, ATTACH, DETACH, PRAGMA.

Schema:
{schema}

Question:
{question}
"""
        response = self.llm.invoke(prompt)
        return (response.content or "").strip().strip("`")

    def _execute_query(self, query: str) -> tuple[list[tuple], list[str]]:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(query)
            rows = cursor.fetchall()
            column_names = [desc[0] for desc in cursor.description or []]
        return rows, column_names

    def _summarize_results(
        self,
        question: str,
        sql_query: str,
        column_names: list[str],
        rows: list[tuple],
    ) -> str:
        preview_rows = rows[: self.row_limit]
        prompt = f"""
Answer the user question using SQL results.
Be concise and factual. If no rows, say so clearly.
If rows are many, summarize and show key records.

Question: {question}
SQL used: {sql_query}
Columns: {column_names}
Rows: {preview_rows}
Total rows returned: {len(rows)}
"""
        response = self.llm.invoke(prompt)
        answer = (response.content or "").strip()
        return f"{answer}\n\n(SQL: {sql_query})"

    @staticmethod
    def _is_safe_query(query: str) -> bool:
        normalized = " ".join(query.lower().split())
        if not normalized.startswith("select"):
            return False
        blocked = [
            "insert ",
            "update ",
            "delete ",
            "drop ",
            "alter ",
            "create ",
            "attach ",
            "detach ",
            "pragma ",
        ]
        return not any(token in normalized for token in blocked)
