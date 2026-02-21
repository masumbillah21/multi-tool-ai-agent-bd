import sqlite3
from pathlib import Path
from typing import Any

from langchain_core.tools import BaseTool
from pydantic import BaseModel, Field

from app.core.config import MAX_SQL_ROWS


class SQLQueryInput(BaseModel):
    question: str = Field(..., description="Natural language question to answer from this database.")


class SQLiteQuestionTool(BaseTool):
    args_schema: type[BaseModel] = SQLQueryInput
    db_path: Path
    table_name: str
    llm: Any
    row_limit: int = MAX_SQL_ROWS

    def _run(self, question: str) -> str:
        try:
            if not self.db_path.exists():
                return (
                    f"Database not found at {self.db_path}. "
                    "Run `python ingest_datasets.py` in backend/ first."
                )

            fields = self._get_fields()
            schema = self._format_schema(fields)
            sql_query = self._generate_sql(question=question, schema=schema, fields=fields)
            if not self._is_safe_query(sql_query):
                return (
                    "I could not generate a safe SELECT query for that request. "
                    "Please rephrase your question with clear filtering details."
                )

            sql_query, rows, column_names = self._execute_query_with_repair(
                question=question,
                schema=schema,
                fields=fields,
                sql_query=sql_query,
            )
            if not rows:
                fallback_sql = self._generate_fallback_sql(
                    question=question,
                    schema=schema,
                    fields=fields,
                    previous_sql=sql_query,
                )
                if fallback_sql and fallback_sql != sql_query and self._is_safe_query(fallback_sql):
                    fallback_sql, fallback_rows, fallback_columns = self._execute_query_with_repair(
                        question=question,
                        schema=schema,
                        fields=fields,
                        sql_query=fallback_sql,
                    )
                    if fallback_rows:
                        sql_query = fallback_sql
                        rows = fallback_rows
                        column_names = fallback_columns

            return self._summarize_results(
                question=question,
                sql_query=sql_query,
                column_names=column_names,
                rows=rows,
            )
        except sqlite3.Error:
            return (
                "I could not execute a valid database query for this request. "
                "Please rephrase with clearer filters."
            )
        except Exception as exc:
            return f"Database tool failed: {exc}"

    def _get_fields(self) -> list[tuple]:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(f"PRAGMA table_info({self.table_name});")
            return cursor.fetchall()

    def _format_schema(self, fields: list[tuple]) -> str:
        if not fields:
            return f"Table `{self.table_name}` has no columns."
        lines = [f"- {name} ({field_type})" for _, name, field_type, *_ in fields]
        return f"Table `{self.table_name}` columns:\n" + "\n".join(lines)

    def _generate_sql(self, question: str, schema: str, fields: list[tuple]) -> str:
        location_columns = self._location_columns(fields)
        prompt = f"""
You write SQLite SELECT queries.
Return only a single SQL query, no markdown, no explanation.
Use only the table `{self.table_name}`.
Always include LIMIT {self.row_limit} unless the user asks for a count/aggregation.
Never use INSERT, UPDATE, DELETE, DROP, ALTER, CREATE, ATTACH, DETACH, PRAGMA.
Do not use UNION.
For city/district/location filters, prefer case-insensitive partial match:
LOWER(column) LIKE LOWER('%value%') instead of exact equality.
If multiple location columns could match, use OR across these columns when relevant: {location_columns}.

Schema:
{schema}

Question:
{question}
"""
        response = self.llm.invoke(prompt)
        return self._normalize_sql(response.content or "")

    def _generate_fallback_sql(
        self,
        question: str,
        schema: str,
        fields: list[tuple],
        previous_sql: str,
    ) -> str:
        location_columns = self._location_columns(fields)
        prompt = f"""
The previous SQL query returned 0 rows.
Rewrite the SQL to keep the same intent but broaden matching.
Use case-insensitive partial matching for place names with LOWER(col) LIKE LOWER('%value%').
If useful, combine relevant location columns using OR.

Return only one SQLite SELECT query, no explanation.
Use only `{self.table_name}` and keep LIMIT {self.row_limit} for non-aggregate queries.
Never use INSERT, UPDATE, DELETE, DROP, ALTER, CREATE, ATTACH, DETACH, PRAGMA.
Do not use UNION.

Schema:
{schema}

Likely location columns:
{location_columns}

Question:
{question}

Previous query:
{previous_sql}
"""
        response = self.llm.invoke(prompt)
        return self._normalize_sql(response.content or "")

    def _generate_repair_sql(
        self,
        question: str,
        schema: str,
        previous_sql: str,
        error_message: str,
    ) -> str:
        prompt = f"""
The previous SQLite query failed with this error:
{error_message}

Rewrite it to a valid SQLite SELECT query with the same intent.
Use only table `{self.table_name}`.
Never use INSERT, UPDATE, DELETE, DROP, ALTER, CREATE, ATTACH, DETACH, PRAGMA, UNION.
Keep LIMIT {self.row_limit} for non-aggregate queries.
Return only SQL, no explanation.

Schema:
{schema}

Question:
{question}

Previous query:
{previous_sql}
"""
        response = self.llm.invoke(prompt)
        return self._normalize_sql(response.content or "")

    @staticmethod
    def _normalize_sql(raw_text: str) -> str:
        raw_sql = raw_text.strip()
        if raw_sql.startswith("```"):
            raw_sql = raw_sql.replace("```sql", "").replace("```", "").strip()
        if raw_sql.lower().startswith("sql\n"):
            raw_sql = raw_sql[4:].strip()
        if raw_sql.lower().startswith("sql "):
            raw_sql = raw_sql[4:].strip()
        select_idx = raw_sql.lower().find("select")
        if select_idx > 0:
            raw_sql = raw_sql[select_idx:]
        raw_sql = raw_sql.split(";", maxsplit=1)[0].strip()
        if raw_sql:
            return f"{raw_sql};"
        return raw_sql

    @staticmethod
    def _location_columns(fields: list[tuple]) -> str:
        keywords = {
            "location",
            "address",
            "district",
            "division",
            "city",
            "city_corporation",
            "upazila",
            "thana",
            "area",
            "region",
        }
        names = [name for _, name, *_ in fields]
        matched = []
        for name in names:
            lowered = name.lower()
            if any(token in lowered for token in keywords):
                matched.append(name)
        return ", ".join(matched) if matched else "none"

    def _execute_query(self, query: str) -> tuple[list[tuple], list[str]]:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(query)
            rows = cursor.fetchall()
            column_names = [desc[0] for desc in cursor.description or []]
        return rows, column_names

    def _execute_query_with_repair(
        self,
        question: str,
        schema: str,
        fields: list[tuple],
        sql_query: str,
    ) -> tuple[str, list[tuple], list[str]]:
        del fields
        current_sql = sql_query
        last_error: Exception | None = None

        for _ in range(2):
            try:
                rows, columns = self._execute_query(current_sql)
                return current_sql, rows, columns
            except sqlite3.Error as exc:
                last_error = exc
                repaired_sql = self._generate_repair_sql(
                    question=question,
                    schema=schema,
                    previous_sql=current_sql,
                    error_message=str(exc),
                )
                if not repaired_sql or repaired_sql == current_sql or not self._is_safe_query(repaired_sql):
                    break
                current_sql = repaired_sql

        if last_error:
            raise last_error
        raise sqlite3.OperationalError("Could not execute generated SQL.")

    def _summarize_results(
        self,
        question: str,
        sql_query: str,
        column_names: list[str],
        rows: list[tuple],
    ) -> str:
        preview_rows = rows[: self.row_limit]
        if not rows:
            prompt = f"""
You are a friendly assistant.
The user asked: "{question}"
No matching data was found.

Write a short, natural response that sounds human.
Do not mention SQL, query execution, row counts, or database internals.
Optionally suggest one simple way to rephrase the request.
"""
        else:
            prompt = f"""
Answer the user's question using the provided results.
Use a natural, conversational tone while staying concise and factual.
Do not mention SQL, queries, row counts, table names, or database internals.

Question: {question}
Columns: {column_names}
Rows: {preview_rows}
"""
        response = self.llm.invoke(prompt)
        answer = (response.content or "").strip()
        if answer:
            return answer
        if rows:
            return "Here’s what I found."
        return "I couldn’t find a matching result for that request."

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
            " union ",
        ]
        return not any(token in normalized for token in blocked)
