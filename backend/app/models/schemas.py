from pydantic import BaseModel, Field


class QueryRequest(BaseModel):
    query: str = Field(..., description="User query for the Bangladesh multi-tool agent.")


class QueryResponse(BaseModel):
    answer: str
    citations: list[str] = []


class IngestRequest(BaseModel):
    only: str | None = Field(
        default=None,
        description=(
            "Optional target to ingest. Allowed: institutions, hospitals, restaurants. "
            "Also accepts singular forms and 'all'."
        ),
    )


class IngestResponse(BaseModel):
    status: str
    message: str
    only: str | None = None
