from fastapi import APIRouter, Body, Depends, HTTPException, status

from app.api.auth import require_ingest_token
from app.data_ingestion.runner import run_ingestion
from app.models.schemas import IngestRequest, IngestResponse, QueryRequest, QueryResponse
from app.services.agent_factory import build_agent_executor, run_agent_query


router = APIRouter()
agent_executor = build_agent_executor()


@router.post("/ask", response_model=QueryResponse)
def ask_question(request: QueryRequest) -> QueryResponse:
    result = run_agent_query(agent_executor, request.query)
    answer = result.get("output", "No answer generated.")
    return QueryResponse(answer=answer, citations=[])


@router.post("/admin/ingest", response_model=IngestResponse, dependencies=[Depends(require_ingest_token)])
def ingest_datasets(request: IngestRequest = Body(default_factory=IngestRequest)) -> IngestResponse:
    normalized_only = _normalize_ingest_target(request.only)
    if normalized_only is False:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                "`only` must be one of: institutions, hospitals, restaurants "
                "(also accepts institution/hospital/restaurant or all)."
            ),
        )

    try:
        run_ingestion(only=normalized_only)
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Ingestion failed: {exc}",
        ) from exc

    target = normalized_only if normalized_only else "all datasets"
    return IngestResponse(
        status="success",
        message=f"Ingestion completed for {target}.",
        only=normalized_only,
    )


def _normalize_ingest_target(value: str | None) -> str | None | bool:
    if value is None:
        return None

    normalized = value.strip().lower()
    if not normalized or normalized in {"all", "*", "any"}:
        return None

    aliases = {
        "institution": "institutions",
        "institutions": "institutions",
        "hospital": "hospitals",
        "hospitals": "hospitals",
        "restaurant": "restaurants",
        "restaurants": "restaurants",
    }
    return aliases.get(normalized, False)
