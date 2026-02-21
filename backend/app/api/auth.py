from fastapi import Header, HTTPException, status

from app.core.config import INGEST_API_TOKEN


def require_ingest_token(x_api_key: str = Header(..., alias="x-api-key")) -> None:
    if not INGEST_API_TOKEN:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="INGEST_API_TOKEN is not configured on the server.",
        )

    if x_api_key != INGEST_API_TOKEN:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing ingest token.",
        )
