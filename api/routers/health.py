"""
Health check endpoint — không cần auth, không cần DB.
Dùng để liveness probe trong Docker Compose / k8s.
"""

from fastapi import APIRouter

router = APIRouter(tags=["Health"])


@router.get("/health", summary="Liveness check")
async def health() -> dict:
    """
    Trả về status OK.

    Returns:
        {"status": "ok"} — HTTP 200.
        Nếu app không respond endpoint này, container orchestrator
        sẽ restart service.
    """
    return {"status": "ok"}