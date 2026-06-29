#tests\integration\api\test_error_handling.py
import pytest
from unittest.mock import patch
from unittest.mock import AsyncMock
from api.dependencies.database import get_db
from api.main import app
import asyncpg

pytestmark = [pytest.mark.error_handling]

async def test_tc34_missing_required_fields(api_client):
    """TC34: Request body thiếu trường bắt buộc (như phoneNumber) -> FastAPI trả về 422 Unprocessable Entity"""
    response = await api_client.post("/sim-swap/v0/check", json={"maxAge": 30}, headers={"X-API-Key": "test_key"})
    assert response.status_code == 422

@pytest.mark.asyncio
async def test_tc36_database_timeout_returns_503(api_client):
    """TC36: DB lỗi → generic handler trả 503."""
    mock_conn = AsyncMock()
    mock_conn.fetchrow.side_effect = asyncpg.InterfaceError("Database connection timeout")

    from api.dependencies.database import get_db
    from api.main import app

    async def bad_db():
        yield mock_conn

    app.dependency_overrides[get_db] = bad_db
    try:
        response = await api_client.post(
            "/sim-swap/v0/check",
            json={"phoneNumber": "+84901234561", "maxAge": 30},
            headers={"X-API-Key": "test_key"}
        )
        assert response.status_code == 503
        body = response.json()
        assert body["error"] == "SERVICE_UNAVAILABLE"
        assert "Database" in body["message"]
    finally:
        app.dependency_overrides.pop(get_db, None)