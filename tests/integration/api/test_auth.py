import pytest
from httpx import AsyncClient, ASGITransport
from api.main import app

pytestmark = [pytest.mark.error_handling]

async def test_tc35_api_key_invalid():
    """TC35: Gọi KHÔNG qua api_client fixture (đã override auth).
    Tạo client riêng, KHÔNG override verify_api_key, gửi key sai."""
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
        timeout=5.0,
    ) as client:
        response = await client.post(
            "/sim-swap/v0/check",
            json={"phoneNumber": "+84901234561", "maxAge": 30},
            headers={"X-API-Key": "wrong_key_here"},
        )
    assert response.status_code == 401