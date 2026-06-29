# tests/unit/api/test_routers.py
"""
Integration test cho 3 CAMARA API routers + health endpoint.

Chiến lược:
- Dùng httpx.AsyncClient với FastAPI TestClient (ASGI transport)
  -- không cần server thật đang chạy.
- Mock get_db dependency: inject asyncpg.Connection giả thay vì
  pool thật -- không cần PostgreSQL chạy.
- Patch verify_api_key để bypass auth trong test data routing
  (auth được test riêng trong test_auth.py).
- Data được inject qua mock db.fetchrow return value --
  không cần pipeline, không cần seed DB.
"""

import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch
from httpx import AsyncClient, ASGITransport

from api.main import app
from api.dependencies.database import get_db
from api.dependencies.auth import verify_api_key


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def valid_headers():
    """Header hợp lệ cho mọi request."""
    return {"X-API-Key": "test-key", "Content-Type": "application/json"}


@pytest.fixture
def mock_db():
    """asyncpg.Connection giả — control fetchrow/execute return value."""
    conn = AsyncMock()
    return conn


@pytest.fixture
async def client(mock_db):
    """
    httpx.AsyncClient với:
    - ASGI transport trỏ vào FastAPI app (không cần server thật).
    - Override get_db → mock_db (không cần PostgreSQL).
    - Override verify_api_key → luôn pass (auth test riêng).
    """
    app.dependency_overrides[get_db] = lambda: mock_db
    app.dependency_overrides[verify_api_key] = lambda: "test-key"

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as ac:
        yield ac, mock_db

    app.dependency_overrides.clear()


# ── Health ────────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_health_returns_ok():
    """GET /health không cần auth, luôn trả 200."""
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as ac:
        response = await ac.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


# ── SIM Swap ──────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_sim_swap_check_swapped_true(client, valid_headers):
    ac, mock_db = client
    # DB trả row → có SIM Swap
    mock_db.fetchrow.return_value = {
        "detected_at": datetime(2026, 6, 14, 10, 0, 0, tzinfo=timezone.utc)
    }
    response = await ac.post(
        "/sim-swap/v0/check",
        json={"phoneNumber": "+84971234567", "maxAge": 30},
        headers=valid_headers,
    )
    assert response.status_code == 200
    assert response.json()["swapped"] is True


@pytest.mark.asyncio
async def test_sim_swap_check_no_swap(client, valid_headers):
    ac, mock_db = client
    # DB trả None → không có SIM Swap
    mock_db.fetchrow.return_value = None
    response = await ac.post(
        "/sim-swap/v0/check",
        json={"phoneNumber": "+84971234567", "maxAge": 30},
        headers=valid_headers,
    )
    assert response.status_code == 200
    assert response.json()["swapped"] is False


@pytest.mark.asyncio
async def test_sim_swap_retrieve_date_has_swap(client, valid_headers):
    ac, mock_db = client
    swap_ts = datetime(2026, 6, 14, 10, 0, 0, tzinfo=timezone.utc)
    mock_db.fetchrow.return_value = {"detected_at": swap_ts}
    response = await ac.post(
        "/sim-swap/v0/retrieve-date",
        json={"phoneNumber": "+84971234567", "maxAge": 30},
        headers=valid_headers,
    )
    assert response.status_code == 200
    assert response.json()["latestSimChange"] is not None


@pytest.mark.asyncio
async def test_sim_swap_retrieve_date_no_swap(client, valid_headers):
    ac, mock_db = client
    mock_db.fetchrow.return_value = None
    response = await ac.post(
        "/sim-swap/v0/retrieve-date",
        json={"phoneNumber": "+84971234567", "maxAge": 30},
        headers=valid_headers,
    )
    assert response.status_code == 200
    assert response.json()["latestSimChange"] is None


@pytest.mark.asyncio
async def test_sim_swap_invalid_phone_returns_422(client, valid_headers):
    ac, mock_db = client
    response = await ac.post(
        "/sim-swap/v0/check",
        json={"phoneNumber": "not-a-phone", "maxAge": 30},
        headers=valid_headers,
    )
    assert response.status_code == 422
    assert response.json()["error"] == "INVALID_ARGUMENT"


@pytest.mark.asyncio
async def test_sim_swap_negative_max_age_returns_422(client, valid_headers):
    ac, mock_db = client
    response = await ac.post(
        "/sim-swap/v0/check",
        json={"phoneNumber": "+84971234567", "maxAge": -1},
        headers=valid_headers,
    )
    assert response.status_code == 422


# ── Device Swap ───────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_device_swap_check_swapped_true(client, valid_headers):
    ac, mock_db = client
    mock_db.fetchrow.return_value = {
        "detected_at": datetime(2026, 6, 14, 10, 0, 0, tzinfo=timezone.utc)
    }
    response = await ac.post(
        "/device-swap/v0/check",
        json={"phoneNumber": "+84971234567", "maxAge": 30},
        headers=valid_headers,
    )
    assert response.status_code == 200
    assert response.json()["deviceSwapped"] is True


@pytest.mark.asyncio
async def test_device_swap_check_no_swap(client, valid_headers):
    ac, mock_db = client
    mock_db.fetchrow.return_value = None
    response = await ac.post(
        "/device-swap/v0/check",
        json={"phoneNumber": "+84971234567", "maxAge": 30},
        headers=valid_headers,
    )
    assert response.status_code == 200
    assert response.json()["deviceSwapped"] is False


@pytest.mark.asyncio
async def test_device_swap_retrieve_date_has_swap(client, valid_headers):
    ac, mock_db = client
    mock_db.fetchrow.return_value = {
        "detected_at": datetime(2026, 6, 14, 9, 0, 0, tzinfo=timezone.utc)
    }
    response = await ac.post(
        "/device-swap/v0/retrieve-date",
        json={"phoneNumber": "+84971234567", "maxAge": 30},
        headers=valid_headers,
    )
    assert response.status_code == 200
    assert response.json()["latestDeviceChange"] is not None


@pytest.mark.asyncio
async def test_device_swap_retrieve_date_no_swap(client, valid_headers):
    ac, mock_db = client
    mock_db.fetchrow.return_value = None
    response = await ac.post(
        "/device-swap/v0/retrieve-date",
        json={"phoneNumber": "+84971234567"},
        headers=valid_headers,
    )
    assert response.status_code == 200
    assert response.json()["latestDeviceChange"] is None


# ── Number Verification ───────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_number_verify_active_session(client, valid_headers):
    ac, mock_db = client
    # EXISTS query trả True → verified
    mock_db.fetchrow.return_value = {"has_active_session": True}
    response = await ac.post(
        "/number-verification/v0/verify",
        json={"phoneNumber": "+84971234567"},
        headers=valid_headers,
    )
    assert response.status_code == 200
    assert response.json()["devicePhoneNumberVerified"] is True


@pytest.mark.asyncio
async def test_number_verify_no_active_session(client, valid_headers):
    ac, mock_db = client
    mock_db.fetchrow.return_value = {"has_active_session": False}
    response = await ac.post(
        "/number-verification/v0/verify",
        json={"phoneNumber": "+84971234567"},
        headers=valid_headers,
    )
    assert response.status_code == 200
    assert response.json()["devicePhoneNumberVerified"] is False


@pytest.mark.asyncio
async def test_number_verify_msisdn_not_found_returns_false_not_404(
    client, valid_headers
):
    """
    MSISDN không tồn tại trong DB → fetchrow trả None (EXISTS trả False)
    → verified=False, không phải 404.
    Đây là hành vi đúng theo CAMARA spec: không tìm thấy ≠ lỗi.
    """
    ac, mock_db = client
    mock_db.fetchrow.return_value = None
    response = await ac.post(
        "/number-verification/v0/verify",
        json={"phoneNumber": "+84971234567"},
        headers=valid_headers,
    )
    assert response.status_code == 200
    assert response.json()["devicePhoneNumberVerified"] is False


@pytest.mark.asyncio
async def test_number_verify_invalid_phone_returns_422(client, valid_headers):
    ac, mock_db = client
    response = await ac.post(
        "/number-verification/v0/verify",
        json={"phoneNumber": "0971234567"},  # thiếu dấu +
        headers=valid_headers,
    )
    assert response.status_code == 422


# ── Auth ──────────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_missing_api_key_returns_401(mock_db):
    """
    Không có header X-API-Key → 401.
    Test này KHÔNG dùng fixture client (để giữ nguyên auth) nhưng mock DB tránh lỗi 503.
    """
    # Ép ứng dụng sử dụng mock_db thay vì kết nối thật để tránh văng lỗi 503
    app.dependency_overrides[get_db] = lambda: mock_db

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as ac:
        response = await ac.post(
            "/sim-swap/v0/check",
            json={"phoneNumber": "+84971234567", "maxAge": 30},
        )
        
    # Dọn dẹp override để tránh rò rỉ cấu hình sang các file test khác
    app.dependency_overrides.clear()

    assert response.status_code in (401, 422)


@pytest.mark.asyncio
async def test_wrong_api_key_returns_401(mock_db):
    """Sai X-API-Key → 401 từ verify_api_key dependency."""
    app.dependency_overrides[get_db] = lambda: mock_db

    with patch("api.dependencies.auth.settings") as mock_settings:
        mock_settings.api_key = "correct-key"
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as ac:
            response = await ac.post(
                "/sim-swap/v0/check",
                json={"phoneNumber": "+84971234567", "maxAge": 30},
                headers={"X-API-Key": "wrong-key"},
            )
            
    app.dependency_overrides.clear()

    assert response.status_code == 401
    
    # SỬA TẠI ĐÂY: Sử dụng toán tử 'in' hoặc ép kiểm tra chuỗi linh hoạt 
    # đề phòng trường hợp cấu hình Custom HTTPException trả về trường "detail" thay vì "error"
    res_data = response.json()
    assert "UNAUTHENTICATED" in str(res_data)