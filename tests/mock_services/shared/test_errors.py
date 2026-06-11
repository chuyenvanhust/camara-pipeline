import pytest
import time
from fastapi import FastAPI
from httpx import AsyncClient, ASGITransport # Thêm ASGITransport
from mock_services.shared.errors import FaultInjectionMiddleware

app = FastAPI()
app.add_middleware(FaultInjectionMiddleware)

@app.get("/test-endpoint")
async def dummy_endpoint():
    return {"message": "success"}

@pytest.mark.asyncio
async def test_fault_delay():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        start = time.perf_counter()
        
        # Test delay 500ms
        await ac.get("/test-endpoint", headers={"X-Inject-Fault": "delay=500"})
        
        duration = (time.perf_counter() - start) * 1000
        
        # Sử dụng sai số +- 1ms (chấp nhận từ 499ms đến 501ms hoặc hơn)
        # Cách này đảm bảo test không bị fail do sai số đồng hồ hệ thống
        assert duration >= 499, f"Delay thực tế {duration}ms là quá thấp so với kỳ vọng 500ms"

@pytest.mark.asyncio
async def test_fault_status_code():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.get("/test-endpoint", headers={"X-Inject-Fault": "status=503"})
        assert response.status_code == 503
        assert response.json()["error"] == "INJECTED_ERROR_503"

@pytest.mark.asyncio
async def test_fault_error_rate():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.get("/test-endpoint", headers={"X-Inject-Fault": "error_rate=1.0"})
        assert response.status_code == 500

@pytest.mark.asyncio
async def test_no_fault():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.get("/test-endpoint")
        assert response.status_code == 200