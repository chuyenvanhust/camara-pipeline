import pytest
from fastapi.testclient import TestClient
from mock_services.gsma_tac.app import app

def test_app_routes_exist():
    routes = [route.path for route in app.routes]
    assert "/tac/{tac_code}" in routes
    assert "/tac/batch" in routes
    assert "/tac" in routes
    assert "/health" in routes

def test_app_lifespan_flow():
    # Sử dụng khối lệnh 'with' để kích hoạt trọn vẹn luồng Startup -> Shutdown của Lifespan
    with TestClient(app) as client:
        response = client.get("/health")
        assert response.status_code == 200
        assert response.json()["status"] == "ok"