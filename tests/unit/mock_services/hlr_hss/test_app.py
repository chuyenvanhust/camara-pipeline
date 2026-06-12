import pytest
from fastapi.testclient import TestClient
from mock_services.hlr_hss.app import app

def test_app_health_endpoint():
    with TestClient(app) as client:
        res = client.get("/health")
        assert res.status_code == 200
        assert res.json()["service"] == "hlr-hss-mock"