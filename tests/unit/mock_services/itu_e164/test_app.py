import pytest
from fastapi.testclient import TestClient
from mock_services.itu_e164.app import app

def test_app_fault_injection_middleware():
    with TestClient(app) as client:
        # Test Header Status = 503
        response = client.post("/validate", 
                               json={"phone_number": "+84912345678"}, 
                               headers={"X-Inject-Fault": "status=503"})
        assert response.status_code == 503
        assert response.json()["error"] == "SERVICE_UNAVAILABLE"