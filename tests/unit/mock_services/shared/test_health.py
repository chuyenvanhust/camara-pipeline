import pytest
from mock_services.shared.health import create_health_response

def test_health_response_structure():
    service_name = "test_service"
    count = 100
    response = create_health_response(service_name, count)
    
    assert response.service_name == service_name
    assert response.record_count == count
    assert response.uptime_seconds >= 0
    # Vì mới khởi chạy nên status phải là DEGRADED (theo logic < 60s)
    assert response.status == "DEGRADED"

def test_health_types():
    response = create_health_response("test", 0)
    assert isinstance(response.uptime_seconds, float)
    assert isinstance(response.status, str)