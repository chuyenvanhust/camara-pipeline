import pytest
from simulator.config import SimulatorConfig

@pytest.fixture
def base_config():
    """Trả về cấu hình simulator tiêu chuẩn cho môi trường kiểm thử"""
    return SimulatorConfig(
        records=50,
        subscribers=10,
        days=5,
        seed=42,
        duplicate_rate=0.0,
        late_arrival_rate=0.0,
        invalid_imei_rate=0.0,
        conflict_rate=0.0,
        missing_field_rate=0.0,
        output="tests/unit/simulator/test_data/radius_test_output.csv",
        kafka=False,
        kafka_topic="radius.test"
    )