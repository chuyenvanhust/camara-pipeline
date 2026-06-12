import pytest
from mock_services.itu_e164.seed import COUNTRY_DB, OPERATOR_DB, load_itu_data_to_memory

def test_seed_database_mapping(setup_itu_test_environment):
    # setup_itu_test_environment trả về một tuple chứa: (cc_path, op_path)
    cc_path, op_path = setup_itu_test_environment
    
    # Kích hoạt nạp dữ liệu test vào RAM
    load_itu_data_to_memory(cc_path=cc_path, op_path=op_path)
    
    # Xác thực logic đối sánh dữ liệu
    assert "84" in COUNTRY_DB
    assert COUNTRY_DB["84"] == "Vietnam"
    assert "91" in OPERATOR_DB["84"]