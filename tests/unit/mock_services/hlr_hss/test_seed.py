import pytest
import os
from mock_services.hlr_hss.seed import load_subscribers_to_memory, SUBSCRIBERS_BY_IMSI

def test_seed_load_to_memory_correctness(setup_hlr_test_environment):
    load_subscribers_to_memory(file_path=setup_hlr_test_environment)
    assert len(SUBSCRIBERS_BY_IMSI) > 0
    # Đọc bản ghi đầu tiên của chuỗi sinh mẫu
    assert "452010000000000" in SUBSCRIBERS_BY_IMSI