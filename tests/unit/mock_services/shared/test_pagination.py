import pytest
from mock_services.shared.pagination import paginate_records

def test_paginate_basic():
    # Tạo dữ liệu giả 1-100
    data = list(range(1, 101))
    
    # Test trang 1, mỗi trang 10 item
    result = paginate_records(data, page=1, limit=10)
    assert len(result.items) == 10
    assert result.items[0] == 1
    assert result.total == 100
    assert result.pages == 10

def test_paginate_last_page():
    data = list(range(1, 25)) # 24 items
    # Trang 3, mỗi trang 10 item -> trang cuối chỉ có 4 item
    result = paginate_records(data, page=3, limit=10)
    assert len(result.items) == 4
    assert result.pages == 3

def test_paginate_invalid_input():
    data = [1, 2, 3]
    with pytest.raises(ValueError):
        paginate_records(data, page=0, limit=10)