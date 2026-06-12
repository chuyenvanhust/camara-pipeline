import pytest
from simulator.config import SimulatorConfig, ErrorRates

# -----------------------------------------------------------------
# Kịch bản 1: Cấu hình mặc định hoặc cấu hình hợp lệ
# -----------------------------------------------------------------
def test_simulator_config_default_valid():
    """ Kiểm tra xem cấu hình mặc định có thỏa mãn các hàm validate hay không """
    config = SimulatorConfig()
    # Mặc định phải thỏa mãn KPI đề bài: 2 triệu bản ghi, throughput 5000/s
    assert config.total_records >= 2000000
    assert config.target_throughput >= 5000
    assert config.validate() is True

def test_simulator_config_custom_valid():
    """ Kiểm tra cấu hình tùy chỉnh nằm trong dải biên hợp lệ """
    custom_rates = ErrorRates(
        duplicate_rate=0.10,      # 10%
        late_arrival_rate=0.05,   # 5%
        invalid_imei_rate=0.05,   # 5%
        conflict_rate=0.02        # 2%
    )
    # Tổng tỷ lệ lỗi = 0.22 (22%) <= 1.0 (100%) -> Hợp lệ
    config = SimulatorConfig(
        total_records=5000000, 
        target_throughput=10000, 
        error_rates=custom_rates
    )
    assert config.validate() is True

# -----------------------------------------------------------------
# Kịch bản 2: Cấu hình SAI dải giá trị vận hành (Hạ tầng/Hiệu năng)
# -----------------------------------------------------------------
@pytest.mark.parametrize("bad_records, bad_throughput", [
    (0, 5000),    # Số lượng record bằng 0
    (-100, 5000), # Số lượng record âm
    (2000000, 0), # Throughput bằng 0
    (2000000, -5) # Throughput âm
])
def test_simulator_config_invalid_bounds(bad_records, bad_throughput):
    """ Đảm bảo ném lỗi ValueError nếu cấu hình số lượng bản ghi hoặc hiệu năng vô lý """
    config = SimulatorConfig(total_records=bad_records, target_throughput=bad_throughput)
    with pytest.raises(ValueError):
        config.validate()

# -----------------------------------------------------------------
# Kịch bản 3: Cấu hình SAI tỷ lệ phân bổ lỗi (Error Rates Bounds)
# -----------------------------------------------------------------
@pytest.mark.parametrize("out_of_bound_rate", [
    ErrorRates(duplicate_rate=-0.01), # Tỷ lệ lỗi bị âm
    ErrorRates(conflict_rate=1.05),   # Tỷ lệ lỗi > 100% (1.0)
])
def test_simulator_config_error_rate_out_of_range(out_of_bound_rate):
    """ Đảm bảo từng tỷ lệ lỗi đơn lẻ phải nằm nghiêm ngặt trong khoảng [0.0, 1.0] """
    config = SimulatorConfig(error_rates=out_of_bound_rate)
    with pytest.raises(ValueError):
        config.validate()

def test_simulator_config_total_error_rate_exceeded():
    """
    Đảm bảo tổng các tỷ lệ lỗi cấu hình không được vượt quá 1.0 (100%).
    Vì nếu vượt quá 100%, bộ tạo log (Simulator) không thể phân bổ xác suất chính xác.
    """
    overflow_rates = ErrorRates(
        duplicate_rate=0.50,
        late_arrival_rate=0.40,
        invalid_imei_rate=0.20 # Tổng lúc này = 1.10 (110%) -> Vượt ranh giới logic
    )
    config = SimulatorConfig(error_rates=overflow_rates)
    with pytest.raises(ValueError):
        config.validate()