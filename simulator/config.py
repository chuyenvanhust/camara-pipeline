from dataclasses import dataclass, field

@dataclass
class ErrorRates:
    """ Cấu hình tỷ lệ phần trăm (0.0 đến 1.0) của từng loại lỗi được inject """
    duplicate_rate: float = 0.05       # 5% dữ liệu trùng lặp
    late_arrival_rate: float = 0.03    # 3% dữ liệu đến muộn
    invalid_imei_rate: float = 0.02    # 2% lỗi IMEI
    conflict_rate: float = 0.01        # 1% xung đột (cùng thời điểm, khác SIM)

@dataclass
class SimulatorConfig:
    """ Cấu hình tổng thể của bộ sinh log dữ liệu mạng """
    total_records: int = 2000000        # Mặc định tối thiểu 2 triệu bản ghi theo KPI
    target_throughput: int = 5000       # Tốc độ sinh: records/giây
    error_rates: ErrorRates = field(default_factory=ErrorRates)

    def validate(self) -> bool:
        """
        Kiểm tra tính hợp lệ của cấu hình (Bounds Validation).
        Gợi ý logic:
            - total_records và target_throughput phải > 0
            - Các tỷ lệ lỗi trong error_rates phải nằm trong khoảng [0.0, 1.0]
            - Tổng các tỷ lệ lỗi không được vượt quá 1.0 (100%)
        """
        # TODO: Triển khai kiểm tra điều kiện cho total_records và target_throughput
        # TODO: Kiểm tra từng trường tỷ lệ lỗi của error_rates
        # TODO: Nếu có bất kỳ điều kiện nào vi phạm, ném ra ValueError với thông báo chi tiết
        return True