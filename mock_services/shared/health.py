#health.py
from datetime import datetime,UTC
from pydantic import BaseModel

# -----------------------------------------------------------------
# 1. Định nghĩa Schema Response bằng Pydantic
# -----------------------------------------------------------------
class HealthResponse(BaseModel):
    status: str          # VD: "OK", "DEGRADED"
    service_name: str    # VD: "gsma_tac_mock"
    record_count: int    # Số lượng bản ghi hiện có trong DB/Memory
    uptime_seconds: float # Thời gian chạy của service tính bằng giây

# -----------------------------------------------------------------
# 2. Helper tính toán Uptime
# -----------------------------------------------------------------
START_TIME = datetime.now(UTC)

def get_service_uptime() -> float:
    """ Tính toán số giây trôi qua kể từ khi khởi chạy dịch vụ """
    # TODO: Tính khoảng cách thời gian từ START_TIME đến hiện tại và trả về dạng seconds
    current_time = datetime.now(UTC)
    uptime = current_time - START_TIME
    return uptime.total_seconds()
    

def create_health_response(service_name: str, current_record_count: int) -> HealthResponse:
    """ Khởi tạo nhanh một object HealthResponse """
    # TODO: Gom các thông tin status, service_name, record_count và uptime để return
    uptime_seconds = get_service_uptime()
    if uptime_seconds < 60:
        status = "DEGRADED"  # Giả lập trạng thái chưa ổn định trong 1 phút đầu tiên sau khi khởi động
    else:        
        status = "OK"

    
    return HealthResponse(
        status=status,
        service_name=service_name,
        record_count=current_record_count,
        uptime_seconds=uptime_seconds
    )
    
