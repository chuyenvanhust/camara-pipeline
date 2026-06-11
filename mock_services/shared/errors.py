#errors.py
import asyncio
import time
import random
import uuid
from fastapi import Request, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from starlette.middleware.base import BaseHTTPMiddleware

# -----------------------------------------------------------------
# 1. Định nghĩa Schema Error chuẩn CAMARA Mock
# -----------------------------------------------------------------
class StandardErrorResponse(BaseModel):
    error: str       = Field(..., description="Mã lỗi viết hoa dạng chuỗi, VD: NOT_FOUND, INTERNAL_ERROR")
    message: str     = Field(..., description="Thông tin chi tiết, mô tả lý do lỗi phục vụ debug")
    request_id: str  = Field(default_factory=lambda: str(uuid.uuid4()), description="Mã định danh duy nhất của request")

# -----------------------------------------------------------------
# 2. Fault Injection Middleware (X-Inject-Fault Handler)
# -----------------------------------------------------------------
class FaultInjectionMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        # Đọc giá trị header "X-Inject-Fault" từ request đến
        fault = request.headers.get("X-Inject-Fault")
        
        if fault:
            # --- Trường hợp 1: delay=X (Giả lập nghẽn mạng / tăng latency) ---
            if fault.startswith("delay="):
                delay_ms = int(fault.split("=")[1])
                await asyncio.sleep(delay_ms / 1000)
                pass

            # --- Trường hợp 2: status=X (Giả lập sập dịch vụ / lỗi hệ thống hạ tầng) ---
            elif fault.startswith("status="):
                # TODO: Tách lấy mã HTTP Status code (VD: status=503 -> 503)
                status_code = int(fault.split("=")[1])
                error_response = StandardErrorResponse(
                    error=f"INJECTED_ERROR_{status_code}",
                    message=f"This is a simulated error with status code {status_code} for testing purposes."
                )
                # TODO: Trả thẳng một JSONResponse chứa StandardErrorResponse với status_code tương ứng mà không gọi call_next
                return JSONResponse(
                    status_code=status_code,
                    content=error_response.model_dump()
                )
                
                

            # --- Trường hợp 3: error_rate=X (Giả lập tỷ lệ lỗi chập chờn Flaky Service) ---
            elif fault.startswith("error_rate="):
                # TODO: Tách lấy tỷ lệ float (VD: error_rate=0.2 -> 20%)
                error_rate = float(fault.split("=")[1])
                
                # TODO: Dùng random.random() bốc thăm. Nếu trúng tỷ lệ, trả lỗi ngẫu nhiên (VD: 500 Internal Error)
                if random.random() < error_rate:
                    status_code = 500
                    error_response = StandardErrorResponse(
                        error=f"INJECTED_ERROR_{status_code}",
                        message=f"This is a simulated random error with status code {status_code} for testing purposes."
                    )
                    return JSONResponse(
                        status_code=status_code,
                        content=error_response.model_dump()
                    )
                pass

            # --- Trường hợp 4: timeout (Giả lập drop gói tin / hệ thống treo) ---
            elif fault == "timeout":
                # TODO: Treo request mãi mãi bằng vòng lặp vô hạn hoặc ngủ cực lâu để phía Pipeline kích hoạt timeout logic
                cnt = 0
                flag = True
                while True:
                    await asyncio.sleep(1)
                    cnt += 1
                    if cnt > 1000: 
                        flag = False
                        break
                if not flag:
                    error_response = StandardErrorResponse(
                        error=f"INJECTED_ERROR_TIMEOUT",
                        message=f"This is a simulated timeout error for testing purposes after waiting for a long time."
                    )
                    return JSONResponse(
                        status_code=504,
                        content=error_response.dict()
                    )
                pass

        # Nếu không trúng header fault hoặc fault đã xử lý delay/error_rate thành công: tiếp tục luồng chạy bình thường
        response = await call_next(request)
        return response