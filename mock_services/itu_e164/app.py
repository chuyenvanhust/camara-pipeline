from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
import uuid
import asyncio
from mock_services.itu_e164.router import router as itu_router
from mock_services.itu_e164.seed import generate_mock_itu_csv, load_itu_data_to_memory

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Trình tự khởi động (Startup)
    generate_mock_itu_csv()
    load_itu_data_to_memory()
    print("🚀 [ITU E.164] Static router engines online.")
    yield
    print("🛑 [ITU E.164] Service stopped.")

app = FastAPI(title="ITU-T E.164 Mock API", version="1.0.0", lifespan=lifespan)

# Kế thừa Phase 1: Giả lập bộ Middleware tích hợp cơ chế X-Inject-Fault từ shared/errors.py
@app.middleware("http")
async def inject_fault_middleware(request: Request, call_next):
    fault_header = request.headers.get("X-Inject-Fault")
    if fault_header:
        if "delay=" in fault_header:
            ms = int(fault_header.split("delay=")[1])
            await asyncio.sleep(ms / 1000.0)
        elif "status=503" in fault_header:
            return JSONResponse(
                status_code=503,
                content={"error": "SERVICE_UNAVAILABLE", "message": "Injected Service Interruption", "request_id": str(uuid.uuid4())}
            )
        elif "timeout" in fault_header:
            await asyncio.sleep(10)  # Ép client rớt kết nối do quá hạn
    return await call_next(request)

app.include_router(itu_router)