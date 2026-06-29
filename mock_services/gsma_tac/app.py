#mock_services\gsma_tac\app.py
from contextlib import asynccontextmanager
from fastapi import FastAPI
from mock_services.gsma_tac.router import router as tac_router
from mock_services.gsma_tac.seed import load_tac_csv_to_memory

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Logic tự động kích hoạt khi Ứng dụng Bắt đầu (Startup)
    try:
        load_tac_csv_to_memory()
        print("🚀 [GSMA TAC] Static CSV database successfully loaded into memory.")
    except Exception as e:
        print(f"⚠️ Warning: Could not auto-load production CSV on boot: {e}")
    
    yield  # Nơi ứng dụng chạy nền nhận request.
    
    # Logic tự động kích hoạt khi Ứng dụng Tắt (Shutdown)
    print("🛑 [GSMA TAC] Shutting down service.")

app = FastAPI(
    title="GSMA TAC Mock API",
    description="Mô phỏng GSMA TAC Allocation Database phục vụ Stage S2 - Validation",
    version="1.0.0",
    lifespan=lifespan  # Đăng ký quản lý vòng đời ứng dụng tại đây
)

app.include_router(tac_router)