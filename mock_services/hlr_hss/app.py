from contextlib import asynccontextmanager
from fastapi import FastAPI
from mock_services.hlr_hss.router import router as hlr_router
from mock_services.hlr_hss.seed import generate_mock_subscribers_csv, load_subscribers_to_memory

@asynccontextmanager
async def lifespan(app: FastAPI):
    prod_db_path = "mock_services/hlr_hss/data/subscribers.csv"
    # Auto bootstrap dữ liệu production phục vụ các module sau kết nối port 8200
    generate_mock_subscribers_csv(prod_db_path, count=1000, seed_value=42)
    load_subscribers_to_memory(prod_db_path)
    print("🚀 [HLR/HSS Enterprise DB] Multi-mapping routing matrix connected.")
    yield
    print("🛑 [HLR/HSS Enterprise DB] Service closed.")

app = FastAPI(title="HLR/HSS Core Network Mock API", version="1.0.0", lifespan=lifespan)

@app.get("/health")
def health_check():
    from mock_services.hlr_hss.seed import SUBSCRIBERS_BY_IMSI
    return {
        "status": "ok",
        "service": "hlr-hss-mock",
        "subscribers": len(SUBSCRIBERS_BY_IMSI),
        "uptime_seconds": 3600
    }

app.include_router(hlr_router)