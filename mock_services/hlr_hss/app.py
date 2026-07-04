from contextlib import asynccontextmanager
import os
from fastapi import FastAPI
from mock_services.hlr_hss.router import router as hlr_router
from mock_services.hlr_hss.seed import (
    generate_mock_subscribers_csv, generate_device_history_csv,
    load_subscribers_to_memory, load_devices_to_memory,
)

@asynccontextmanager
async def lifespan(app: FastAPI):
    prod_db_path = "mock_services/hlr_hss/data/subscribers.csv"
    device_db_path = "mock_services/hlr_hss/data/device_history.csv"

    if not os.path.exists(prod_db_path):
        print(f" [HLR/HSS] {prod_db_path} không tồn tại — tạo dữ liệu fallback (count=100000, seed=42).")
        generate_mock_subscribers_csv(prod_db_path, count=100000, seed_value=42)
    if not os.path.exists(device_db_path):
        generate_device_history_csv(device_db_path, count=100000, seed_value=42)

    load_subscribers_to_memory(prod_db_path)
    load_devices_to_memory(device_db_path)
    print(" [HLR/HSS Enterprise DB] Multi-mapping routing matrix connected (IMSI + IMEI).")
    yield
    print(" [HLR/HSS Enterprise DB] Service closed.")

app = FastAPI(title="HLR/HSS Core Network Mock API", version="1.1.0", lifespan=lifespan)

@app.get("/health")
def health_check():
    from mock_services.hlr_hss.seed import SUBSCRIBERS_BY_IMSI, DEVICES_BY_MSISDN
    return {
        "status": "ok", "service": "hlr-hss-mock",
        "subscribers": len(SUBSCRIBERS_BY_IMSI), "devices_tracked": len(DEVICES_BY_MSISDN),
        "uptime_seconds": 3600,
    }

app.include_router(hlr_router)