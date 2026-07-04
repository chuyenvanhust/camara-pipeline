"""
Settings đọc từ environment variables, dùng pydantic-settings.

Tất cả giá trị đều có fallback mặc định cho môi trường dev/lab.
Production cần set đầy đủ qua .env hoặc Docker environment.

Dùng singleton `settings` (import trực tiếp):
    from api.config import settings
    print(settings.api_key)
"""

"""
Settings đọc từ environment variables.

Không dùng pydantic-settings để tránh dependency bổ sung.
Đọc trực tiếp qua os.getenv với fallback mặc định cho dev/lab.
"""

import os
from pydantic import BaseModel


class Settings(BaseModel):
    """
    Cấu hình API server.
    Đọc từ os.getenv tại thời điểm khởi tạo singleton.
    """

    api_key: str
    api_host: str
    api_port: int
    database_url: str
    db_pool_size: int
    gsma_tac_api_url: str
    hlr_hss_api_url: str
    itu_e164_api_url: str


# Singleton — khởi tạo 1 lần, import trực tiếp từ mọi nơi
settings = Settings(
    api_key=os.getenv("API_KEY", "dev-secret"),
    api_host=os.getenv("API_HOST", "0.0.0.0"),
    api_port=int(os.getenv("API_PORT", "8000")),
    database_url=os.getenv(
        "DATABASE_URL",
        "postgresql://camara:camara@postgres:5432/camara_db",
    ),
    db_pool_size=int(os.getenv("DB_POOL_SIZE", "10")),
    gsma_tac_api_url=os.getenv("GSMA_TAC_API_URL", "http://camara-mock-gsma-tac:8100"),
    hlr_hss_api_url=os.getenv("HLR_HSS_API_URL", "http://camara-mock-hlr-hss:8200"),
    itu_e164_api_url=os.getenv("ITU_E164_API_URL", "http://camara-mock-itu-e164:8300"),
)
