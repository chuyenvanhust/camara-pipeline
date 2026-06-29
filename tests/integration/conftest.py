# tests/integration/conftest.py
import os
import sys
from pathlib import Path
from dotenv import load_dotenv
from api.dependencies.auth import verify_api_key


# 1. Ép hệ thống đọc và GHI ĐÈ (override=True) toàn bộ biến môi trường từ file .env local
root_dir = Path(__file__).resolve().parent.parent.parent
env_path = root_dir / ".env.test"
load_dotenv(dotenv_path=env_path, override=True)

# 2. Thiết lập policy cho Windows Selector Loop
import asyncio
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

import pytest
import asyncpg
from httpx import AsyncClient, ASGITransport

from api.main import app
import api.dependencies.database as db_module

# ── 1. Session-scoped event loop ─────────────────────────────────────────────
'''@pytest.fixture(scope="session")
def event_loop_policy():
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    return asyncio.get_event_loop_policy()'''

# ── 2. DB Pool — session scope ───────────────────────────────────────────────
@pytest.fixture(scope="session")
async def db_pool(event_loop_policy):
    """
    Tạo asyncpg Pool cho integration test. 
    Sử dụng toán tử toán logíc hoặc fallback an toàn hướng về localhost/postgres.
    """
    # Ép buộc sử dụng biến cấu hình cục bộ của bạn, xóa bỏ hoàn toàn hardcode 'avnadmin' nếu có
    db_user = os.getenv("DB_USER") or "postgres"
    db_password = os.getenv("DB_PASSWORD") or "camara"
    db_host = os.getenv("DB_HOST") or "postgres"
    db_port = os.getenv("DB_PORT") or "5433"
    db_name = os.getenv("DB_NAME") or "camara_db"

    pool = await asyncpg.create_pool(
        host=db_host,
        port=int(db_port),
        user=db_user,
        password=db_password,
        database=db_name,
        min_size=1,
        max_size=5,
    )
    
    # Inject pool vào hệ thống FastAPI dependencies
    db_module._pool = pool
    yield pool
    db_module._pool = None
    await pool.close()
    
@pytest.fixture(scope="session", autouse=True)
async def setup_db_schema(db_pool):
    fixtures_dir = Path(__file__).resolve().parent / "fixtures"
    
    async with db_pool.acquire() as conn:
        # CHỈ CẦN SEED DATA. KHÔNG CHẠY MIGRATION Ở ĐÂY.
        # Docker đã chạy migration thông qua /docker-entrypoint-initdb.d/
        
        for filename in ["seed_data.sql", "edge_cases.sql"]:
            file_path = fixtures_dir / filename
            if file_path.exists():
                try:
                    # Đọc và chạy file seed
                    sql = file_path.read_text(encoding="utf-8")
                    await conn.execute(sql)
                except Exception as e:
                    print(f"Error seeding {filename}: {e}")

# ── 3. DB Client — session scope ─────────────────────────────────────────────
# THAY ĐỔI QUAN TRỌNG: scope="session" thay vì "function"
# Lý do: function-scoped async fixture chạy trên loop khác với
# session-scoped db_pool → "attached to different loop".
# Clean DB được thực hiện qua clean_db fixture riêng (cũng session scope).
@pytest.fixture(scope="session")
async def db_client(db_pool):
    """
    Connection dùng để inject test data.
    Session scope — dùng chung cho toàn bộ session.
    Mỗi test tự gọi TRUNCATE qua clean_db trước/sau khi chạy.
    """
    async with db_pool.acquire() as connection:
        yield connection


# ── 4. API Client — session scope ────────────────────────────────────────────
@pytest.fixture(scope="session")
async def api_client(db_pool):
    # Override auth: bypass verify_api_key cho toàn bộ integration test
    # Integration test kiểm tra business logic, không kiểm tra auth
    # (auth được test riêng trong test_tc35_api_key_invalid với key sai thật)
    app.dependency_overrides[verify_api_key] = lambda: "test_key"

    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
        timeout=5.0,
    ) as client:
        yield client

    app.dependency_overrides.pop(verify_api_key, None)


# ── 5. Clean DB — dùng db_pool trực tiếp, KHÔNG dùng db_client ──────────────
@pytest.fixture(autouse=True)
async def clean_db(db_pool):
    """
    Truncate các bảng test trước mỗi test function (yield trước,
    truncate sau — đảm bảo state sạch sau mỗi test kể cả khi fail).

    KHÔNG nhận db_client (connection riêng) — tự acquire connection
    mới từ pool để tránh conflict với connection db_client đang giữ.

    autouse=True + scope mặc định (function) nhưng body là sync
    wrapper gọi async qua pool.acquire() — đây là pattern an toàn
    nhất để tránh loop mismatch trong teardown.
    """
    # Setup: không làm gì (test bắt đầu với DB sạch từ lần clean trước)
    yield

    # Teardown: truncate sau khi test xong
    tables = ["swap_event", "duplicate_log", "invalid_log",
          "conflict_log", "radius_sessions", "processed_records_log"]
    try:
        async with db_pool.acquire() as conn:
            for table in tables:
                try:
                    await conn.execute(f"TRUNCATE TABLE {table} CASCADE;")
                except asyncpg.UndefinedTableError:
                    pass  # Bảng chưa tồn tại trong môi trường test
    except Exception:
        pass  # Không để clean_db fail làm crash teardown