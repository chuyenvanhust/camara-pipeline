from datetime import datetime, timezone
"""
Cấu hình DUY NHẤT cho toàn bộ hệ sinh dữ liệu (simulator + gsma_tac mock +
hlr_hss mock). Mọi seed.py / generator.py PHẢI import từ đây,
KHÔNG hard-code lại seed hay count ở nơi khác — đây chính là nguyên nhân
khiến dữ liệu trước đây bị lệch.
"""
RADIUS_SIMULATION_START = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
MASTER_SEED: int = 42


SUBSCRIBER_POOL_SIZE: int = 1_000_000


TAC_POOL_SIZE: int = 2_000

GSMA_MOCK_URL: str = "http://camara-mock-gsma-tac:8100"