from datetime import datetime, timezone
"""
Cấu hình DUY NHẤT cho toàn bộ hệ sinh dữ liệu (simulator + gsma_tac mock +
hlr_hss mock). Mọi seed.py / generator.py PHẢI import từ đây,
KHÔNG hard-code lại seed hay count ở nơi khác — đây chính là nguyên nhân
khiến dữ liệu trước đây bị lệch.
"""
RADIUS_SIMULATION_START = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
MASTER_SEED: int = 42

# Số thuê bao gốc (IMSI/MSISDN). Bắt buộc khớp giữa:
#   - mock_services/hlr_hss/seed.py
#   - simulator/generator.py (giới hạn index tối đa được phép sinh)
SUBSCRIBER_POOL_SIZE: int = 100_000

# Số TAC. Bắt buộc khớp giữa:
#   - mock_services/gsma_tac/seed.py
#   - simulator/generator.py (fallback pool khi GSMA mock offline)
TAC_POOL_SIZE: int = 2_000

GSMA_MOCK_URL: str = "http://camara-mock-gsma-tac:8100"