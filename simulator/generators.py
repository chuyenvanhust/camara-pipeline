import random
import requests
from typing import List, Dict
from shared.seed_config import MASTER_SEED, GSMA_MOCK_URL, TAC_POOL_SIZE, SUBSCRIBER_POOL_SIZE
from shared.tac_pool import generate_tac_codes
from shared.subscriber_pool import base_subscriber


class RadiusDataGenerator:
    def __init__(self, seed: int = MASTER_SEED, gsma_url: str = GSMA_MOCK_URL):
        self.seed = seed
        self.gsma_url = gsma_url
        self.tac_pool: List[str] = []
        self.rng = random.Random(self.seed)  # RNG cục bộ, KHÔNG đụng global random

    def fetch_tac_pool_from_mock(self):
        try:
            response = requests.get(
                f"{self.gsma_url}/tac",
                params={"page": 1, "page_size": TAC_POOL_SIZE},  # lấy đủ cả pool, không chỉ trang đầu
                timeout=5,
            )
            if response.status_code == 200:
                data = response.json()
                records = []
                if isinstance(data, list):
                    records = data
                elif isinstance(data, dict) and "items" in data:   # FIX: đúng key theo PaginatedTacResponse
                    records = data["items"]

                self.tac_pool = [
                    (item.get("tac_code") or item.get("tac"))
                    for item in records
                    if isinstance(item, dict)
                ]
                self.tac_pool = [t for t in self.tac_pool if t]

                if not self.tac_pool:
                    # FIX: sync "thành công" (200 OK) nhưng rỗng vẫn phải fallback,
                    # tránh im lặng sinh TAC ngẫu nhiên không khớp GSMA DB
                    print(" [Simulator Generator] GSMA trả về 0 TAC hợp lệ, dùng fallback deterministic.")
                    self._fallback_tac_pool()
                else:
                    print(f" [Simulator Generator] Synchronized {len(self.tac_pool)} valid TACs from GSMA Mock.")
            else:
                self._fallback_tac_pool()
        except requests.RequestException:
            self._fallback_tac_pool()

    def _fallback_tac_pool(self):
        self.tac_pool = generate_tac_codes(self.seed, TAC_POOL_SIZE)
        print(f" [Simulator] Generated {len(self.tac_pool)} deterministic fallback TACs "
              f"(identical to gsma_tac seed, seed={self.seed}).")

    def generate_luhn_checksum(self, number_str: str) -> str:
        digits = [int(d) for d in number_str]
        for i in range(len(digits) - 1, -1, -2):
            val = digits[i] * 2
            digits[i] = val if val < 10 else val - 9
        total = sum(digits)
        return str((10 - (total % 10)) % 10)

    def generate_valid_imei(self) -> str:
        tac = self.rng.choice(self.tac_pool) if self.tac_pool else f"{self.rng.randint(0, 999999):06d}"
        fac = "00"
        snr = f"{self.rng.randint(0, 999999):06d}"
        partial_imei = f"{tac}{fac}{snr}"
        checksum = self.generate_luhn_checksum(partial_imei)
        return f"{partial_imei}{checksum}"

    def generate_base_subscriber(self, index: int) -> Dict[str, str]:
        """Sinh cặp định danh cơ sở. Bắt buộc index < SUBSCRIBER_POOL_SIZE
        để đảm bảo IMSI/MSISDN sinh ra LUÔN tồn tại trong HLR mock đã seed."""
        sub = base_subscriber(index)
        if index >= SUBSCRIBER_POOL_SIZE:
            raise ValueError(
                f"index={index} vượt quá SUBSCRIBER_POOL_SIZE={SUBSCRIBER_POOL_SIZE}. "
                "Tăng SUBSCRIBER_POOL_SIZE trong shared/seed_config.py và seed lại HLR mock "
                "trước khi sinh thêm dữ liệu, nếu không bản ghi sẽ không tra cứu được ở HLR."
            )
        if not sub["msisdn"].startswith("+"):
            sub["msisdn"] = "+" + sub["msisdn"]
        return sub