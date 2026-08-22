import random
from typing import List, Dict
from shared.seed_config import MASTER_SEED, TAC_POOL_SIZE, SUBSCRIBER_POOL_SIZE
from shared.tac_pool import generate_tac_codes
from shared.subscriber_pool import base_subscriber


class RadiusDataGenerator:
    def __init__(self, seed: int = MASTER_SEED):
        self.seed = seed
        self.rng = random.Random(self.seed)  # RNG cục bộ, KHÔNG đụng global random
        self.tac_pool: List[str] = generate_tac_codes(self.seed, TAC_POOL_SIZE)


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