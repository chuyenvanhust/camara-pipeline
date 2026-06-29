import random
import requests
from typing import List, Dict

class RadiusDataGenerator:
    def __init__(self, seed: int = 42, gsma_url: str = "http://camara-mock-gsma-tac:8100"):
        self.seed = seed
        self.gsma_url = gsma_url
        self.tac_pool: List[str] = []
        random.seed(self.seed)

    def fetch_tac_pool_from_mock(self):
        """Gọi API sang GSMA TAC Mock để đồng bộ Pool dữ liệu TAC hợp lệ"""
        try:
            response = requests.get(f"{self.gsma_url}/tac", timeout=5)
            if response.status_code == 200:
                data = response.json()
                # Giả định mock trả về danh sách dict hoặc list chứa mã TAC
                if isinstance(data, list):
                    self.tac_pool = [item.get("tac") if isinstance(item, dict) else item for item in data]
                elif isinstance(data, dict) and "records" in data:
                    self.tac_pool = [item["tac"] for item in data["records"]]
                
                print(f"✅ [Simulator Generator] Synchronized {len(self.tac_pool)} valid TACs from GSMA Mock.")
            else:
                self._fallback_tac_pool()
        except requests.RequestException:
            print("⚠️ [Simulator Generator] GSMA Mock offline. Using local deterministic fallback TAC pool.")
            self._fallback_tac_pool()

    def _fallback_tac_pool(self):
        """Mạng lưới TAC dự phòng nếu Mock Service chưa kịp bật up"""
        self.tac_pool = [f"356123{i:02d}" for i in range(20)]

    def generate_luhn_checksum(self, number_str: str) -> str:
        """Tính toán số kiểm tra checksum theo thuật toán Luhn chuẩn mã IMEI/IMEISV"""
        digits = [int(d) for d in number_str]
        for i in range(len(digits) - 1, -1, -2):
            digits[i] *= 2
            if digits[i] > 9:
                digits[i] -= 9
        total = sum(digits)
        return str((10 - (total % 10)) % 10)

    def generate_valid_imei(self) -> str:
        """Sinh mã IMEI 15 chữ số hợp lệ chứa TAC thật đã đồng bộ"""
        tac = random.choice(self.tac_pool) if self.tac_pool else "35612300"
        serial = f"{random.randint(0, 999999):06d}"
        partial_imei = f"{tac}{serial}"
        checksum = self.generate_luhn_checksum(partial_imei)
        return f"{partial_imei}{checksum}"

    def generate_base_subscriber(self, index: int) -> Dict[str, str]:
        """Sinh cặp định danh cơ sở đồng bộ hoàn toàn với thuật toán của hlr_hss mock"""
        imsi = f"452010{index:09d}"
        msisdn = f"+8497{index:07d}"
        return {"imsi": imsi, "msisdn": msisdn}