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
                if isinstance(data, list):
                    self.tac_pool = [item.get("tac") if isinstance(item, dict) else item for item in data]
                elif isinstance(data, dict) and "records" in data:
                    self.tac_pool = [item["tac"] for item in data["records"]]
                
                print(f"✅ [Simulator Generator] Synchronized {len(self.tac_pool)} valid TACs from GSMA Mock.")
            else:
                self._fallback_tac_pool()
        except requests.RequestException:
            print("⚠️ [Simulator Generator] GSMA Mock offline. Using local sequential fallback TAC pool.")
            self._fallback_tac_pool()

    def _fallback_tac_pool(self):
        """
        [CẬP NHẬT] Tạo pool TAC tuần tự từ 000000 đến 999999.
        Việc tạo 1 triệu chuỗi tốn khoảng 80MB RAM, hoàn toàn ổn định cho môi trường hiện tại.
        """
        print("🔄 [Simulator] Generating 1,000,000 sequential TACs (000000-999999)...")
        # Tạo danh sách các chuỗi từ "000000" đến "999999"
        self.tac_pool = [f"{i:06d}" for i in range(1000000)]

    def generate_luhn_checksum(self, number_str: str) -> str:
        digits = [int(d) for d in number_str]
        # Sửa range để khớp với Validator: nhân đôi các index 13, 11, 9, 7, 5, 3, 1
        for i in range(len(digits) - 1, -1, -2):
            val = digits[i] * 2
            digits[i] = val if val < 10 else val - 9
        total = sum(digits)
        return str((10 - (total % 10)) % 10)

    def generate_valid_imei(self) -> str:
        """
        [CẬP NHẬT] Sinh mã IMEI 15 chữ số.
        Cấu trúc: TAC(6 số) + FAC(2 số) + SNR(6 số) + Checksum(1 số) = 15 số.
        """
        # Chọn ngẫu nhiên 1 TAC từ pool tuần tự 000000-999999
        tac = random.choice(self.tac_pool) if self.tac_pool else f"{random.randint(0, 999999):06d}"
        
        # 2 số tiếp theo thường là Final Assembly Code (mặc định 00)
        fac = "00"
        
        # 6 số tiếp theo là Serial Number ngẫu nhiên
        snr = f"{random.randint(0, 999999):06d}"
        
        # Gộp thành 14 số đầu
        partial_imei = f"{tac}{fac}{snr}"
        
        # Tính số thứ 15 (Checksum)
        checksum = self.generate_luhn_checksum(partial_imei)
        
        return f"{partial_imei}{checksum}"

    def generate_base_subscriber(self, index: int) -> Dict[str, str]:
        """Sinh cặp định danh cơ sở đồng bộ với mock hlr_hss"""
        imsi = f"452010{index:09d}"
        msisdn = f"+8497{index:07d}"
        return {"imsi": imsi, "msisdn": msisdn}