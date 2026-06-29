#!/usr/bin/env python3
#pipeline\conflict_resolution\swap_detector.py
import requests
from pyspark.sql import Row

class SwapDetector:
    """
    Module phụ trách xử lý hậu kỳ riêng cho Conflict loại C.
    Chứa các logic nghiệp vụ tương tác Network/API I/O: Gọi HLR/HSS Mock và đối chiếu DB PostgreSQL.
    """
    def __init__(self, hlr_mock_url: str = "http://camara-mock-hlr-hss:8200", db_connection=None):
        self.hlr_url = hlr_mock_url
        self.db = db_connection

    def verify_and_emit_swap(self, conflict_c_row: Row) -> dict | None:
        """
        Xử lý tuần tự cho từng bản ghi nghi ngờ phát hiện hoán đổi SIM (Conflict C).
        
        Args:
            conflict_c_row (Row): Chứa thông tin bản ghi biến động msisdn, imsi, event_timestamp...
        Returns:
            dict: Payload cấu trúc `swap_event` chuẩn hóa để emit vào Kafka nếu HLR xác nhận.
            None: Nếu HLR/HSS từ chối xác nhận sự kiện hoán đổi.
        """
        msisdn = conflict_c_row["msisdn"]
        new_imsi = conflict_c_row["imsi"]
        detected_at = conflict_c_row["event_timestamp"]
        
        # -------------------------------------------------------------------------
        # TODO 1: GỌI HLR/HSS MOCK API XÁC MINH LỊCH SỬ IMSI
        # Endpoint: GET /subscribers/{msisdn}/imsi-history
        # -------------------------------------------------------------------------
        try:
            response = requests.get(f"{self.hlr_url}/subscribers/{msisdn}/imsi-history", timeout=5)
            if response.status_code != 200:
                return None
            
            hlr_data = response.json() # Cấu trúc giả định: {"msisdn": "...", "history": [{"imsi": "...", "assigned_at": "..."}]}
            history = hlr_data.get("history", [])
            
            # Tìm kiếm bản ghi trùng khớp với new_imsi trong danh sách lịch sử của HLR
            matched_hlr_record = next((item for item in history if item["imsi"] == new_imsi), None)
            if not matched_hlr_record:
                return None # Không tìm thấy trên HLR -> Không xác nhận SIM Swap hợp lệ
                
            confirmed_at = matched_hlr_record["assigned_at"]
            old_imsi = hlr_data.get("old_imsi") # Hoặc lấy từ phần tử kế cận trong mảng lịch sử
            
        except requests.RequestException:
            # Ghi log lỗi kết nối mạng ngoại vi tại đây nếu cần
            return None

        # -------------------------------------------------------------------------
        # TODO 2: ĐỐI CHIẾU LỊCH SỬ RADIUS SESSIONS TRONG POSTGRESQL (NẾU CÓ)
        # Mục đích: Tính toán khoảng cách thời gian hoặc kiểm tra trạng thái cũ để tối ưu hóa
        # -------------------------------------------------------------------------
        if self.db:
            query = "SELECT last_active FROM radius_sessions WHERE msisdn = %s ..."
            self.db.execute(query, (msisdn,))
            pass

        # -------------------------------------------------------------------------
        # TODO 3: EMIT SWAP EVENT PAYLOAD
        # Cấu trúc JSON Output đồng bộ bàn giao cho module CAMARA API
        # -------------------------------------------------------------------------
        swap_event = {
            "msisdn": msisdn,
            "old_imsi": old_imsi,
            "new_imsi": new_imsi,
            "swap_type": "SIM_SWAP",
            "detected_at": str(detected_at),
            "confirmed_at": str(confirmed_at),
            "source": "RADIUS_CONFLICT_C"
        }
        
        # Thao tác đẩy dữ liệu vào Kafka producer sẽ được thực hiện bởi Driver/Worker gọi hàm này
        return swap_event