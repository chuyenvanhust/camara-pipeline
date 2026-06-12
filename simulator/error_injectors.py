import random
from typing import List, Dict, Any
from datetime import datetime, timedelta

class ErrorInjector:
    def __init__(self, config):
        self.config = config
        random.seed(config.seed)

    def inject_duplicates(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Nhân bản bản ghi giữ nguyên Session ID và Timestamp theo tỷ lệ cấu hình"""
        if self.config.duplicate_rate <= 0:
            return records
        
        output = []
        for rec in records:
            output.append(rec)
            if random.random() < self.config.duplicate_rate:
                # Tạo bản sao sâu nông để giữ nguyên định danh nhưng tạo luồng trùng
                dup_rec = rec.copy()
                output.append(dup_rec)
        return output

    def inject_late_arrivals(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Đẩy ingest_timestamp lên rất cao mô phỏng mạng trễ (Late Arrival)"""
        if self.config.late_arrival_rate <= 0:
            return records

        for rec in records:
            if random.random() < self.config.late_arrival_rate:
                # Đẩy thời gian nhận (ingest) muộn hơn thời gian sinh sự kiện (event) 3 tiếng (> 7200s threshold)
                evt_time = datetime.fromisoformat(rec["event_timestamp"].replace("Z", ""))
                late_ingest = evt_time + timedelta(hours=3)
                rec["ingest_timestamp"] = late_ingest.isoformat() + "Z"
        return records

    def inject_invalid_imei(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Phá hỏng checksum Luhn hoặc gán mã TAC ma không tồn tại"""
        if self.config.invalid_imei_rate <= 0:
            return records

        for rec in records:
            if random.random() < self.config.invalid_imei_rate:
                imei = list(rec["imei"])
                # Phá hoại bằng cách đổi chữ số cuối cùng (checksum digit) thành chữ số sai hoặc chữ chữ cái 'X'
                imei[-1] = "9" if imei[-1] != "9" else "0"
                rec["imei"] = "".join(imei)
        return records

    def inject_conflicts(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Tiêm các dạng Xung đột Nghiệp vụ (Conflict A / B / C) theo tỷ lệ 50/30/20"""
        if self.config.conflict_rate <= 0:
            return records

        for rec in records:
            if random.random() < self.config.conflict_rate:
                conflict_type = random.choices(["A", "B", "C"], weights=[50, 30, 20])[0]
                if conflict_type == "A":
                    # Conflict A: 1 IP gán cho 2 IMSI khác nhau tại cùng một mốc thời gian
                    rec["imsi"] = f"452010999{random.randint(1000, 9999)}"
                elif conflict_type == "B":
                    # Conflict B: 1 IMSI chiếm giữ 2 IP khác nhau đồng thời
                    rec["framed_ip"] = f"10.0.{random.randint(1,254)}.{random.randint(1,254)}"
                elif conflict_type == "C":
                    # Conflict C: Dấu hiệu SIM Swap (1 MSISDN liên kết với IMSI lạ hoàn toàn)
                    rec["imsi"] = f"452010888{random.randint(1000, 9999)}"
        return records

    def inject_missing_fields(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Xóa ngẫu nhiên các trường bắt buộc (Mandatory) của bản ghi"""
        if self.config.missing_field_rate <= 0:
            return records

        mandatory_fields = ["acct_status_type", "acct_session_id", "msisdn"]
        for rec in records:
            if random.random() < self.config.missing_field_rate:
                field_to_drop = random.choice(mandatory_fields)
                rec[field_to_drop] = ""  # Để trống trường dữ liệu
        return records