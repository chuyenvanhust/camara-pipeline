#simulator\error_injectors.py
import random
from typing import List, Dict, Any
from datetime import datetime, timedelta

from shared.seed_config import SUBSCRIBER_POOL_SIZE
from shared.subscriber_pool import (
    base_subscriber, has_sim_swap, swap_new_imsi_subscriber,
    has_device_swap, device_swap_new_imei_subscriber,
)


SWAP_ELIGIBLE_INDICES = [i for i in range(SUBSCRIBER_POOL_SIZE) if has_sim_swap(i)]
DEVICE_SWAP_ELIGIBLE_INDICES = [i for i in range(SUBSCRIBER_POOL_SIZE) if has_device_swap(i)]
class ErrorInjector:
    """
    Tiêm lỗi/kịch bản nghiệp vụ vào dữ liệu RADIUS đã sinh sạch.
    Mỗi loại lỗi có tỷ lệ riêng, lấy trực tiếp từ SimulatorConfig
    (duplicate_rate, late_arrival_rate, invalid_imei_rate, conflict_rate,
    missing_field_rate) — không gộp chung 1 ngân sách.

    Dùng RNG cục bộ (self.rng = random.Random(config.seed)), KHÔNG gọi
    random.seed() trên global random module, để tránh bị các thành phần
    khác (RadiusDataGenerator, gsma_tac/seed.py, hlr_hss/seed.py) tranh
    chấp/reseed lẫn nhau khi chạy chung tiến trình.
    """

    def __init__(self, config):
        self.config = config
        self.rng = random.Random(config.seed)
        self._swap_cursor = 0
        self._device_swap_cursor = 0

    def inject_duplicates(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Nhân bản bản ghi giữ nguyên Session ID và Timestamp theo tỷ lệ duplicate_rate"""
        if self.config.duplicate_rate <= 0:
            return records

        output = []
        for rec in records:
            output.append(rec)
            if self.rng.random() < self.config.duplicate_rate:
                dup_rec = rec.copy()
                output.append(dup_rec)
        return output

    def inject_late_arrivals(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Đẩy ingest_timestamp muộn hơn event_timestamp 3 tiếng (> 7200s threshold),
        theo tỷ lệ late_arrival_rate, mô phỏng mạng trễ."""
        if self.config.late_arrival_rate <= 0:
            return records

        for rec in records:
            if self.rng.random() < self.config.late_arrival_rate:
                evt_time = datetime.fromisoformat(rec["event_timestamp"].replace("Z", ""))
                late_ingest = evt_time + timedelta(hours=3)
                rec["ingest_timestamp"] = late_ingest.strftime("%Y-%m-%dT%H:%M:%SZ")
        return records

    def inject_invalid_imei(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Phá hỏng checksum Luhn của IMEI (đổi chữ số cuối) theo tỷ lệ invalid_imei_rate
        -> IMEI không hợp lệ, GSMA TAC mock sẽ reject khi validate."""
        if self.config.invalid_imei_rate <= 0:
            return records

        for rec in records:
            if self.rng.random() < self.config.invalid_imei_rate:
                if "imei" not in rec or not rec["imei"]:
                    continue
                imei = list(rec["imei"])
                imei[-1] = "9" if imei[-1] != "9" else "0"
                rec["imei"] = "".join(imei)
        return records

    def inject_conflicts(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Conflict A/B/C/D have been removed to prevent oscillation and duplicate events.
        SIM and Device Swaps are now persistently generated in simulator.py.
        """
        return records


    def inject_missing_fields(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Xóa ngẫu nhiên 1 trong các trường bắt buộc theo tỷ lệ missing_field_rate"""
        if self.config.missing_field_rate <= 0:
            return records

        mandatory_fields = ["acct_status_type", "acct_session_id", "msisdn"]
        for rec in records:
            if self.rng.random() < self.config.missing_field_rate:
                field_to_drop = self.rng.choice(mandatory_fields)
                rec[field_to_drop] = ""
        return records