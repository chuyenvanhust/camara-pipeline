#simulator\error_injectors.py
import random
from typing import List, Dict, Any
from datetime import datetime, timedelta

from shared.seed_config import SUBSCRIBER_POOL_SIZE
from shared.subscriber_pool import base_subscriber, has_sim_swap, swap_new_imsi_subscriber

SWAP_ELIGIBLE_INDICES = [i for i in range(SUBSCRIBER_POOL_SIZE) if has_sim_swap(i)]
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
                rec["ingest_timestamp"] = late_ingest.isoformat() + "Z"
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
        Tiêm Conflict A/B/C ĐỘC LẬP, mỗi loại đúng 2% trên tổng số session,
        không chia sẻ chung 1 ngân sách xác suất (fix rate).
        """
        CONFLICT_RATE_A = 0.02
        CONFLICT_RATE_B = 0.02
        CONFLICT_RATE_C = 0.02

        output: List[Dict[str, Any]] = []
        i, n = 0, len(records)

        while i < n:
            start_rec = records[i]
            stop_rec = records[i + 1] if i + 1 < n else None
            output.append(start_rec)

            if stop_rec is None:
                i += 2
                continue

            # Roll 1 lần duy nhất, chia thành 3 khoảng KHÔNG chồng lấn
            # -> mỗi loại đúng 2%, tổng 3 loại = 6%, loại trừ lẫn nhau
            r = self.rng.random()
            if r < CONFLICT_RATE_A:
                conflict_type = "A"
            elif r < CONFLICT_RATE_A + CONFLICT_RATE_B:
                conflict_type = "B"
            elif r < CONFLICT_RATE_A + CONFLICT_RATE_B + CONFLICT_RATE_C:
                conflict_type = "C"
            else:
                conflict_type = None

            if conflict_type == "A":
                mutated_stop = dict(stop_rec)
                cur_idx = start_rec.get("_sub_idx", 0)
                other_idx = (cur_idx + 1) % SUBSCRIBER_POOL_SIZE
                if other_idx == cur_idx:
                    other_idx = (cur_idx + 2) % SUBSCRIBER_POOL_SIZE
                other_sub = base_subscriber(other_idx)

                if self.rng.random() < 0.5:
                    mutated_stop["imsi"] = other_sub["imsi"]
                else:
                    mutated_stop["msisdn"] = other_sub["msisdn"]
                output.append(mutated_stop)

            elif conflict_type == "B":
                extra_start = dict(start_rec)
                extra_start["acct_session_id"] = f"SESS_B_{i:010d}"
                extra_start["acct_session_time"] = "0"
                try:
                    t0 = datetime.fromisoformat(start_rec["event_timestamp"].replace("Z", ""))
                    t1 = datetime.fromisoformat(stop_rec["event_timestamp"].replace("Z", ""))
                    mid = t0 + (t1 - t0) / 2
                    extra_start["event_timestamp"] = mid.isoformat() + "Z"
                    extra_start["ingest_timestamp"] = (mid + timedelta(seconds=2)).isoformat() + "Z"
                except (ValueError, TypeError):
                    pass
                output.append(extra_start)
                output.append(stop_rec)

            elif conflict_type == "C":
                output.append(stop_rec)

                # FIX: giữ NGUYÊN msisdn của chính session đang xét
                # (đúng định nghĩa "cùng msisdn mapping sang imsi mới"),
                # không lấy msisdn của 1 subscriber ngẫu nhiên khác nữa.
                cur_idx = SWAP_ELIGIBLE_INDICES[self._swap_cursor % len(SWAP_ELIGIBLE_INDICES)]
                self._swap_cursor += 1

                same_msisdn = base_subscriber(cur_idx)["msisdn"]
                new_imsi = swap_new_imsi_subscriber(cur_idx, SUBSCRIBER_POOL_SIZE)["imsi"]
                swap_session_id = f"SESS_C_{i:010d}"

                try:
                    t_stop = datetime.fromisoformat(stop_rec["event_timestamp"].replace("Z", ""))
                    t_swap_start = t_stop + timedelta(minutes=10)  # sau khi session cũ kết thúc -> hợp lý
                    t_swap_stop = t_swap_start + timedelta(seconds=60)
                except (ValueError, TypeError):
                    t_swap_start = t_swap_stop = None

                swap_start = dict(start_rec)
                swap_start.update({
                    "acct_session_id": swap_session_id,
                    "acct_status_type": "Start",
                    "acct_session_time": "0",
                    "msisdn": same_msisdn,   # giữ nguyên MSISDN
                    "imsi": new_imsi,        # đổi IMSI mới -> tín hiệu SIM Swap
                    "_sub_idx": cur_idx,
                })
                swap_stop = dict(stop_rec)
                swap_stop.update({
                    "acct_session_id": swap_session_id,
                    "acct_status_type": "Stop",
                    "acct_session_time": "60",
                    "msisdn": same_msisdn,
                    "imsi": new_imsi,
                    "_sub_idx": cur_idx,
                })
                if t_swap_start is not None:
                    swap_start["event_timestamp"] = t_swap_start.isoformat() + "Z"
                    swap_start["ingest_timestamp"] = (t_swap_start + timedelta(seconds=2)).isoformat() + "Z"
                    swap_stop["event_timestamp"] = t_swap_stop.isoformat() + "Z"
                    swap_stop["ingest_timestamp"] = (t_swap_stop + timedelta(seconds=2)).isoformat() + "Z"

                output.append(swap_start)
                output.append(swap_stop)
            else:
                output.append(stop_rec)

            i += 2

        return output

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