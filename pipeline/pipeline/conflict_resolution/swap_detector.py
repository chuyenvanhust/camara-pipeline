#!/usr/bin/env python3
# pipeline/conflict_resolution/swap_detector.py
"""
Xác minh Conflict C (SIM Swap, identity_type="imsi") và Conflict D
(Device Swap, identity_type="imei") qua HLR/HSS Mock — dùng endpoint
BATCH /subscribers/batch-history thay vì gọi từng record một.

Lịch sử sửa lỗi:
  [FIX-1] old_imsi suy từ history array (mock không có field top-level).
  [FIX-2] write_swap_events() ghi list kết quả vào swap_event.
  [FIX-3] Chuyển sang batch API — trước đây N request tuần tự/mỗi-thread
          riêng lẻ mỗi micro-batch (~160 req/batch) là nguyên nhân chính
          Spark báo "Current batch is falling behind".
"""

import logging
from typing import Dict, List, Optional

import psycopg2
import requests

logger = logging.getLogger(__name__)


class SwapDetector:
    def __init__(
        self,
        identity_type: str,                       # "imsi" (Conflict C) | "imei" (Conflict D)
        hlr_mock_url: str = "http://camara-mock-hlr-hss:8200",
        timeout_seconds: float = 10.0,
        batch_size: int = 500,                     # khớp max_length của BatchHistoryRequest
    ):
        assert identity_type in ("imsi", "imei")
        self.identity_type = identity_type
        self.swap_type = "SIM_SWAP" if identity_type == "imsi" else "DEVICE_SWAP"
        self.hlr_url = hlr_mock_url
        self.timeout_seconds = timeout_seconds
        self.batch_size = batch_size

    def verify_batch(self, conflict_rows: List[Dict]) -> List[Dict]:
        """
        Args: conflict_rows — mỗi phần tử có msisdn, event_timestamp, và
            field identity tương ứng (imsi hoặc imei) = giá trị MỚI nghi ngờ.
        Returns: list swap_event đã được HLR/HSS xác nhận thật.
        """
        if not conflict_rows:
            return []

        by_msisdn: Dict[str, List[Dict]] = {}
        for row in conflict_rows:
            by_msisdn.setdefault(row["msisdn"], []).append(row)

        msisdns = list(by_msisdn.keys())
        history_by_msisdn: Dict[str, Optional[Dict]] = {}

        for i in range(0, len(msisdns), self.batch_size):
            chunk = msisdns[i:i + self.batch_size]
            try:
                resp = requests.post(
                    f"{self.hlr_url}/subscribers/batch-history",
                    json={"msisdns": chunk, "identity_type": self.identity_type},
                    timeout=self.timeout_seconds,
                )
                resp.raise_for_status()
                data = resp.json()
            except requests.RequestException:
                logger.warning(
                    "HLR/HSS batch-history không phản hồi cho %d msisdn (identity=%s) — "
                    "bỏ qua chunk.", len(chunk), self.identity_type,
                )
                continue
            except ValueError:
                logger.exception("HLR/HSS batch-history trả JSON không hợp lệ")
                continue

            for item in data.get("results", []):
                history_by_msisdn[item["msisdn"]] = item.get("history") if item.get("found") else None

        confirmed_events: List[Dict] = []
        id_key = self.identity_type
        old_key, new_key = f"old_{id_key}", f"new_{id_key}"
        source_tag = "RADIUS_CONFLICT_C" if id_key == "imsi" else "RADIUS_CONFLICT_D"

        for msisdn, rows in by_msisdn.items():
            history_resp = history_by_msisdn.get(msisdn)
            if not history_resp:
                continue
            entries = history_resp.get("history", [])
            if len(entries) < 2:
                continue  # chỉ 1 lần gán -> không phải swap

            new_value = entries[-1]["value"]       # ASCENDING, phần tử cuối = mới nhất
            old_value = entries[-2]["value"]
            confirmed_at = entries[-1]["assigned_at"]

            for row in rows:
                if row.get(id_key) != new_value:
                    continue
                confirmed_events.append({
                    "msisdn": msisdn,
                    old_key: old_value,
                    new_key: new_value,
                    "swap_type": self.swap_type,
                    "detected_at": str(row["event_timestamp"]),
                    "confirmed_at": str(confirmed_at),
                    "source": source_tag,
                })

        return confirmed_events


def write_swap_events(events: List[Dict], db_dsn: Dict) -> None:
    if not events:
        return

    sql = """
        INSERT INTO swap_event
            (msisdn, old_imsi, new_imsi, old_imei, new_imei, swap_type, detected_at, confirmed_at, source)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    data = [
        (
            e["msisdn"], e.get("old_imsi"), e.get("new_imsi"),
            e.get("old_imei"), e.get("new_imei"),
            e["swap_type"], e["detected_at"], e["confirmed_at"], e["source"],
        )
        for e in events
    ]

    conn = psycopg2.connect(**db_dsn)
    try:
        with conn.cursor() as cur:
            cur.executemany(sql, data)
        conn.commit()
        logger.info("Wrote %d rows to swap_event", len(data))
    except Exception:
        conn.rollback()
        logger.exception("Failed to write swap_event")
        raise
    finally:
        conn.close()