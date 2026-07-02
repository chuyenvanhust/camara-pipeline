#!/usr/bin/env python3
# pipeline/conflict_resolution/swap_detector.py
"""
Module phụ trách xử lý hậu kỳ riêng cho Conflict loại C (SIM Swap signal).
Chứa logic nghiệp vụ tương tác Network I/O: gọi HLR/HSS Mock để xác minh
lịch sử IMSI, và ghi kết quả vào bảng swap_event (PostgreSQL).

Được gọi từ pipeline/processing/processor.py::_process_sim_swap_signals(),
với danh sách record đã được _resolve_conflicts() xác định là conflict C.

Lịch sử sửa lỗi (xem docs/adr/ hoặc commit log):
  [FIX-1] old_imsi trước đây lấy từ hlr_data.get("old_imsi") — field này
          KHÔNG tồn tại trong response thực tế của HLR/HSS Mock API (xem
          mock_services/hlr_hss/README.md: response chỉ có `history` array,
          không có field `old_imsi` ở top-level). Sửa: tự suy old_imsi từ
          phần tử ngay trước trong history đã sort theo assigned_at.
  [FIX-2] verify_and_emit_swap() trước đây chỉ trả về dict, không có cơ
          chế ghi DB nào gọi nó từ processor.py. Thêm hàm write_swap_events()
          ở cuối file để ghi list kết quả vào bảng swap_event.
"""

import logging
from typing import Dict, List, Optional

import psycopg2
import requests

logger = logging.getLogger(__name__)


class SwapDetector:
    """
    Xác minh tín hiệu SIM Swap (conflict C) bằng cách đối chiếu với
    HLR/HSS Mock API — nguồn sự thật về lịch sử gán IMSI cho từng MSISDN.
    """

    def __init__(
        self,
        hlr_mock_url: str = "http://camara-mock-hlr-hss:8200",
        db_connection=None,
        timeout_seconds: float = 5.0,
    ):
        self.hlr_url = hlr_mock_url
        self.db = db_connection
        self.timeout_seconds = timeout_seconds

    def verify_and_emit_swap(self, conflict_c_row: Dict) -> Optional[Dict]:
        """
        Xử lý 1 record nghi ngờ SIM Swap (conflict C từ pipeline).

        Quy trình:
            1. Gọi HLR/HSS Mock: GET /subscribers/{msisdn}/imsi-history
            2. Sort history theo assigned_at, tìm vị trí của new_imsi
            3. old_imsi = phần tử NGAY TRƯỚC new_imsi trong history đã sort
               (không dùng field "old_imsi" không tồn tại trong response)
            4. Nếu new_imsi không có trong history -> false positive, return None

        Args:
            conflict_c_row: dict chứa msisdn, imsi (= new_imsi), event_timestamp.
                Có thể là dict thuần (từ pandas .to_dict()) hoặc pyspark.sql.Row
                (cả 2 đều hỗ trợ __getitem__ theo key).

        Returns:
            dict: payload chuẩn hóa khớp schema bảng swap_event nếu HLR xác
                nhận (msisdn, old_imsi, new_imsi, swap_type, detected_at,
                confirmed_at, source).
            None: nếu HLR/HSS không xác nhận, lỗi mạng, hoặc response không
                hợp lệ — coi là false positive, không ghi swap_event.
        """
        msisdn = conflict_c_row["msisdn"]
        new_imsi = conflict_c_row["imsi"]
        detected_at = conflict_c_row["event_timestamp"]

        try:
            response = requests.get(
                f"{self.hlr_url}/subscribers/{msisdn}/imsi-history",
                timeout=self.timeout_seconds,
            )
            if response.status_code != 200:
                logger.debug(
                    "HLR/HSS trả status %d cho msisdn=%s -> bỏ qua",
                    response.status_code, msisdn,
                )
                return None

            hlr_data = response.json()
            history = hlr_data.get("history", [])

            if not history:
                logger.debug("HLR/HSS không có history cho msisdn=%s", msisdn)
                return None

      
            sorted_history = sorted(history, key=lambda x: x["assigned_at"])

            matched_index = next(
                (i for i, item in enumerate(sorted_history) if item["imsi"] == new_imsi),
                None,
            )

            if matched_index is None:
           
                logger.debug(
                    "new_imsi=%s không tìm thấy trong HLR history của msisdn=%s",
                    new_imsi, msisdn,
                )
                return None

            confirmed_at = sorted_history[matched_index]["assigned_at"]

       
            if matched_index == 0:
                logger.debug(
                    "msisdn=%s: new_imsi=%s là lần gán đầu tiên, không phải SIM Swap",
                    msisdn, new_imsi,
                )
                return None

            old_imsi = sorted_history[matched_index - 1]["imsi"]

        except requests.RequestException:
            logger.warning(
                "HLR/HSS Mock không phản hồi cho msisdn=%s (timeout/connection error)",
                msisdn,
            )
            return None
        except (KeyError, ValueError, TypeError):
            logger.exception(
                "HLR/HSS Mock trả response không đúng format cho msisdn=%s", msisdn
            )
            return None

        swap_event = {
            "msisdn": msisdn,
            "old_imsi": old_imsi,
            "new_imsi": new_imsi,
            "swap_type": "SIM_SWAP",
            "detected_at": str(detected_at),
            "confirmed_at": str(confirmed_at),
            "source": "RADIUS_CONFLICT_C",
        }

        return swap_event


def write_swap_events(events: List[Dict], db_dsn: Dict) -> None:
    """
    

    Args:
        events: list dict, mỗi phần tử có shape của verify_and_emit_swap()
            trả về (không None — caller phải filter None trước khi gọi).
        db_dsn: dict connection params cho psycopg2.connect(**db_dsn).

    Raises:
        Exception: nếu INSERT thất bại — caller (processor.py) chịu trách
            nhiệm catch và log, không để crash toàn bộ batch.
    """
    if not events:
        return

    sql = """
        INSERT INTO swap_event
            (msisdn, old_imsi, new_imsi, swap_type, detected_at, confirmed_at, source)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    data = [
        (
            e["msisdn"],
            e["old_imsi"],
            e["new_imsi"],
            e["swap_type"],
            e["detected_at"],
            e["confirmed_at"],
            e["source"],
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