# tests/unit/pipeline/deduplication/test_dedup_job.py
import pandas as pd
from datetime import datetime
from pipeline_v1.deduplication.dedup_job import dedup_pandas_state_func


class FakeState:
    """Giả lập GroupState của Spark cho test pure function."""
    def __init__(self, existing_value=None):
        self.exists = existing_value is not None
        self._value = existing_value
        self.updated_value = None

    @property
    def get(self):
        return self._value

    def update(self, value):
        self.updated_value = value


def test_dedup_pandas_state_func_first_batch_no_prior_state():
    """
    4 record cùng (session_id, status_type), KHÔNG có state trước đó.

    Sau sort theo event_timestamp tăng dần: 08:30, 10:00, 10:00, 10:15

    - 08:30 "Expired historical record (>1h)"
        -> mốc gốc đầu tiên (last_seen=None) -> is_duplicate=False
        -> last_seen = 08:30
    - 10:00 "First Record"
        -> cách last_seen (08:30) = 5400s > 3600s -> KHÔNG duplicate
        -> is_duplicate=False, cập nhật last_seen = 10:00
    - 10:00 "Exact Duplicate"
        -> cách last_seen (10:00) = 0s <= 3600s -> is_duplicate=True
    - 10:15 "Near Duplicate within 1h"
        -> cách last_seen (10:00) = 900s <= 3600s -> is_duplicate=True
    """
    pdf = pd.DataFrame([
        {"acct_session_id": "SESS_001", "acct_status_type": "Start",
         "event_timestamp": datetime.fromisoformat("2026-06-14 10:00:00"),
         "payload_data": "First Record"},
        {"acct_session_id": "SESS_001", "acct_status_type": "Start",
         "event_timestamp": datetime.fromisoformat("2026-06-14 10:00:00"),
         "payload_data": "Exact Duplicate"},
        {"acct_session_id": "SESS_001", "acct_status_type": "Start",
         "event_timestamp": datetime.fromisoformat("2026-06-14 10:15:00"),
         "payload_data": "Near Duplicate within 1h"},
        {"acct_session_id": "SESS_001", "acct_status_type": "Start",
         "event_timestamp": datetime.fromisoformat("2026-06-14 08:30:00"),
         "payload_data": "Expired historical record (>1h)"},
    ])

    state = FakeState(existing_value=None)
    result = dedup_pandas_state_func(("SESS_001", "Start"), pdf, state)

    result_map = dict(zip(result["payload_data"], result["is_duplicate"]))

    assert result_map["Expired historical record (>1h)"] is False  # mốc gốc đầu tiên
    assert result_map["First Record"] is False                      # > 3600s từ mốc trước -> mốc mới
    assert result_map["Exact Duplicate"] is True                     # 0s từ mốc -> duplicate
    assert result_map["Near Duplicate within 1h"] is True            # 900s từ mốc -> duplicate

    first_record_ts = pdf.loc[pdf["payload_data"] == "First Record", "event_timestamp"].iloc[0]
    expected_last_seen_ms = int(first_record_ts.timestamp() * 1000)
    assert state.updated_value == (expected_last_seen_ms,)