#!/usr/bin/env python3
# pipeline/pipeline/state/redis_state_manager.py
"""
Redis Global State Store — thay thế TOÀN BỘ logic pandas nội-bộ-batch
(.shift()/.groupby()/vòng lặp reset-mỗi-batch) của cả 4 loại Conflict
A/B/C/D bằng một kho trạng thái TOÀN CỤC, sống xuyên suốt mọi micro-batch
và sống sót qua restart job.

--------------------------------------------------------------------------
BỐI CẢNH (xem BÁO CÁO PHÂN TÍCH LỖI LOGIC HỆ THỐNG XỬ LÝ RADIUS, mục II/III)
--------------------------------------------------------------------------
Code cũ có 2 dạng lỗi giống hệt nhau cho cả 4 loại conflict:
  - C/D: dùng .shift(1) -> chỉ so sánh 2 bản ghi NẰM CHUNG 1 micro-batch.
  - A  : lấy "bản ghi đầu tiên trong batch" của session làm baseline —
         nếu Start thật sự đã xảy ra ở batch TRƯỚC, baseline giả này
         luôn trùng với chính nó -> không bao giờ phát hiện được conflict.
  - B  : tập open_sessions là biến cục bộ, bị XOÁ SẠCH mỗi lần hàm được
         gọi (mỗi batch) -> không thể nhớ một session đã Start ở batch
         trước còn đang mở hay chưa.
Cả 4 loại đều là hệ quả của cùng 1 sai lầm kiến trúc: coi Spark Structured
Streaming là dữ liệu tĩnh (Stateless) trong khi nó là luồng động cần
"trí nhớ" xuyên-batch (Stateful).

Giải pháp: 3 nhóm key Redis độc lập, mỗi nhóm ứng với 1 nhóm conflict:

    Conflict A  -> session_start:{session_id}  (STRING "imsi|msisdn",
                   set 1 lần duy nhất bằng SETNX — Start đầu tiên luôn
                   là "sự thật", không bị ghi đè bởi các event sau)
    Conflict B  -> open_sessions:{imsi}         (SET các session_id đang
                   mở — thêm khi Start, xoá khi Stop/Interim)
    Conflict C/D-> subscriber:{msisdn}           (HASH last_imsi/last_imei/...)

--------------------------------------------------------------------------
YÊU CẦU BẮT BUỘC: KHÔNG GỌI ĐƠN LẺ
--------------------------------------------------------------------------
Mọi thao tác đọc/ghi Redis trong module này PHẢI đi qua redis.pipeline()
và được chia theo LÔ (mặc định 300, kẹp cứng trong khoảng 200-500 record
mỗi lần round-trip mạng — theo đúng yêu cầu nghiệp vụ). Tuyệt đối không
gọi lệnh Redis rời rạc cho từng session_id/imsi/msisdn trong một vòng
lặp Python riêng biệt (N round-trip/batch) — đó chính là lỗi đã khiến
Conflict C verify cũ (gọi API tuần tự từng bản ghi) làm Spark báo
"batch is falling behind" (xem [FIX-3] trong
conflict_resolution/swap_detector.py).
"""

import logging
from typing import Dict, List, Optional

import redis

logger = logging.getLogger(__name__)

# Kẹp batch size trong khoảng nghiệp vụ yêu cầu: 200-500.
#   - Quá nhỏ (<200): số round-trip mạng/batch tăng, ăn vào throughput.
#   - Quá lớn (>500): một lệnh pipeline() giữ quá lâu, có thể tạo
#     head-of-line blocking cho các client Redis khác dùng chung instance.
_MIN_BATCH_SIZE = 200
_MAX_BATCH_SIZE = 500
_DEFAULT_BATCH_SIZE = 300


def _clamp_batch_size(value: int) -> int:
    clamped = max(_MIN_BATCH_SIZE, min(_MAX_BATCH_SIZE, value))
    if clamped != value:
        logger.warning(
            "REDIS_BATCH_SIZE=%d nằm ngoài khoảng cho phép [%d, %d] — "
            "đã kẹp về %d.",
            value, _MIN_BATCH_SIZE, _MAX_BATCH_SIZE, clamped,
        )
    return clamped


class RedisStateManager:
    """
    Schema lưu trữ trong Redis:

      Conflict C/D — Key: subscriber:{msisdn}
                     Value: Hash {last_imsi, last_imei, last_session_id,
                                   last_status, last_event_ts}
      Conflict A   — Key: session_start:{session_id}
                     Value: String "start_imsi|start_msisdn" (SETNX — chỉ
                            ghi 1 lần, Start đầu tiên là baseline vĩnh viễn)
      Conflict B   — Key: open_sessions:{imsi}
                     Value: Set {session_id đang mở, chưa Stop/Interim}

    fetch_batch()/update_batch()                 : Conflict C/D
    fetch_session_baselines()/save_session_baselines() : Conflict A
    fetch_open_sessions()/save_open_sessions()   : Conflict B

    Tất cả đều đọc/ghi theo LÔ 200-500 record/round-trip qua pipeline().
    """

    KEY_PREFIX = "subscriber:"
    SESSION_KEY_PREFIX = "session_start:"
    OPEN_SESSIONS_PREFIX = "open_sessions:"

    #: TTL mặc định cho session_start / open_sessions — tránh Redis phình
    #: to vô hạn nếu 1 session không bao giờ nhận được Stop (thiết bị mất
    #: sóng/crash...). 48h là đủ rộng so với thời lượng phiên RADIUS
    #: thông thường, chỉnh qua SESSION_STATE_TTL_SECONDS nếu cần.
    DEFAULT_SESSION_TTL_SECONDS = 172800

    def __init__(
        self,
        host: str = "camara-redis",
        port: int = 6379,
        db: int = 0,
        batch_size: int = _DEFAULT_BATCH_SIZE,
        session_ttl_seconds: int = DEFAULT_SESSION_TTL_SECONDS,
        socket_timeout: float = 5.0,
        socket_connect_timeout: float = 5.0,
    ):
        self.batch_size = _clamp_batch_size(batch_size)
        self.session_ttl_seconds = session_ttl_seconds
        # redis-py không mở connection thật ngay lúc này (lazy) — an toàn
        # để khởi tạo singleton ở module import time kể cả khi Redis
        # chưa sẵn sàng (giống cách SwapDetector không gọi HLR lúc init).
        self._client = redis.Redis(
            host=host,
            port=port,
            db=db,
            decode_responses=True,
            socket_timeout=socket_timeout,
            socket_connect_timeout=socket_connect_timeout,
        )

    @staticmethod
    def _key(msisdn: str) -> str:
        return f"{RedisStateManager.KEY_PREFIX}{msisdn}"

    @staticmethod
    def _session_key(session_id: str) -> str:
        return f"{RedisStateManager.SESSION_KEY_PREFIX}{session_id}"

    @staticmethod
    def _open_key(imsi: str) -> str:
        return f"{RedisStateManager.OPEN_SESSIONS_PREFIX}{imsi}"

    @staticmethod
    def _chunks(items: List, size: int):
        for i in range(0, len(items), size):
            yield items[i:i + size]

    # ----------------------------------------------------------------
    # FETCH — đọc theo lô (pipeline HGETALL), KHÔNG gọi đơn lẻ
    # ----------------------------------------------------------------
    def fetch_batch(self, msisdns: List[str]) -> Dict[str, Dict[str, str]]:
        """
        Trả về {msisdn: {last_imsi, last_imei, last_session_id,
        last_status, last_event_ts}} cho những msisdn ĐÃ TỪNG có state
        trong Redis. Msisdn lần đầu xuất hiện (chưa từng có state) sẽ
        KHÔNG có mặt trong dict trả về — coi như "chưa có lịch sử".

        Số round-trip mạng = ceil(len(unique_msisdns) / batch_size), mỗi
        round-trip gánh 200-500 lệnh HGETALL qua 1 pipeline duy nhất.
        """
        unique_msisdns = list(dict.fromkeys(m for m in msisdns if m))
        if not unique_msisdns:
            return {}

        result: Dict[str, Dict[str, str]] = {}
        for chunk in self._chunks(unique_msisdns, self.batch_size):
            try:
                pipe = self._client.pipeline(transaction=False)
                for msisdn in chunk:
                    pipe.hgetall(self._key(msisdn))
                values = pipe.execute()  # <-- 1 round-trip cho cả chunk (200-500 key)
            except redis.RedisError:
                logger.exception(
                    "Redis fetch_batch lỗi khi đọc chunk %d msisdn — coi "
                    "như 'chưa có state cũ' cho chunk này (an toàn: sẽ "
                    "chỉ bỏ sót candidate C/D mới, không văng exception "
                    "làm chết batch Spark).",
                    len(chunk),
                )
                continue

            for msisdn, value in zip(chunk, values):
                if value:
                    result[msisdn] = value

        return result

    # ----------------------------------------------------------------
    # UPDATE — ghi theo lô (pipeline HSET), KHÔNG gọi đơn lẻ
    # ----------------------------------------------------------------
    def update_batch(self, updates: List[Dict[str, Optional[str]]]) -> None:
        """
        updates: list các dict {msisdn, last_imsi, last_imei,
                 last_session_id, last_status, last_event_ts}.
        Mỗi msisdn CHỈ NÊN xuất hiện 1 lần — ứng với bản ghi MỚI NHẤT
        (theo event_timestamp) của msisdn đó trong batch hiện tại, để
        batch tiếp theo so sánh đúng "trạng thái gần nhất".
        """
        if not updates:
            return

        for chunk in self._chunks(updates, self.batch_size):
            try:
                pipe = self._client.pipeline(transaction=False)
                for u in chunk:
                    msisdn = u.get("msisdn")
                    if not msisdn:
                        continue
                    mapping = {
                        k: ("" if v is None else str(v))
                        for k, v in u.items()
                        if k != "msisdn"
                    }
                    if mapping:
                        pipe.hset(self._key(msisdn), mapping=mapping)
                pipe.execute()  # <-- 1 round-trip cho cả chunk (200-500 record)
            except redis.RedisError:
                logger.exception(
                    "Redis update_batch lỗi khi ghi chunk %d record — "
                    "state của chunk này KHÔNG được cập nhật. Batch kế "
                    "tiếp có thể so sánh dựa trên state cũ hơn (bỏ sót 1 "
                    "nhịp), nhưng KHÔNG mất dữ liệu radius.clean.",
                    len(chunk),
                )
                continue

    # ==================================================================
    # CONFLICT A — session_start:{session_id} = "start_imsi|start_msisdn"
    # ==================================================================

    def fetch_session_baselines(self, session_ids: List[str]) -> Dict[str, Dict[str, Optional[str]]]:
        """
        Trả về {session_id: {start_imsi, start_msisdn}} cho những session
        ĐÃ TỪNG có bản ghi Start được lưu baseline trong Redis. Session
        chưa từng thấy -> không có mặt trong dict trả về.
        """
        unique_ids = list(dict.fromkeys(s for s in session_ids if s))
        if not unique_ids:
            return {}

        result: Dict[str, Dict[str, Optional[str]]] = {}
        for chunk in self._chunks(unique_ids, self.batch_size):
            try:
                pipe = self._client.pipeline(transaction=False)
                for sid in chunk:
                    pipe.get(self._session_key(sid))
                values = pipe.execute()  # <-- 1 round-trip cho cả chunk
            except redis.RedisError:
                logger.exception(
                    "Redis fetch_session_baselines lỗi khi đọc chunk %d "
                    "session_id — coi như 'chưa có Start baseline' cho "
                    "chunk này (Conflict A của chunk này có thể bị bỏ "
                    "sót, không văng exception làm chết batch).",
                    len(chunk),
                )
                continue

            for sid, raw in zip(chunk, values):
                if raw:
                    imsi, _, msisdn = raw.partition("|")
                    result[sid] = {
                        "start_imsi": imsi or None,
                        "start_msisdn": msisdn or None,
                    }
        return result

    def save_session_baselines(self, baselines: List[Dict[str, Optional[str]]]) -> None:
        """
        baselines: list các dict {session_id, start_imsi, start_msisdn}.
        Dùng SET ... NX — CHỈ ghi nếu session_id CHƯA có baseline, để bản
        ghi Start THẬT SỰ ĐẦU TIÊN luôn là "sự thật" tham chiếu, không bị
        các event đến sau (kể cả event lỗi/trễ) ghi đè.
        """
        if not baselines:
            return

        for chunk in self._chunks(baselines, self.batch_size):
            try:
                pipe = self._client.pipeline(transaction=False)
                for b in chunk:
                    sid = b.get("session_id")
                    if not sid:
                        continue
                    value = f"{b.get('start_imsi') or ''}|{b.get('start_msisdn') or ''}"
                    pipe.set(self._session_key(sid), value, nx=True, ex=self.session_ttl_seconds)
                pipe.execute()  # <-- 1 round-trip cho cả chunk
            except redis.RedisError:
                logger.exception(
                    "Redis save_session_baselines lỗi khi ghi chunk %d "
                    "session_id — baseline của chunk này KHÔNG được lưu, "
                    "Conflict A cho các session đó có thể bị đánh giá "
                    "lại từ đầu ở batch sau (không mất dữ liệu radius).",
                    len(chunk),
                )
                continue

    # ==================================================================
    # CONFLICT B — open_sessions:{imsi} = SET các session_id đang mở
    # ==================================================================

    def fetch_open_sessions(self, imsis: List[str]) -> Dict[str, set]:
        """
        Trả về {imsi: {session_id đang mở, ...}} cho những IMSI đang có
        ít nhất 1 session mở (đã Start, chưa Stop/Interim) tính đến thời
        điểm gần nhất Redis được cập nhật.
        """
        unique_imsis = list(dict.fromkeys(i for i in imsis if i))
        if not unique_imsis:
            return {}

        result: Dict[str, set] = {}
        for chunk in self._chunks(unique_imsis, self.batch_size):
            try:
                pipe = self._client.pipeline(transaction=False)
                for imsi in chunk:
                    pipe.smembers(self._open_key(imsi))
                values = pipe.execute()  # <-- 1 round-trip cho cả chunk
            except redis.RedisError:
                logger.exception(
                    "Redis fetch_open_sessions lỗi khi đọc chunk %d imsi "
                    "— coi như 'không có session nào đang mở' cho chunk "
                    "này (Conflict B của chunk này có thể bị bỏ sót).",
                    len(chunk),
                )
                continue

            for imsi, members in zip(chunk, values):
                if members:
                    result[imsi] = set(members)
        return result

    def save_open_sessions(self, updates: Dict[str, set]) -> None:
        """
        updates: {imsi: {session_id đang mở, ...}} — TRẠNG THÁI ĐẦY ĐỦ
        mới nhất của tập session đang mở cho mỗi IMSI (không phải diff),
        được GHI ĐÈ toàn bộ (DELETE rồi SADD lại) để đảm bảo nhất quán
        với các session đã bị đóng (Stop/Interim) trong batch hiện tại.
        Key rỗng -> chỉ DELETE (dọn sạch, không tạo set rỗng).
        """
        if not updates:
            return

        items = list(updates.items())
        for chunk in self._chunks(items, self.batch_size):
            try:
                pipe = self._client.pipeline(transaction=False)
                for imsi, session_ids in chunk:
                    key = self._open_key(imsi)
                    pipe.delete(key)
                    if session_ids:
                        pipe.sadd(key, *session_ids)
                        pipe.expire(key, self.session_ttl_seconds)
                pipe.execute()  # <-- 1 round-trip cho cả chunk
            except redis.RedisError:
                logger.exception(
                    "Redis save_open_sessions lỗi khi ghi chunk %d imsi "
                    "— trạng thái open-session của chunk này KHÔNG được "
                    "cập nhật (batch sau có thể so sánh dựa trên state cũ "
                    "hơn, không mất dữ liệu radius.clean).",
                    len(chunk),
                )
                continue

    def ping(self) -> bool:
        """Health-check nhanh, dùng lúc khởi động job (không nằm trong
        đường xử lý hot-path của từng batch)."""
        try:
            return bool(self._client.ping())
        except redis.RedisError:
            return False
