# Đề Nghị Sửa Chữa — CAMARA RADIUS Pipeline (Mục tiêu: Go-Live Phần Cứng Thực Tế)

**Phạm vi:** F-01 → F-20, đối chiếu trực tiếp với source code hiện tại (không suy diễn từ báo cáo cũ). Mỗi mục gồm: đánh giá mức độ đúng/tác động, vị trí chính xác trong codebase, code cũ, code đề nghị thay.

**Quy ước mức độ:**
- **P0 — Chặn go-live.** Gây mất/sai dữ liệu, hoặc hệ thống không hoạt động đúng thiết kế trên phần cứng thật.
- **P1 — Phải sửa trước khi chịu tải sản xuất.** Không sai ngay lập tức nhưng sẽ vỡ khi có tải, restart, hoặc chạy dài hạn.
- **P2 — Dọn dẹp/cứng hoá.** Không chặn go-live nhưng nên làm trong 30-60 ngày đầu.

---

# NHÓM P0 — BẮT BUỘC XONG TRƯỚC GO-LIVE

## F-01 — Kafka offset commit không gắn với kết quả xử lý batch

**Đánh giá:** Đúng, đã tự kiểm chứng trong quá trình vận hành thử (không phải suy diễn). Đây là lỗ hổng nghiêm trọng nhất: dữ liệu RADIUS thật từ GGSN không thể replay lại như file CSV test.

**Vị trí:** `pipeline/modules/shared/base_consumer.py`, dòng 56-69 (config consumer) và dòng 133-150 (vòng lặp xử lý batch).

**Code cũ:**
```python
self.consumer = AIOKafkaConsumer(
    self.topic,
    bootstrap_servers=self.bootstrap_servers,
    group_id=self.group_id,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    auto_commit_interval_ms=5000,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    ...
)
...
try:
    await self.process_batch(batch)
except Exception as exc:
    self.metrics.increment("errors", batch_size)
    logger.error(
        f"[{self.group_id}] Error processing batch of {batch_size}: {exc}",
        exc_info=True,
    )
    # <-- không re-raise, không retry, không dừng: vòng lặp tiếp tục
    # offset của batch này vẫn bị auto-commit ở lần commit 5s kế tiếp
```

**Vấn đề cụ thể:** `enable_auto_commit=True` khiến Kafka commit theo *vị trí đã đọc* (`getmany()`), không phụ thuộc việc `process_batch()` có ghi DB thành công hay không. Batch lỗi bị log rồi bỏ qua vĩnh viễn — không có cơ chế nào phát hiện hay khôi phục.

**Code đề nghị:**
```python
self.consumer = AIOKafkaConsumer(
    self.topic,
    bootstrap_servers=self.bootstrap_servers,
    group_id=self.group_id,
    auto_offset_reset="earliest",
    enable_auto_commit=False,          # tự quản lý commit, chỉ commit sau khi xử lý xong
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    ...
)
```

```python
# base_consumer.py — vòng lặp run(), thay khối try/except cũ
MAX_BATCH_RETRIES = int(os.getenv("MAX_BATCH_RETRIES", "3"))

while self.running:
    data = await self.consumer.getmany(
        timeout_ms=BATCH_TIMEOUT_MS, max_records=BATCH_MAX_RECORDS,
    )
    if not data:
        continue

    for tp, tp_messages in data.items():
        if not tp_messages:
            continue
        batch = [m.value for m in tp_messages]
        batch_size = len(batch)
        self.metrics.increment("processed", batch_size)

        attempt = 0
        while True:
            try:
                await self.process_batch(batch)
                break
            except Exception as exc:
                attempt += 1
                self.metrics.increment("errors", batch_size)
                logger.error(
                    f"[{self.group_id}] Batch lỗi trên {tp} (lần {attempt}): {exc}",
                    exc_info=True,
                )
                if attempt >= MAX_BATCH_RETRIES:
                    await self._send_to_dlq(tp, tp_messages, exc)
                    break
                await asyncio.sleep(min(2 ** attempt, 10))

        # Chỉ commit SAU KHI batch của partition này đã xử lý xong (hoặc đã vào DLQ)
        last_offset = tp_messages[-1].offset
        await self.consumer.commit({tp: last_offset + 1})
```

`_send_to_dlq` (thêm mới trong `base_consumer.py`, dùng cho mọi consumer con):
```python
async def _send_to_dlq(self, tp, tp_messages, exc: Exception):
    """Ghi batch lỗi vượt quá retry vào topic DLQ, kèm raw payload + lỗi, để không mất dữ liệu."""
    if not hasattr(self, "_dlq_producer") or self._dlq_producer is None:
        from aiokafka import AIOKafkaProducer
        self._dlq_producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        await self._dlq_producer.start()

    dlq_topic = f"{self.topic}.dlq"
    for m in tp_messages:
        await self._dlq_producer.send(
            dlq_topic,
            value={
                "original_topic": m.topic,
                "partition": m.partition,
                "offset": m.offset,
                "raw_value": m.value,
                "error": str(exc),
                "consumer_group": self.group_id,
            },
        )
    await self._dlq_producer.flush()
    logger.error(f"[{self.group_id}] Đã đẩy {len(tp_messages)} message vào {dlq_topic} sau {MAX_BATCH_RETRIES} lần retry thất bại.")
```

**Ghi chú vận hành:** đổi sang manual commit làm tăng khả năng xử lý trùng khi restart giữa lúc DB đã ghi nhưng offset chưa commit (at-least-once). Cần kết hợp với F-02 (idempotency qua `event_id`) để đạt effectively-once — hai fix này phải triển khai cùng nhau, không tách rời.

---

## F-02 — Ghi DB/Redis/history/audit không atomic

**Đánh giá:** Đúng. Xác nhận trực tiếp trong `db.py`: mỗi thao tác batch tự mở connection riêng (`async with self.pool.acquire()`), không có transaction chung.

**Vị trí:** `pipeline/modules/shared/db.py`, dòng 176-259 (các hàm `batch_upsert_sim_state`, `batch_insert_sim_swap_history`, `batch_insert_audit_logs`); `pipeline/modules/sim_swap/consumer.py` dòng 172-182 (nơi gọi 4 lệnh này tuần tự, độc lập).

**Code cũ (db.py):**
```python
async def batch_upsert_sim_state(self, records: List[Tuple[str, str]]) -> None:
    if not records:
        return
    assert self.pool is not None
    async with self.pool.acquire() as conn:          # connection #1
        await conn.executemany(
            "INSERT INTO msisdn_sim (msisdn, imsi_current, updated_at) "
            "VALUES ($1, $2, NOW()) ON CONFLICT (msisdn) "
            "DO UPDATE SET imsi_current = EXCLUDED.imsi_current, updated_at = NOW()",
            records
        )

async def batch_insert_sim_swap_history(self, records: List[tuple]) -> None:
    if not records:
        return
    assert self.pool is not None
    async with self.pool.acquire() as conn:          # connection #2 — khác connection #1
        await conn.copy_records_to_table(
            "sim_swap_history", records=records,
            columns=["msisdn", "imsi_old", "imsi_new", "changed_at"],
        )

async def batch_insert_audit_logs(self, records) -> None:
    if not records:
        return
    assert self.pool is not None
    async with self.pool.acquire() as conn:          # connection #3
        await conn.executemany(
            "INSERT INTO audit_log (event_type, msisdn, details) VALUES ($1, $2, $3::jsonb)",
            records
        )
```

```python
# sim_swap/consumer.py — process_batch(), Step 5, gọi tuần tự không transaction
if all_upserts:
    await self.db.batch_upsert_sim_state(all_upserts)      # nếu crash sau dòng này...
if swap_records:
    await self.db.batch_insert_sim_swap_history(swap_records)   # ...history sẽ thiếu
if swap_audit:
    await self.db.batch_insert_audit_logs(swap_audit)
if cache_updates:
    await self.redis.mset(cache_updates)                    # cache có thể lệch DB nếu crash trước dòng này
```

**Code đề nghị (db.py — gộp 3 thao tác Postgres vào 1 transaction, 1 connection):**
```python
async def commit_sim_swap_batch(
    self,
    upserts: List[Tuple[str, str]],
    history: List[tuple],
    audit: List[Tuple[str, Optional[str], str]],
) -> None:
    """
    Ghi atomic: state (msisdn_sim) + history (sim_swap_history) + audit_log
    trong CÙNG một transaction. Nếu bất kỳ bước nào lỗi, toàn bộ rollback —
    không bao giờ để state mới tồn tại mà thiếu history tương ứng.
    """
    assert self.pool is not None
    async with self.pool.acquire() as conn:
        async with conn.transaction():
            if upserts:
                await conn.executemany(
                    "INSERT INTO msisdn_sim (msisdn, imsi_current, updated_at) "
                    "VALUES ($1, $2, NOW()) ON CONFLICT (msisdn) "
                    "DO UPDATE SET imsi_current = EXCLUDED.imsi_current, updated_at = NOW()",
                    upserts
                )
            if history:
                await conn.copy_records_to_table(
                    "sim_swap_history", records=history,
                    columns=["msisdn", "imsi_old", "imsi_new", "changed_at"],
                )
            if audit:
                await conn.executemany(
                    "INSERT INTO audit_log (event_type, msisdn, details) VALUES ($1, $2, $3::jsonb)",
                    audit
                )
```

```python
# sim_swap/consumer.py — process_batch(), Step 5 thay bằng:
if all_upserts or swap_records or swap_audit:
    await self.db.commit_sim_swap_batch(all_upserts, swap_records, swap_audit)

# Redis KHÔNG còn nằm trong transaction Postgres (không thể — khác engine).
# Coi Redis là projection có thể rebuild: chỉ update SAU KHI Postgres commit
# thành công, và version-guard để tránh event cũ ghi đè event mới (out-of-order).
if cache_updates:
    await self.redis.mset(cache_updates)
```

Áp dụng tương tự cho `device_swap/consumer.py` (hàm `commit_device_swap_batch` cùng cấu trúc).

**Điều kiện để coi là xong:** fault-injection (kill process ngay sau từng câu lệnh SQL trong transaction) không bao giờ để lại state mới mà thiếu history/audit tương ứng.

---

## F-03 — Notification chặn hot path và retry không hoạt động

**Đánh giá:** Đúng, và **nghiêm trọng hơn** mức "làm chậm" — retry path thực tế **không bao giờ chạy được**, đã kiểm chứng bằng cách đọc chéo 3 file.

**Vị trí:**
- `pipeline/modules/sim_swap/notifier.py` dòng 46-55, `device_swap/notifier.py` dòng 55-61 (nơi push vào queue sai)
- `pipeline/modules/shared/notification.py` dòng 50-94 (`NotificationRetryWorker` — worker duy nhất biết đọc queue)
- `pipeline/run_pipeline.py` — không có bất kỳ dòng nào import/khởi động `NotificationRetryWorker`

**Code cũ (sim_swap/notifier.py):**
```python
success = await send_callback(callback_url, payload, max_retries=3)
if success:
    logger.info(f"Successfully notified {callback_url}")
else:
    logger.warning(f"Failed to notify {callback_url}, adding to retry queue")
    queue_item = {
        "log_id": log_id, "callback_url": callback_url,
        "payload": payload, "attempt": 1,
    }
    await self.redis.rpush("retry:sim_swap", str(queue_item))   # (1) sai tên queue
                                                                   # (2) str(dict) không phải JSON
```

```python
# shared/notification.py — worker đọc queue KHÁC ("notification_retry_queue"),
# và parse bằng json.loads() — sẽ crash nếu nhận được str(dict) từ (1)/(2) ở trên
raw_item = await self.redis.blpop(self.queue_name, timeout=5)   # queue_name mặc định khác "retry:sim_swap"
_, data = raw_item
item = json.loads(data)   # KeyError/JSONDecodeError nếu data là repr Python, không phải JSON
```

```python
# run_pipeline.py — không có dòng nào sau đây tồn tại trong file thật:
# worker = NotificationRetryWorker(redis_client, queue_name=...)
# asyncio.create_task(worker.start())
```

**Vấn đề bổ sung — chặn hot path:** `notify_subscriptions()` được `await` trực tiếp trong vòng lặp xử lý batch của consumer (`sim_swap/consumer.py` dòng ~197-203), nghĩa là `send_callback()` (timeout 10s, tối đa 3 lần retry nội bộ = tối đa 30s/subscriber) block toàn bộ batch tiếp theo nếu endpoint callback của khách hàng chậm/down.

**Code đề nghị — outbox pattern, tách hẳn notification khỏi hot path:**

Bước 1 — Migration thêm cột `next_retry_at` idempotent-key (đã có sẵn `notification_log.next_retry_at`, chỉ cần dùng đúng):
```sql
-- storage/migrations/002_notification_outbox_index.sql (file mới)
CREATE INDEX IF NOT EXISTS idx_notification_claim
    ON notification_log (status, next_retry_at)
    WHERE status IN ('PENDING', 'FAILED');
```

Bước 2 — Consumer KHÔNG gọi HTTP nữa, chỉ ghi vào `notification_log` (đã có transaction từ F-02):
```python
# sim_swap/consumer.py — process_batch(), bỏ hẳn khối gọi self.notifier.notify_subscriptions()
# trong vòng lặp chính. Thay bằng: ghi PENDING trong CÙNG transaction ở commit_sim_swap_batch(),
# để dispatcher riêng xử lý sau — không có await HTTP nào trong consumer nữa.
```

Bước 3 — Dispatcher độc lập (file mới `pipeline/dispatcher/notification_dispatcher.py`), chạy như process/task riêng, claim bằng `FOR UPDATE SKIP LOCKED` để nhiều instance chạy song song an toàn:
```python
# pipeline/dispatcher/notification_dispatcher.py
import asyncio, logging
import httpx
from pipeline.modules.shared.db import DatabasePool

logger = logging.getLogger(__name__)

class NotificationDispatcher:
    def __init__(self, db: DatabasePool, batch_size: int = 50, poll_interval: float = 2.0):
        self.db = db
        self.batch_size = batch_size
        self.poll_interval = poll_interval
        self._client = httpx.AsyncClient(timeout=httpx.Timeout(connect=3.0, read=10.0))
        self.running = False

    async def _claim_batch(self):
        async with self.db.pool.acquire() as conn:
            async with conn.transaction():
                rows = await conn.fetch(
                    """
                    SELECT id, subscription_id, event_type, payload, attempts
                    FROM notification_log
                    WHERE status IN ('PENDING', 'FAILED')
                      AND (next_retry_at IS NULL OR next_retry_at <= NOW())
                    ORDER BY created_at
                    FOR UPDATE SKIP LOCKED
                    LIMIT $1
                    """,
                    self.batch_size,
                )
                if rows:
                    await conn.executemany(
                        "UPDATE notification_log SET status = 'IN_PROGRESS' WHERE id = $1",
                        [(r["id"],) for r in rows],
                    )
                return rows

    async def _dispatch_one(self, row, callback_url: str):
        try:
            resp = await self._client.post(callback_url, json=row["payload"])
            ok = resp.status_code in (200, 201, 202, 204)
        except Exception as exc:
            ok = False
            logger.warning(f"Callback lỗi cho log_id={row['id']}: {exc}")

        async with self.db.pool.acquire() as conn:
            if ok:
                await conn.execute(
                    "UPDATE notification_log SET status='SENT', attempts=attempts+1, "
                    "last_attempt_at=NOW() WHERE id=$1", row["id"],
                )
            else:
                attempts = row["attempts"] + 1
                if attempts >= 5:
                    await conn.execute(
                        "UPDATE notification_log SET status='DEAD', attempts=$2, "
                        "last_attempt_at=NOW() WHERE id=$1", row["id"], attempts,
                    )
                else:
                    backoff_s = min(2 ** attempts, 60)
                    await conn.execute(
                        "UPDATE notification_log SET status='FAILED', attempts=$2, "
                        "last_attempt_at=NOW(), next_retry_at=NOW() + ($3 || ' seconds')::interval "
                        "WHERE id=$1", row["id"], attempts, str(backoff_s),
                    )

    async def start(self):
        self.running = True
        while self.running:
            rows = await self._claim_batch()
            if not rows:
                await asyncio.sleep(self.poll_interval)
                continue
            # callback_url cần JOIN subscription — bổ sung trong query _claim_batch thực tế
            await asyncio.gather(*[
                self._dispatch_one(r, r["callback_url"]) for r in rows
            ])

    def stop(self):
        self.running = False
```

Bước 4 — Khởi động dispatcher như service riêng trong `docker-compose.yml` (không chạy chung process với 3 consumer, để callback chậm không ảnh hưởng throughput ingest):
```yaml
  notification-dispatcher:
    build:
      context: .
      dockerfile: pipeline/Dockerfile
    container_name: camara-notification-dispatcher
    depends_on:
      postgres:
        condition: service_healthy
    networks:
      - camara-network
    environment:
      - DB_HOST=camara-postgres
      - DB_PORT=5432
      - DB_USER=postgres
      - DB_PASSWORD=camara
      - DB_NAME=camara_db
    command: ["python", "-m", "pipeline.dispatcher.notification_dispatcher"]
```

Sau khi áp dụng, xoá bỏ hoàn toàn `retry:sim_swap`, `retry:device_swap`, và `NotificationRetryWorker` cũ (3 protocol không tương thích trước đây) — chỉ còn 1 outbox (`notification_log`) + 1 dispatcher.

---

## F-04 — Ingestion/Kafka không đủ durability cho phần cứng thật

**Đánh giá:** Đúng, xác nhận bằng chính log runtime: `"Producer started | batch=256KB linger=10ms compression=None acks=1"`.

**Vị trí:** `pipeline/ingestion/producer.py` dòng 56-63 (config), dòng 139-148 (gather không kiểm tra kết quả); `docker-compose.yml` dòng 31-61 (Kafka single-broker, RF ngầm định = 1 vì chỉ 1 broker).

**Code cũ (producer.py):**
```python
INGESTION_ACKS = os.getenv("INGESTION_ACKS", "1")          # mặc định acks=1, không phải "all"
...
INGESTION_COMPRESSION_TYPE = os.getenv("INGESTION_COMPRESSION_TYPE", "none")
...
self._producer = AIOKafkaProducer(
    ...
    acks=INGESTION_ACKS,
    retry_backoff_ms=500,
    # KHÔNG có enable_idempotence=True
)
```

```python
fut = await self._producer.send(topic=self.topic, key=partition_key, value=record)
pending.append(fut)
count += 1                                       # count tăng ngay khi enqueue, KHÔNG chờ ack

if count % FLUSH_EVERY_N_RECORDS == 0:
    await asyncio.gather(*pending, return_exceptions=True)   # exception bị nuốt vào list, không kiểm tra
    pending.clear()
```

**Code đề nghị:**
```python
# producer.py — production profile mặc định (vẫn cho phép override qua env cho môi trường dev/lab)
INGESTION_ACKS = os.getenv("INGESTION_ACKS", "all")             # đổi default: "1" -> "all"
INGESTION_COMPRESSION_TYPE = os.getenv("INGESTION_COMPRESSION_TYPE", "lz4")  # đổi default: none -> lz4
ENABLE_IDEMPOTENCE = os.getenv("INGESTION_ENABLE_IDEMPOTENCE", "true").lower() == "true"

self._producer = AIOKafkaProducer(
    ...
    acks=INGESTION_ACKS,
    enable_idempotence=ENABLE_IDEMPOTENCE,   # bắt buộc bật khi acks=all để tránh duplicate do retry
    retry_backoff_ms=500,
)
```

```python
# producer.py — publish_csv(), kiểm tra thật kết quả gather thay vì nuốt exception
if count % FLUSH_EVERY_N_RECORDS == 0:
    results = await asyncio.gather(*pending, return_exceptions=True)
    failed = [r for r in results if isinstance(r, Exception)]
    if failed:
        self.metrics_failed = getattr(self, "metrics_failed", 0) + len(failed)
        logger.error(
            "[S1] %d/%d record trong batch gửi thất bại (ví dụ lỗi đầu: %s)",
            len(failed), len(pending), failed[0],
        )
    pending.clear()
    ...

# Cuối publish_csv(): raise nếu có record thất bại, KHÔNG báo "ingest completed" như thành công hoàn toàn
if getattr(self, "metrics_failed", 0) > 0:
    logger.error("[S1] Ingest hoàn tất NHƯNG có %d record gửi thất bại — cần kiểm tra trước khi coi là an toàn.", self.metrics_failed)
```

**Kafka broker — bắt buộc cho phần cứng thật (không phải config code, mà infra):**
```yaml
# docker-compose.yml (hoặc chuyển sang cụm K8s/VM riêng cho go-live) —
# 1 broker KHÔNG đáp ứng được acks=all có ý nghĩa (không có replica để chờ ack).
# Yêu cầu tối thiểu: 3 broker, ví dụ mở rộng thêm 2 service kafka-2, kafka-3
# theo cùng mẫu image confluentinc/cp-kafka:7.5.0, rồi:
kafka:
  environment:
    KAFKA_DEFAULT_REPLICATION_FACTOR: 3
    KAFKA_MIN_INSYNC_REPLICAS: 2
```
```python
# run_pipeline.py — ensure_kafka_topics(), đổi replication_factor mặc định
async def ensure_kafka_topics(
    bootstrap_servers: str,
    topics: List[str],
    num_partitions: int = 4,
    replication_factor: int = 3,   # đổi default: 1 -> 3 (yêu cầu cụm >=3 broker)
) -> None:
```

**Nếu go-live giai đoạn đầu chỉ có 1 node phần cứng** (chưa có cụm 3 broker): phải công bố rõ ràng RPO/RTO thực tế trong tài liệu vận hành (ví dụ "mất broker = mất toàn bộ dữ liệu chưa consume, RTO không xác định") — không được gọi đây là "high availability".

---

## F-05 — Baseline release không nhất quán (kiến trúc/schema/tài liệu/test lệch nhau)

**Đánh giá:** Đúng, xác nhận trực tiếp trong quá trình debug ở các phiên trước — nhiều lần `docker-compose.yml`, migration, và hành vi thực tế của container không khớp nhau (ví dụ: `command:` tự chạy pipeline gây double-ingest mà không ai chủ đích thiết kế vậy).

**Vị trí:** `git status` tại root repo; `storage/migrations/` (chỉ còn 1 file `001_init_schema.sql`, không có 002+); test tại `tests/integration/pipeline/`.

**Hành động đề nghị (không phải code diff — đây là quy trình):**

1. Chạy và ghi lại kết quả:
```bash
git status --short | tee /tmp/git_status_before_golive.txt
```
Với mọi file `deleted`/`untracked` liên quan đến kiến trúc cũ (Spark, mock service không còn dùng) hoặc module asyncio mới: quyết định dứt khoát **giữ (commit) hoặc xoá hẳn (git rm)**, không để ở trạng thái lửng.

2. Kiểm tra test hiện có có thực sự chạy qua Kafka hay tự insert kết quả mong đợi:
```bash
grep -rn "INSERT INTO\|execute(" tests/integration/pipeline/*.py | grep -v "assert\|SELECT"
```
Nếu test tự `INSERT` dữ liệu kỳ vọng trực tiếp vào DB (bypass Kafka/consumer thật) — test đó **không chứng minh được pipeline chạy đúng**, cần viết lại thành E2E test thật: publish vào Kafka → chờ consumer xử lý → assert trên DB.

3. Trước go-live, bắt buộc: `git clone` sạch vào máy khác + `docker compose up` + `bash scripts/reset.sh` + `bash scripts/run_pipeline.sh` phải chạy được từ đầu không cần sửa tay gì — đây là tiêu chí chấp nhận tối thiểu, không phải tuỳ chọn.

---

# NHÓM P1 — PHẢI SỬA TRƯỚC KHI CHỊU TẢI SẢN XUẤT

## F-06 — Lifecycle/signal handling phân tán, có thể treo shutdown

**Đánh giá:** Đúng. Mỗi consumer tự đăng ký signal handler riêng — handler đăng ký sau đè handler trước (`loop.add_signal_handler` chỉ giữ 1 handler/signal/process).

**Vị trí:** `pipeline/modules/shared/base_consumer.py` dòng 102-108 (trong `run()`); `pipeline/run_pipeline.py` dòng 105-108 (nơi tạo 3 task không giám sát).

**Code cũ:**
```python
# base_consumer.py — run(), MỖI consumer (3 instance) tự làm việc này
loop = asyncio.get_running_loop()
for sig in (signal.SIGINT, signal.SIGTERM):
    try:
        loop.add_signal_handler(sig, lambda: asyncio.create_task(self.stop()))
    except NotImplementedError:
        pass
```
```python
# run_pipeline.py — tạo 3 task, không kiểm tra task nào chết
c1_task = asyncio.create_task(ip_consumer.run())
c2_task = asyncio.create_task(device_consumer.run())
c3_task = asyncio.create_task(sim_consumer.run())
...
await asyncio.gather(c1_task, c2_task, c3_task, return_exceptions=True)  # chỉ ở cuối, che lỗi giữa chừng
```

**Code đề nghị — chỉ orchestrator bắt signal, có giám sát task chết giữa chừng:**
```python
# base_consumer.py — XOÁ khối add_signal_handler khỏi run(); consumer chỉ còn biết stop() khi được gọi từ ngoài
async def run(self):
    await self.initialize()
    self.running = True
    try:
        assert self.consumer is not None
        while self.running:
            ...  # giữ nguyên vòng lặp getmany/process_batch
    except asyncio.CancelledError:
        pass
    finally:
        await self.stop()
```

```python
# run_pipeline.py — orchestrator là nơi DUY NHẤT bắt signal + giám sát task
import signal

async def run_pipeline_async(input_file=None, duration=None):
    ...
    consumers = [ip_consumer, device_consumer, sim_consumer]
    tasks = {
        asyncio.create_task(c.run(), name=c.group_id): c
        for c in consumers
    }

    shutdown_event = asyncio.Event()

    def _handle_signal():
        _log(">>> Nhận tín hiệu dừng, bắt đầu graceful shutdown...")
        shutdown_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _handle_signal)

    async def supervise():
        """Fail-fast: nếu 1 task chết ngoài ý muốn, dừng toàn bộ ngay, không chờ hết duration."""
        done, pending = await asyncio.wait(
            [asyncio.create_task(shutdown_event.wait()), *tasks.keys()],
            return_when=asyncio.FIRST_COMPLETED,
        )
        for t in done:
            if t in tasks and t.exception() is not None:
                _log(f">>> Consumer '{t.get_name()}' chết ngoài ý muốn: {t.exception()}")
                shutdown_event.set()

    supervisor_task = asyncio.create_task(supervise())

    # ... heartbeat loop giữ nguyên, nhưng kiểm tra shutdown_event.is_set() thay vì chỉ duration ...

    for c in consumers:
        c.running = False
    results = await asyncio.gather(*tasks.keys(), return_exceptions=True)
    failures = [r for r in results if isinstance(r, Exception)]
    supervisor_task.cancel()

    if failures:
        _log(f">>> Pipeline dừng với {len(failures)} lỗi.")
        sys.exit(1)   # exit code phản ánh lỗi thật, không phải luôn 0
```

---

## F-07 — Redis state có thể bị evict sai (allkeys-lru áp cho cả correctness state)

**Đánh giá:** Đúng. `maxmemory-policy allkeys-lru` áp dụng cho TOÀN BỘ key trong Redis instance, kể cả `sim:<msisdn>`/`device:<msisdn>` (state cần đúng, không phải cache thuần).

**Vị trí:** `docker-compose.yml` dòng 93-112 (Redis service); `pipeline/modules/ip_msisdn/redis_store.py` dòng 72-84 (`accounting_off` — SMEMBERS + N lệnh DEL không giới hạn).

**Code cũ (docker-compose.yml):**
```yaml
redis:
  command: ["redis-server", "--appendonly", "yes", "--maxmemory", "512mb", "--maxmemory-policy", "allkeys-lru"]
```

**Vấn đề:** nếu Redis đầy 512MB, key `sim:<msisdn>` (không có TTL, đúng ra phải tồn tại vĩnh viễn theo thiết kế — fallback về DB chỉ là optimization) có thể bị evict ngẫu nhiên theo LRU giống hệt key cache tạm thời khác. Vì code có fallback `if imsi_old is None: imsi_old = await self.db.get_current_imsi(msisdn)`, mất cache sim/device không gây sai (chỉ chậm hơn) — nhưng `ip-ggsn:<ip>` (IP mapping, không có fallback DB nào) **mất là mất thật**, không phục hồi được.

**Code đề nghị — tách Redis theo vai trò (2 instance hoặc 2 logical DB với policy khác nhau):**
```yaml
# docker-compose.yml — Redis correctness-state (sim/device cache + IP mapping): KHÔNG được evict
redis-state:
  image: redis:7-alpine
  container_name: camara-redis-state
  command: ["redis-server", "--appendonly", "yes", "--maxmemory", "512mb", "--maxmemory-policy", "noeviction"]
  # noeviction: khi đầy, ghi mới sẽ lỗi rõ ràng (OOM command not allowed) thay vì âm thầm mất dữ liệu cũ
  ...

# Redis cache thuần (nếu có nhu cầu cache riêng, tách khỏi state) vẫn có thể dùng allkeys-lru
```

```python
# ip_msisdn/redis_store.py — accounting_off(), giới hạn kích thước xử lý 1 lần
async def accounting_off(self, nas_identifier: str):
    if not nas_identifier:
        return
    ggsn_key = self._ggsn_key(nas_identifier)

    CHUNK_SIZE = 500
    cursor = 0
    total_deleted = 0
    while True:
        # dùng SSCAN thay vì SMEMBERS để không load toàn bộ set vào memory 1 lần
        # với NAS có hàng chục nghìn session đồng thời
        cursor, framed_ips = await self.redis.sscan(ggsn_key, cursor=cursor, count=CHUNK_SIZE)
        if framed_ips:
            async with self.redis.pipeline(transaction=False) as pipe:
                for ip in framed_ips:
                    pipe.delete(self._ip_key(ip))
                    pipe.srem(ggsn_key, ip)
                await pipe.execute()
            total_deleted += len(framed_ips)
        if cursor == 0:
            break
    await self.redis.delete(ggsn_key)
    logger.info(f"Accounting-Off: Cleared {total_deleted} IP mappings for NAS {nas_identifier}")
```

---

## F-08 — Observability là cấu hình rỗng (không có metric thật nào được export)

**Đánh giá:** Đúng, xác nhận trực tiếp: `api/main.py` không có route `/metrics`; `prometheus.yml` scrape 3 target (`spark-pipeline`, `postgres-exporter`, `gsma-tac-mock`, `hlr-hss-mock`, `itu-e164-mock`) hoàn toàn không tồn tại trong `docker-compose.yml` hiện tại.

**Vị trí:** `infra/prometheus/prometheus.yml` (toàn bộ file); `api/main.py` (thiếu route); `pipeline/modules/shared/metrics.py` (chỉ giữ dict trong process, không export).

**Code cũ (metrics.py):**
```python
class ModuleMetrics:
    def __init__(self, name: str):
        self.counters: Dict[str, int] = {...}
    def increment(self, metric: str, amount: int = 1):
        if metric in self.counters:
            self.counters[metric] += amount
    def log_summary(self):
        logger.info(f"[{self.name} Metrics Summary] {self.counters}")
        # chỉ log 1 lần khi consumer STOP — không ai nhìn thấy khi đang chạy production
```

**Code đề nghị:**
```python
# pipeline/modules/shared/metrics.py — thêm export Prometheus song song với counter cũ
from prometheus_client import Counter, Histogram, start_http_server

BATCH_PROCESSED = Counter("pipeline_batch_processed_total", "Tổng batch đã xử lý", ["group_id"])
EVENTS_DETECTED = Counter("pipeline_events_detected_total", "Tổng swap event phát hiện", ["group_id"])
BATCH_ERRORS = Counter("pipeline_batch_errors_total", "Tổng batch lỗi", ["group_id"])
BATCH_LATENCY = Histogram("pipeline_batch_latency_seconds", "Thời gian xử lý 1 batch", ["group_id"])
DB_POOL_WAIT = Histogram("pipeline_db_pool_wait_seconds", "Thời gian chờ connection pool", ["group_id"])

class ModuleMetrics:
    def __init__(self, name: str):
        self.name = name
        self.counters: Dict[str, int] = {...}   # giữ nguyên cho log_summary cũ

    def increment(self, metric: str, amount: int = 1):
        if metric in self.counters:
            self.counters[metric] += amount
        if metric == "processed":
            BATCH_PROCESSED.labels(group_id=self.name).inc(amount)
        elif metric == "events_detected":
            EVENTS_DETECTED.labels(group_id=self.name).inc(amount)
        elif metric == "errors":
            BATCH_ERRORS.labels(group_id=self.name).inc(amount)
```
```python
# run_pipeline.py — mở cổng /metrics cho chính pipeline (hiện KHÔNG có scrape target nào cho nó)
from prometheus_client import start_http_server

async def run_pipeline_async(...):
    start_http_server(9200)   # pipeline tự expose metrics tại :9200/metrics
    ...
```
```python
# api/main.py — thêm route /metrics thật
from prometheus_client import make_asgi_app
metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)
```
```yaml
# infra/prometheus/prometheus.yml — XOÁ các target không tồn tại, THÊM target pipeline thật
scrape_configs:
  - job_name: 'fastapi-app'
    metrics_path: '/metrics'
    static_configs:
      - targets: ['fastapi:8000']

  - job_name: 'pipeline'
    metrics_path: '/metrics'
    static_configs:
      - targets: ['pipeline:9200']

  # XOÁ: spark-pipeline, postgres-db (postgres-exporter), gsma-tac-mock, hlr-hss-mock, itu-e164-mock
  # — không container nào trong docker-compose.yml hiện tại phục vụ các target này.
  # Nếu cần Kafka lag / Postgres metrics: thêm đúng exporter thật (kafka-exporter, postgres_exporter)
  # như service riêng trong compose trước khi thêm lại vào đây.
```
Cần thêm `prometheus-client` vào `requirements.txt` (pipeline + api).

---

## F-09 — Connection pool sizing không theo workload thật

**Đánh giá:** Đúng về hướng, nhưng mức độ rủi ro phụ thuộc số instance triển khai thật (1 process = 3 consumer dùng chung 1 event loop hiện tại, KHÔNG phải 3 process riêng — cần làm rõ trước khi kết luận "OOM").

**Vị trí:** `pipeline/modules/shared/db.py` dòng 27-31 (`min_size=5, max_size=20`); mỗi 3 consumer tự khởi tạo `DatabasePool()` riêng trong `BaseKafkaConsumer.__init__` → 3 pool độc lập trong CÙNG 1 process.

**Code cũ (base_consumer.py, dòng 39):**
```python
def __init__(self, topic, group_id, bootstrap_servers=...):
    ...
    self.db = DatabasePool()   # mỗi consumer (ip/device/sim) tự tạo pool riêng — 3 pool x (5-20) = 15-60 connection cho 1 process
```

**Code đề nghị — 1 pool dùng chung cho cả 3 consumer trong process:**
```python
# run_pipeline.py — tạo pool 1 lần, truyền vào cả 3 consumer thay vì để mỗi consumer tự connect()
shared_db = DatabasePool()
await shared_db.connect()

ip_consumer = IPMsisdnConsumer(topic=raw_topic, group_id="cg-ip-msisdn", db=shared_db)
device_consumer = DeviceSwapConsumer(topic=raw_topic, group_id="cg-device-swap", db=shared_db)
sim_consumer = SimSwapConsumer(topic=raw_topic, group_id="cg-sim-swap", db=shared_db)
```
```python
# base_consumer.py — cho phép inject pool có sẵn thay vì luôn tự tạo mới
def __init__(self, topic, group_id, bootstrap_servers=..., db: Optional[DatabasePool] = None):
    ...
    self.db = db or DatabasePool()
    self._owns_db = db is None   # chỉ tự close() nếu tự tạo, tránh đóng pool được share

async def initialize(self):
    if self._owns_db:
        await self.db.connect()
    ...

async def stop(self):
    ...
    if self._owns_db:
        await self.db.close()
```
```python
# db.py — giảm pool size vì giờ chỉ còn 1 pool cho cả process (không phải x3)
self.pool = await asyncpg.create_pool(
    dsn=self.dsn,
    min_size=int(os.getenv("DB_POOL_MIN", "4")),
    max_size=int(os.getenv("DB_POOL_MAX", "12")),
    command_timeout=30,
    timeout=10,   # acquire timeout — fail rõ ràng thay vì treo vô hạn khi pool cạn
)
```
```yaml
# docker-compose.yml — postgres, giảm shared_buffers phù hợp mem_limit 768m
# (hiện tại shared_buffers=512MB chiếm 2/3 mem_limit, không còn chỗ cho work_mem*max_connections lúc peak)
postgres:
  command: [
    "postgres",
    "-c", "synchronous_commit=off",
    "-c", "max_connections=100",       # giảm từ 200 — với 1 shared pool (12) + API (10), 100 đã dư headroom
    "-c", "shared_buffers=256MB",      # giảm từ 512MB
    "-c", "work_mem=8MB",
    "-c", "max_wal_size=2GB",
    "-c", "wal_buffers=16MB",
    "-c", "checkpoint_timeout=15min"
  ]
```

---

## F-10 — Batch write cần benchmark, không kết luận bằng phép nhân RTT lý thuyết

**Đánh giá:** Đúng cách tiếp cận — đây là khuyến nghị **đo trước khi tối ưu**, không phải một lỗi cần code diff ngay. `executemany` của asyncpg (bản hiện dùng, xem `requirements.txt`) đã atomic và có thể pipeline nội bộ, không chắc là N round-trip riêng lẻ như giả định trong báo cáo cũ.

**Vị trí:** `pipeline/modules/shared/db.py` dòng 182-190 (`batch_upsert_sim_state`, dùng `executemany`), dòng 225-233 (tương tự cho audit).

**Hành động đề nghị (script benchmark, không phải sửa code chính trước khi có số liệu):**
```python
# scripts/bench_batch_write.py (file mới — chỉ chạy để lấy số liệu, không phải code production)
import asyncio, time
from pipeline.modules.shared.db import DatabasePool

async def bench(n: int):
    db = DatabasePool()
    await db.connect()
    records = [(f"84900{i:06d}", f"IMSI{i:09d}") for i in range(n)]

    t0 = time.perf_counter()
    await db.batch_upsert_sim_state(records)
    t_executemany = time.perf_counter() - t0

    # So sánh với INSERT ... SELECT FROM UNNEST
    t0 = time.perf_counter()
    async with db.pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO msisdn_sim (msisdn, imsi_current, updated_at)
            SELECT * FROM unnest($1::text[], $2::text[]), NOW()
            ON CONFLICT (msisdn) DO UPDATE SET imsi_current = EXCLUDED.imsi_current, updated_at = NOW()
            """,
            [r[0] for r in records], [r[1] for r in records],
        )
    t_unnest = time.perf_counter() - t0

    print(f"n={n}: executemany={t_executemany*1000:.1f}ms  unnest={t_unnest*1000:.1f}ms")
    await db.close()

if __name__ == "__main__":
    for n in (50, 100, 500, 2000):
        asyncio.run(bench(n))
```
Chạy trên phần cứng thật (không phải laptop dev) trước khi quyết định có cần đổi từ `executemany` sang `UNNEST` hay không. Chỉ đổi code nếu số liệu cho thấy `executemany` vượt ngân sách latency batch (mục tiêu: p95 batch xử lý < `BATCH_TIMEOUT_MS` hiện tại = 100ms cộng dồn cả 3 bước DB).

---

## F-11 — History/audit không có lifecycle (partition/retention)

**Đánh giá:** Đúng về nguyên tắc, nhưng mốc "cần partition ngay" phụ thuộc volume thật của phần cứng production — chưa có số liệu forecast cụ thể trong repo hiện tại.

**Vị trí:** `storage/migrations/001_init_schema.sql` dòng 27-44 (`device_swap_history`, `sim_swap_history` — không partition), dòng 61-68 (`audit_log` — chỉ có PK, không index theo `event_type`/`created_at`).

**Code cũ:**
```sql
CREATE TABLE IF NOT EXISTS audit_log (
    id         BIGSERIAL PRIMARY KEY,
    event_type VARCHAR(32) NOT NULL,
    msisdn     VARCHAR(16),
    details    JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
-- không có index nào ngoài PK — mọi query theo event_type/msisdn/created_at đều seq scan
```

**Code đề nghị (migration mới, không sửa `001_init_schema.sql` đã chạy — xem F-13):**
```sql
-- storage/migrations/003_audit_retention_index.sql
CREATE INDEX IF NOT EXISTS idx_audit_event_time ON audit_log (event_type, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_audit_msisdn_time ON audit_log (msisdn, created_at DESC) WHERE msisdn IS NOT NULL;

-- Retention job (chạy qua cron/pg_cron), xoá audit_log quá N ngày theo chính sách nghiệp vụ.
-- Đặt N cụ thể sau khi có yêu cầu compliance/pháp lý thật, KHÔNG hardcode tuỳ tiện ở đây.
```
```sql
-- Chỉ chuyển sang RANGE partition khi có số liệu forecast thật (ví dụ EXPLAIN ANALYZE
-- cho thấy seq scan trên audit_log > ngân sách, hoặc ước tính vượt 30-50M dòng).
-- Mẫu partition theo tháng (áp dụng SAU khi đã đo, không áp dụng mù trước go-live):
-- CREATE TABLE audit_log_2026_01 PARTITION OF audit_log FOR VALUES FROM ('2026-01-01') TO ('2026-02-01');
```

---

## F-12 — API chưa production-grade (serving, readiness, security)

**Đánh giá:** Đúng, xác nhận trực tiếp: `api_key` mặc định `"dev-secret"` không có guard, `/health` không probe DB thật, và `docker-compose.yml` publish cổng Postgres/Redis/Kafka thẳng ra host + Grafana `admin/admin` + anonymous access.

**Vị trí:** `api/config.py` (default secret); `api/main.py` (route `/health`); `api/dependencies/auth.py`; `docker-compose.yml` dòng 63-186 (`ports:` của postgres/redis/kafka/grafana).

**Code cũ (api/main.py):**
```python
@app.get("/health")
async def health():
    return {"status": "ok"}   # luôn "ok" — không phản ánh DB có sống hay không
```
```python
# api/config.py
api_key=os.getenv("API_KEY", "dev-secret"),   # fallback mặc định, không fail nếu quên set ở production
```
```yaml
# docker-compose.yml — publish thẳng ra host, ai cũng truy cập được nếu có network access
postgres:
  ports: ["5432:5432"]
redis:
  ports: ["6379:6379"]
kafka:
  ports: ["9092:9092", "29092:29092"]
grafana:
  environment:
    - GF_SECURITY_ADMIN_USER=admin
    - GF_SECURITY_ADMIN_PASSWORD=admin
    - GF_AUTH_ANONYMOUS_ENABLED=true
```

**Code đề nghị:**
```python
# api/main.py — /health tách readiness thật khỏi liveness
@app.get("/health/live")
async def liveness():
    """Liveness: process còn sống — dùng cho container restart policy."""
    return {"status": "ok"}

@app.get("/health/ready")
async def readiness():
    """Readiness: DB thực sự truy vấn được — dùng cho load balancer routing."""
    from api.dependencies.database import get_pool
    try:
        pool = get_pool()
        async with pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
        return {"status": "ready"}
    except Exception as exc:
        return JSONResponse(status_code=503, content={"status": "not_ready", "error": str(exc)})
```
```python
# api/config.py — fail startup rõ ràng nếu dùng default secret trong production
import sys

class Settings(BaseModel):
    api_key: str
    environment: str
    ...

settings = Settings(
    api_key=os.getenv("API_KEY", "dev-secret"),
    environment=os.getenv("ENVIRONMENT", "dev"),
    ...
)

if settings.environment == "production" and settings.api_key == "dev-secret":
    sys.exit("FATAL: API_KEY vẫn là giá trị mặc định 'dev-secret' trong môi trường production. "
             "Đặt biến môi trường API_KEY trước khi khởi động.")
```
```yaml
# docker-compose.yml — go-live profile: KHÔNG publish port DB/Redis/Kafka ra host
# (chỉ giữ trên camara-network nội bộ; nếu cần truy cập từ ngoài, dùng SSH tunnel/bastion, không mở port thẳng)
postgres:
  # XOÁ mục "ports:" — service khác trong cùng network vẫn kết nối qua tên container:5432
redis:
  # XOÁ mục "ports:"
kafka:
  # Giữ lại listener nội bộ; XOÁ hoặc giới hạn PLAINTEXT_HOST theo firewall nếu cần debug từ host

grafana:
  environment:
    - GF_SECURITY_ADMIN_USER=${GRAFANA_ADMIN_USER}       # bắt buộc set qua .env, không hardcode
    - GF_SECURITY_ADMIN_PASSWORD=${GRAFANA_ADMIN_PASSWORD}
    - GF_AUTH_ANONYMOUS_ENABLED=false
```
Đặt reverse proxy (nginx/Traefik) phía trước FastAPI làm TLS termination + rate limit — nằm ngoài phạm vi code Python hiện tại, cần thêm 1 service mới trong compose hoặc infra riêng.

---

## F-13 — Không có đường nâng cấp schema (migration runner thật)

**Đánh giá:** Đúng, xác nhận trực tiếp trong quá trình vận hành thử ở các phiên trước: sửa `001_init_schema.sql` không áp dụng lại được cho volume Postgres đã tồn tại dữ liệu (`CREATE TABLE IF NOT EXISTS` chỉ chạy lần đầu khi data directory rỗng).

**Vị trí:** `docker-compose.yml` dòng 84-86 (`volumes: ./storage/migrations:/docker-entrypoint-initdb.d` — cơ chế init script của Postgres image, không phải migration framework); `storage/migrations/` (chỉ có 1 file).

**Code cũ:**
```yaml
postgres:
  volumes:
    - postgres_data:/var/lib/postgresql/data
    - ./storage/migrations:/docker-entrypoint-initdb.d   # chỉ chạy khi data dir RỖNG, không phải mỗi lần start
```

**Code đề nghị — dùng migration runner thật (ví dụ Alembic, nhẹ và phổ biến với asyncpg/SQLAlchemy):**
```bash
pip install alembic
alembic init storage/alembic
```
```python
# storage/alembic/versions/0001_init_schema.py — chuyển nội dung 001_init_schema.sql thành migration đầu tiên
def upgrade():
    op.execute(open("storage/migrations/001_init_schema.sql").read())

def downgrade():
    op.execute("DROP TABLE IF EXISTS notification_log, audit_log, subscription, "
               "sim_swap_history, device_swap_history, msisdn_sim, msisdn_device CASCADE;")
```
```python
# storage/alembic/versions/0002_audit_retention_index.py — F-11 trở thành migration có thể apply lại
def upgrade():
    op.execute("CREATE INDEX IF NOT EXISTS idx_audit_event_time ON audit_log (event_type, created_at DESC);")

def downgrade():
    op.execute("DROP INDEX IF EXISTS idx_audit_event_time;")
```
```bash
# scripts/run_pipeline.sh — thay khối "for sql_file in $(ls storage/migrations/*.sql...)" bằng:
alembic -c storage/alembic.ini upgrade head
```
Từ giờ, mọi thay đổi schema là 1 migration mới (`alembic revision`), không sửa trực tiếp file cũ đã từng chạy trên bất kỳ môi trường nào.

---

## F-14 — Data contract/event-time không được bảo vệ (fail-open)

**Đánh giá:** Đúng, xác nhận trực tiếp trong code.

**Vị trí:** `pipeline/modules/sim_swap/consumer.py` dòng 28-35 (`_parse_event_time`), tương tự trong `device_swap/consumer.py`; `pipeline/ingestion/producer.py` dòng 137 (partition key).

**Code cũ:**
```python
@staticmethod
def _parse_event_time(message: Dict[str, Any]) -> datetime:
    event_time = message.get("timestamp") or message.get("event_timestamp")
    dt = datetime.now(timezone.utc)          # fallback mặc định NGAY TỪ ĐẦU
    try:
        if event_time and "T" in str(event_time):
            dt = datetime.fromisoformat(str(event_time).replace("Z", "+00:00"))
    except Exception:
        pass                                  # lỗi parse bị NUỐT HOÀN TOÀN, dt vẫn là "bây giờ"
    return dt
```
```python
# producer.py — partition_key rơi về "" nếu thiếu msisdn, Kafka sẽ round-robin ngẫu nhiên
# thay vì đảm bảo cùng 1 msisdn luôn vào cùng 1 partition (phá ordering theo thuê bao)
partition_key = record.get("msisdn", "")
```

**Code đề nghị:**
```python
@staticmethod
def _parse_event_time(message: Dict[str, Any]) -> Optional[datetime]:
    """Trả None nếu không parse được — KHÔNG fallback về 'now', để caller quyết định
    (đưa vào DLQ thay vì âm thầm tạo swap event với timestamp sai)."""
    event_time = message.get("timestamp") or message.get("event_timestamp")
    if not event_time:
        return None
    try:
        return datetime.fromisoformat(str(event_time).replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None
```
```python
# sim_swap/consumer.py — process_batch(), Step 4: kiểm tra event_time trước khi coi là swap hợp lệ
dt = self._parse_event_time(msg)
if dt is None:
    self.metrics.increment("errors")
    logger.warning(f"[{self.group_id}] Bỏ qua message có event_time không hợp lệ: msisdn={msisdn}")
    # đẩy vào DLQ giống cơ chế F-01, KHÔNG âm thầm dùng datetime.now()
    invalid_records.append(msg)
    continue
```
```python
# producer.py — publish_csv(), reject record thiếu msisdn thay vì gửi với key rỗng
for record in reader.read_records():
    partition_key = record.get("msisdn")
    if not partition_key:
        logger.warning("[S1] Bỏ qua record thiếu msisdn, không đảm bảo được partition ordering: %s", record)
        skipped_count += 1
        continue
    fut = await self._producer.send(topic=self.topic, key=partition_key, value=record)
    ...
```

---

# NHÓM P2 — KHÔNG CHẶN GO-LIVE, LÀM TRONG 30-60 NGÀY ĐẦU

## F-15 — Ba consumer group trên một topic

**Đánh giá:** Đây là pub-sub chủ đích (mỗi group nhận 1 bản copy độc lập qua network, Kafka không nhân 3 lần storage của topic) — **không phải lỗi**. Giữ nguyên kiến trúc hiện tại.

## F-16 — Cooperative rebalance

**Đánh giá:** Cần xác minh version `aiokafka` đang dùng có hỗ trợ `CooperativeStickyAssignor` trước khi áp dụng — không copy cấu hình từ Java Kafka client sang trực tiếp. Kiểm tra: `pip show aiokafka` rồi đọc changelog tương ứng trước khi đổi `partition_assignment_strategy` trong `base_consumer.py` dòng 56-69.

## F-17 — Metrics race condition

**Đánh giá:** Rủi ro thấp trong code hiện tại — `ModuleMetrics.increment()` chạy đồng bộ trong 1 event loop (không có thread), nên không có race thật. Ưu tiên F-08 (export metrics) trước, không cần thread-safety cho tới khi có thiết kế multi-thread thật.

## F-18 — `executemany` = N round-trip

**Đánh giá:** Đã xử lý ở F-10 — cần benchmark, không kết luận bằng lý thuyết.

## F-19 — Tách topic để giảm I/O

**Đánh giá:** Không cần thiết ở quy mô hiện tại. Mỗi consumer group đã scale độc lập trong giới hạn số partition chung (4). Chỉ tách topic khi có nhu cầu thật về retention/security/schema khác nhau giữa các luồng — không tách chỉ vì lý do "giảm I/O".

## F-20 — Working tree bẩn (git status)

**Đánh giá:** Đã gộp vào F-05 (governance/release risk) — xử lý cùng, không tách hành động riêng.

---

# Thứ tự triển khai đề nghị (theo phụ thuộc kỹ thuật, không phải theo số thứ tự)

1. **F-05** trước tiên — dọn baseline, vì mọi thay đổi sau đó cần chạy trên 1 commit sạch để so sánh được trước/sau.
2. **F-01 + F-02** cùng lúc — hai fix này phụ thuộc lẫn nhau (manual commit cần idempotency từ transaction, transaction cần biết offset để không double-write khi replay).
3. **F-04** — đổi producer + yêu cầu hạ tầng Kafka 3-broker, nên làm song song với bước 2 vì cùng động chạm tới đường ống ingest.
4. **F-03** — sau khi F-01/F-02 ổn định (outbox pattern phụ thuộc transaction đã atomic).
5. **F-14** — dễ làm, không phụ thuộc gì, có thể chèn vào bất kỳ lúc nào trong nhóm P0/P1.
6. **F-06, F-07, F-09, F-12, F-13** — độc lập với nhau, có thể chia song song cho nhiều người nếu có team.
7. **F-08** — làm cuối nhóm P1, vì cần hệ thống đã tương đối ổn định để metrics phản ánh đúng, không đo trên hệ thống còn đang sửa dở.
8. **F-10, F-11** — chỉ cần khi có số liệu tải thật từ go-live, không làm mù trước.
