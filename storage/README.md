# storage/

Schema PostgreSQL và migration cho storage layer.

## Files

```
storage/
├── migrate.py                 # Versioned migration runner (checksum + advisory lock)
├── migrations/
│   ├── 001_init_schema.sql    # State, history, subscription, audit, notification
│   ├── 002_notification_outbox_index.sql
│   ├── 003_audit_retention_index.sql
│   └── 004_go_live_hardening.sql  # event_id, replay-safe state, radius_session_state
└── README.md
```

## Chạy migration

```bash
# Tự động khi docker compose up (service migrate, chạy trước fastapi/pipeline)
docker compose up migrate

# Hoặc thủ công
python -m storage.migrate

# Reset dev (truncate — không xóa volume)
bash scripts/reset_db.sh
```

Migration được ghi vào bảng `schema_migrations` với checksum SHA-256. Volume Postgres đã tồn tại vẫn nhận migration mới; migration đã apply không được sửa nội dung.

## Schema tóm tắt

### `msisdn_device` / `msisdn_sim` — current state

| Column | Ghi chú |
|--------|---------|
| `msisdn` | PK, E.164 |
| `imei_current` / `imsi_current` | Giá trị hiện tại |
| `last_event_at`, `last_event_id` | Version theo event-time + Kafka offset |
| `last_source_partition`, `last_source_offset` | Replay-safe ordering |

### `device_swap_history` / `sim_swap_history`

| Column | Ghi chú |
|--------|---------|
| `event_id` | UNIQUE — `{topic}:{partition}:{offset}` |
| `source_topic`, `source_partition`, `source_offset` | Kafka provenance |
| `changed_at` | Thời điểm swap |

### `radius_session_state` — Number Verification

| Column | Ghi chú |
|--------|---------|
| `acct_session_id` | PK — `{nas}:{session_id}` |
| `active` | Session Start chưa Stop |
| `last_event_at` | Dùng cho cửa sổ 24h |

### `audit_log` / `notification_log`

- `audit_log`: `UNIQUE(event_id, event_type)` — idempotent replay
- `notification_log`: outbox pattern, `UNIQUE(event_id, subscription_id)`, `locked_at` cho claim recovery

### `subscription`

Open Gateway callback registration (`SIM_SWAP` / `DEVICE_SWAP`).

## PostgreSQL runtime

`docker-compose.yml` bật `synchronous_commit=on` cho dữ liệu nghiệp vụ — tránh mất dữ liệu sau khi Kafka offset đã commit.
