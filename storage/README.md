# storage/

Schema PostgreSQL và tài liệu thiết kế cho storage layer.

## Files

```
storage/
├── migrations/
│   ├── 001_init_schema.sql   # Tạo 5 bảng chính
│   ├── 002_indexes.sql       # 5 composite B-Tree index
│   └── 003_partitions.sql    # Monthly partitions cho radius_sessions
└── docs/
    └── schema_design.md      # Tài liệu: lý do chọn partitioning/indexing
```

## Chạy migration

```bash
# Tự động khi docker compose up (entrypoint script)
# Hoặc thủ công theo thứ tự:
psql -U camara -d camara_db -f storage/migrations/001_init_schema.sql
psql -U camara -d camara_db -f storage/migrations/002_indexes.sql
psql -U camara -d camara_db -f storage/migrations/003_partitions.sql

# Reset (dev only)
bash scripts/reset_db.sh
```

## Schema tóm tắt

### `radius_sessions` — bảng chính
Lưu toàn bộ RADIUS record đã qua pipeline. Partition RANGE by `event_timestamp` (monthly).

| Column | Type | Ghi chú |
|--------|------|---------|
| `id` | BIGSERIAL | PK |
| `acct_session_id` | UUID | Session identifier |
| `acct_status_type` | VARCHAR(16) | Start / Stop / Interim-Update |
| `event_timestamp` | TIMESTAMPTZ | Thời điểm sự kiện — partition key |
| `ingest_timestamp` | TIMESTAMPTZ | Thời điểm record vào pipeline |
| `msisdn` | VARCHAR(16) | E.164 |
| `imsi` | CHAR(15) | |
| `imei` | CHAR(15) | |
| `rat_type` | VARCHAR(8) | LTE / NR / WCDMA / GSM |
| `framed_ip` | INET | |
| `nas_ip` | INET | |
| `mcc_mnc` | CHAR(6) | |
| `late_arrival` | BOOLEAN | True nếu record đến muộn > threshold |

### `swap_event` — SIM Swap / Device Swap events
| Column | Type | Ghi chú |
|--------|------|---------|
| `id` | BIGSERIAL | PK |
| `msisdn` | VARCHAR(16) | |
| `old_imsi` | CHAR(15) | IMSI trước khi swap |
| `new_imsi` | CHAR(15) | IMSI sau khi swap |
| `old_imei` | CHAR(15) | IMEI trước (Device Swap) |
| `new_imei` | CHAR(15) | IMEI sau (Device Swap) |
| `swap_type` | VARCHAR(16) | `SIM_SWAP` hoặc `DEVICE_SWAP` |
| `detected_at` | TIMESTAMPTZ | Từ RADIUS event_timestamp |
| `confirmed_at` | TIMESTAMPTZ | Từ HLR/HSS mock imsi-history |
| `source` | VARCHAR(32) | `RADIUS_CONFLICT_C` |

### `duplicate_log`, `conflict_log`, `invalid_log`
Bảng audit — lưu records bị loại kèm lý do. Không partition.

## Indexes

| Index | Bảng | Columns | Dùng bởi |
|-------|------|---------|---------|
| `idx_msisdn_ts` | `radius_sessions` | `(msisdn, event_timestamp DESC)` | SIM Swap API, Number Verification |
| `idx_imsi_ts` | `radius_sessions` | `(imsi, event_timestamp DESC)` | Session lookup, Device Swap |
| `idx_imei_ts` | `radius_sessions` | `(imei, event_timestamp DESC)` | Device Swap IMEI history |
| `idx_swap_msisdn` | `swap_event` | `(msisdn, detected_at DESC)` | SIM Swap API — query chính |
| `idx_swap_imei` | `swap_event` | `(imei, detected_at DESC)` | Device Swap API — query chính |

Xem lý do chọn indexes và so sánh phương án thay thế tại [`docs/schema_design.md`](docs/schema_design.md).
