# docs/

Tài liệu kỹ thuật: OpenAPI spec cho 3 API endpoint và Architecture Decision Records.

## OpenAPI Specs (`openapi/`)

| File | API | Compliance |
|------|-----|-----------|
| `sim_swap.yaml` | SIM Swap v0 | CAMARA spec chính thức |
| `device_swap.yaml` | Device Swap v0 | Custom — xem ADR-005 |
| `number_verification.yaml` | Number Verification v0 | CAMARA spec chính thức |

Xem trực tiếp qua Swagger UI tại http://localhost:8000/docs (tự động sinh từ FastAPI).

## Architecture Decision Records (`adr/`)

Mỗi ADR ghi lại: bối cảnh → các phương án cân nhắc → quyết định → lý do → hệ quả.

| ADR | Tiêu đề | Quyết định |
|-----|---------|-----------|
| ADR-001 | Định dạng input | CSV UTF-8 thay vì binary RADIUS AVP |
| ADR-002 | Định nghĩa Conflict | 3 loại A/B/C, xử lý theo ưu tiên A→B→C |
| ADR-003 | Storage partitioning | RANGE by timestamp (monthly) vs HASH by IMSI |
| ADR-004 | Dedup state store | Spark RocksDB built-in vs Redis standalone |
| ADR-005 | Device Swap API design | Tự thiết kế theo SIM Swap pattern (CAMARA chưa có spec) |
