# tests/integration

Integration test suite — 36 test case bao gồm happy path, edge case, và late arrival.

## Cấu trúc

```
tests/integration
├── conftest.py                      # pytest fixtures dùng chung
├── fixtures/
│   ├── seed_data.sql                # 500 subscribers, 30 ngày dữ liệu nền
│   └── edge_cases.sql               # Dữ liệu cho các edge case đặc biệt
├── api/
│   ├── test_sim_swap.py             # TC01–TC09
│   ├── test_device_swap.py          # TC10–TC16
│   ├── test_number_verification.py  # TC17–TC22
│   ├── test_auth.py                 # TC35
│   └── test_error_handling.py       # TC34, TC36
└── pipeline/
    ├── test_validation.py           # TC32–TC33
    ├── test_deduplication.py        # TC23–TC25
    ├── test_conflict_resolution.py  # TC26–TC28
    └── test_late_arrival.py         # TC29–TC31
```

## Thiết lập

```bash
# Khởi động stack test (PostgreSQL isolated, không ảnh hưởng dev DB)
docker compose -f docker-compose.test.yml up -d

# Cài dependencies
pip install -e ".[test]"

# Chạy tất cả
pytest tests/integration -v


## Chiến lược inject test data

Test data được **inject thẳng vào PostgreSQL** (không qua Kafka/Spark pipeline).
Lý do: test case chạy nhanh < 1s, kiểm soát chính xác state DB,
không phụ thuộc vào pipeline đang chạy.

`conftest.py` cung cấp:
- `db_client` — asyncpg connection đến test DB
- `api_client` — `httpx.AsyncClient` đến FastAPI test instance
- `seed_db` — fixture tự động chạy `seed_data.sql` trước mỗi test session
- `clean_db` — fixture xóa dữ liệu sau mỗi test function

## Danh sách test case

| TC# | File | Scenario | Marker |
|-----|------|----------|--------|
| TC01 | test_sim_swap.py | Swap 1 ngày trước → check=true | happy_path |
| TC02 | test_sim_swap.py | Swap 7 ngày, maxAge=30 → true | happy_path |
| TC03 | test_sim_swap.py | Swap 31 ngày, maxAge=30 → false | happy_path |
| TC04 | test_sim_swap.py | Chưa từng swap → false, retrieve=null | happy_path |
| TC05 | test_sim_swap.py | Swap < 1 phút trước → true | edge_case |
| TC06 | test_sim_swap.py | Swap đúng tại boundary maxAge ngày → true | edge_case |
| TC07 | test_sim_swap.py | Swap tại maxAge+1 giây → false | edge_case |
| TC08 | test_sim_swap.py | 200, swapped=false | edge_case |
| TC09 | test_sim_swap.py | maxAge=0 → false | edge_case |
| TC10 | test_device_swap.py | IMEI thay đổi 1 ngày trước → true | happy_path |
| TC11 | test_device_swap.py | IMEI không thay đổi → false | happy_path |
| TC12 | test_device_swap.py | Swap 35 ngày, maxAge=30 → false | happy_path |
| TC13 | test_device_swap.py | Swap < 1 phút → true | edge_case |
| TC14 | test_device_swap.py | 200, deviceSwapped=false | edge_case |
| TC15 | test_device_swap.py | Boundary maxAge chính xác | edge_case |
| TC16 | test_device_swap.py | phoneNumber format sai → 422 | edge_case |
| TC17 | test_number_verification.py | Session active trong 24h → verified=true | happy_path |
| TC18 | test_number_verification.py | Session đã Stop → false | happy_path |
| TC19 | test_number_verification.py | MSISDN không tồn tại → false (không 404) | happy_path |
| TC20 | test_number_verification.py | Session Start < 1 phút → true | edge_case |
| TC21 | test_number_verification.py | Nhiều session active chồng nhau → true | edge_case |
| TC22 | test_number_verification.py | MSISDN format không hợp lệ → 422 | edge_case |
| TC23 | test_deduplication.py | Exact duplicate → 1 giữ, 1 drop | pipeline |
| TC24 | test_deduplication.py | Cùng session_id nhưng timestamp khác 1ms → cả 2 giữ | pipeline |
| TC25 | test_deduplication.py | Duplicate đến sau late arrival window → vẫn detect | pipeline, late_arrival |
| TC26 | test_conflict_resolution.py | Stop có imsi khác Start → CONFLICT_A | pipeline |
| TC27 | test_conflict_resolution.py | 2 Start active cùng imsi → CONFLICT_B | pipeline |
| TC28 | test_conflict_resolution.py | MSISDN → IMSI mới → emit swap_event | pipeline |
| TC29 | test_late_arrival.py | Record đến sau 1h → late_arrival=true | late_arrival |
| TC30 | test_late_arrival.py | Record đến sau 6h → xử lý bình thường | late_arrival |
| TC31 | test_late_arrival.py | Record đến sau 25h → drop + log | late_arrival |
| TC32 | test_validation.py | IMEI fail Luhn → ERR_IMEI_LUHN_FAIL | pipeline |
| TC33 | test_validation.py | IMEI TAC không trong GSMA mock → ERR_IMEI_TAC_UNKNOWN | pipeline |
| TC34 | test_error_handling.py | Thiếu field bắt buộc → 422 | error_handling |
| TC35 | test_auth.py | API Key sai → 401 | error_handling |
| TC36 | test_error_handling.py | DB timeout → 503 | error_handling |
