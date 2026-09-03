# simulator/

Sinh dữ liệu RADIUS Accounting log giả lập đầu vào cho pipeline.
Output là file CSV tuân thủ RFC 2866 + 3GPP TS 29.061 VSA.

## Vai trò trong hệ thống

```
simulator/ -- sinh CSV --> data/radius_log.csv
       |                         |
       | --kafka (direct test)   +--> UDP sender --> radius-ingestion
       v                                           |
Kafka radius.accounting.raw <----------------------+
```

## Files

| File | Vai trò |
|------|---------|
| `simulator.py` | Entry point CLI — parse args, orchestrate toàn bộ quá trình sinh dữ liệu |
| `generators.py` | Sinh MSISDN / IMSI / IMEI / session hợp lệ; đọc TAC list từ GSMA TAC mock |
| `error_injectors.py` | Inject các loại lỗi theo tỷ lệ cấu hình |
| `config.py` | Dataclass `SimulatorConfig` chứa tất cả tham số |

## Cách chạy

```bash
# Mặc định: 2 triệu records, seed=42
python simulator/simulator.py

# Tùy chỉnh tỷ lệ lỗi
python simulator/simulator.py \
  --records 2000000 \
  --subscribers 100000 \
  --days 90 \
  --seed 42 \
  --duplicate-rate 0.03 \
  --late-arrival-rate 0.05 \
  --invalid-imei-rate 0.02 \
  --conflict-rate 0.01 \
  --missing-field-rate 0.005 \
  --output data/radius_log.csv

# Stream thẳng vào Kafka (bỏ qua bước ghi file)
python simulator/simulator.py --kafka --kafka-topic radius.accounting.raw
```

## Phụ thuộc external

Trước khi chạy simulator, **GSMA TAC mock phải đang chạy** tại `http://localhost:8100`.
`generators.py` gọi `GET /tac` để tải danh sách TAC hợp lệ vào bộ nhớ,
sau đó dùng để sinh IMEI đảm bảo có TAC thật.

```bash
# Khởi động mock services trước
docker compose -f mock_services/docker-compose.mock.yml up -d gsma-tac
# Sau đó mới chạy simulator
python simulator/simulator.py
```

## Các loại lỗi được inject

| Injector | Mô tả | Flag CLI |
|----------|-------|----------|
| `DuplicateInjector` | Copy record, giữ nguyên `(session_id, status_type, event_timestamp)` | `--duplicate-rate` |
| `LateArrivalInjector` | Đẩy `ingest_timestamp` lên > `LATE_ARRIVAL_THRESHOLD_SECONDS` | `--late-arrival-rate` |
| `InvalidImeiInjector` | Phá Luhn checksum **hoặc** dùng TAC không có trong GSMA TAC mock | `--invalid-imei-rate` |
| `ConflictInjector` | Sinh conflict loại A / B / C theo tỷ lệ 50/30/20 | `--conflict-rate` |
| `MissingFieldInjector` | Xóa ngẫu nhiên 1 trong 3 mandatory field | `--missing-field-rate` |

## Reproducibility

Seed cố định `--seed 42` đảm bảo cùng output mọi lần chạy.
Simulator, HLR/HSS mock, và GSMA TAC mock đều dùng cùng seed
để dữ liệu khớp nhau (IMEI sinh ra có TAC tồn tại trong mock,
MSISDN/IMSI sinh ra có trong HLR/HSS mock).

## Schema output CSV

```
acct_status_type, acct_session_id, acct_session_time,
event_timestamp, ingest_timestamp, msisdn, imsi, imei,
rat_type, framed_ip, nas_ip, mcc_mnc
```
