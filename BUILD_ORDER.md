# Thá»© tá»± Code & Báº£n Ä‘á»“ Phá»¥ thuá»™c (Build Order & Dependency Map)
## CAMARA Network API Data Pipeline â€” 7 Phase Â· 20 Modules


> **TÀI LIỆU LỖI THỜI — Phase 4 đã thay đổi hoàn toàn**
>
> Kiến trúc Spark Streaming 5-stage (S1 Ingestion → S2 Validation → S3 Deduplication
> → S4 Conflict → S5 Storage) và toàn bộ Phase 2 (mock services) đã bị loại bỏ.
> Pipeline hiện tại dùng **3 Kafka consumer modules** chạy song song:
> cg-ip-msisdn, cg-device-swap, cg-sim-swap.
>
> - Kiến trúc mới: xem README.md section 0
> - Lý do bỏ Spark: xem docs/adr/0001-drop-spark-use-kafka-consumer.md
> - Tài liệu này được giữ lại để tham khảo lịch sử — Phase 1, 5, 6, 7 vẫn có giá trị,
>   chỉ Phase 2 và 4 không còn đúng.


TĂ i liá»‡u nĂ y hÆ°á»›ng dáº«n chi tiáº¿t thá»© tá»± phĂ¡t triá»ƒn (build order) vĂ  sÆ¡ Ä‘á»“ phá»¥ thuá»™c giá»¯a cĂ¡c module trong há»‡ thá»‘ng **CAMARA Network API Data Pipeline**. CĂ¡c module Ä‘Æ°á»£c thiáº¿t káº¿ vĂ  sáº¯p xáº¿p theo lá»™ trĂ¬nh tá»‘i Æ°u hĂ³a kháº£ nÄƒng test Ä‘á»™c láº­p, giáº£m thiá»ƒu phá»¥ thuá»™c chĂ©o vĂ  há»— trá»£ phĂ¡t triá»ƒn song song hiá»‡u quáº£.

---

## đŸ—ºï¸ SÆ¡ Ä‘á»“ phá»¥ thuá»™c tá»•ng quan (Dependency Graph)

```mermaid
graph TD
    %% Phase 1
    subgraph P1["Phase 1: Zero Dependency (Utility & Config)"]
        shared["mock_services/shared/"]
        config_models["config.py + schemas/common.py"]
        storage_migrations["storage/migrations/"]
    end

    %% Phase 2
    subgraph P2["Phase 2: Mock External Services"]
        gsma_tac["mock_services/gsma_tac/"]
        itu_e164["mock_services/itu_e164/"]
        hlr_hss["mock_services/hlr_hss/"]
    end
    shared --> gsma_tac
    shared --> itu_e164
    shared --> hlr_hss

    %% Phase 3
    subgraph P3["Phase 3: Data Simulator"]
        simulator["simulator/ (Data Generator)"]
    end
    gsma_tac --> simulator
    config_models --> simulator

    %% Phase 4
    subgraph P4["Phase 4: Pipeline Core"]
        s1_ingest["S1: Ingestion (csv -> Kafka)"]
        s2_val["S2: Validation (rules + Spark)"]
        s3_dedup["S3: Deduplication (RocksDB)"]
        s4_conflict["S4: Conflict Resolution"]
        s5_storage["S5: Storage (PostgreSQL)"]
    end
    simulator --> s1_ingest
    s1_ingest --> s2_val
    gsma_tac --> s2_val
    hlr_hss --> s2_val
    itu_e164 --> s2_val
    storage_migrations --> s2_val
    s2_val --> s3_dedup
    s3_dedup --> s4_conflict
    hlr_hss --> s4_conflict
    storage_migrations --> s4_conflict
    s4_conflict --> s5_storage
    storage_migrations --> s5_storage

    %% Phase 5
    subgraph P5["Phase 5: FastAPI API Layer"]
        api_deps["api/dependencies/"]
        api_routers["api/routers/"]
    end
    storage_migrations --> api_deps
    config_models --> api_deps
    s5_storage --> api_routers
    api_deps --> api_routers

    %% Phase 6
    subgraph P6["Phase 6: Reporting & Monitoring"]
        reporting["reporting/ (HTML Report)"]
        infra_monitoring["infra/ (Grafana & Prometheus)"]
    end
    s5_storage --> reporting
    s5_storage --> infra_monitoring
    api_routers --> infra_monitoring

    %% Phase 7
    subgraph P7["Phase 7: End-to-End Integration & Load Test"]
        e2e_tests["tests/pipeline/ (E2E pytest)"]
        load_test["scripts/run_load_test.sh (k6)"]
    end
    s5_storage --> e2e_tests
    gsma_tac --> e2e_tests
    hlr_hss --> e2e_tests
    itu_e164 --> e2e_tests
    api_routers --> load_test
    s5_storage --> load_test

    %% Styling
    classDef p1 fill:#E8F5E9,stroke:#1D6F42,stroke-width:2px;
    classDef p2 fill:#EBF3FB,stroke:#2E75B6,stroke-width:2px;
    classDef p3 fill:#FFF8E1,stroke:#E8A317,stroke-width:2px;
    classDef p4 fill:#F3E5F5,stroke:#9C27B0,stroke-width:2px;
    classDef p5 fill:#FCE4EC,stroke:#C00000,stroke-width:2px;
    classDef p6 fill:#F5F5F5,stroke:#9E9E9E,stroke-width:2px;
    classDef p7 fill:#EDE7F6,stroke:#673AB7,stroke-width:2px;

    class shared,config_models,storage_migrations p1;
    class gsma_tac,itu_e164,hlr_hss p2;
    class simulator p3;
    class s1_ingest,s2_val,s3_dedup,s4_conflict,s5_storage p4;
    class api_deps,api_routers p5;
    class reporting,infra_monitoring p6;
    class e2e_tests,load_test p7;
```

---

## â¡ Kháº£ nÄƒng lĂ m song song (Parallel Tracks)

> [!TIP]
> Äá»ƒ tÄƒng tá»‘c Ä‘á»™ phĂ¡t triá»ƒn dá»± Ă¡n, cĂ¡c pháº§n sau cĂ³ thá»ƒ triá»ƒn khai song song:
> - **CĂ¡c module trong Phase 1** cháº¡y song song Ä‘á»™c láº­p hoĂ n toĂ n.
> - **CĂ¡c mock services á»Ÿ Phase 2** phĂ¡t triá»ƒn song song sau khi hoĂ n thĂ nh `mock_services/shared/` cá»§a Phase 1.
> - **Storage models** (`pipeline/storage/models.py`) cĂ³ thá»ƒ viáº¿t song song vá»›i **SQL migrations** (`storage/migrations/`).
> - **FastAPI Schema & Dependency** (`api/schemas/` vĂ  `api/dependencies/`) cĂ³ thá»ƒ viáº¿t song song trong khi Ä‘ang code Phase 4.
> - **Háº¡ táº§ng giĂ¡m sĂ¡t** (`infra/` Grafana, Prometheus) cĂ³ thá»ƒ cáº¥u hĂ¬nh song song khi lĂ m Phase 5.

---

## đŸ“‹ Chi tiáº¿t tá»«ng giai Ä‘oáº¡n (Phase Details)

### Phase 1: Zero Dependency â€” Code & Test hoĂ n toĂ n Ä‘á»™c láº­p
*Giai Ä‘oáº¡n ná»n táº£ng, chá»©a cĂ¡c utility thuáº§n tĂºy vĂ  cáº¥u hĂ¬nh, khĂ´ng phá»¥ thuá»™c vĂ o database hay network.*

| Module | ÄÆ°á»ng dáº«n / TĂªn file | LĂ½ do cĂ³ thá»ƒ code trÆ°á»›c | CĂ¡ch test | Output cho module sau |
|---|---|---|---|---|
| **shared** | `mock_services/shared/`<br>â†³ `health.py`, `pagination.py`, `errors.py` | Tiá»‡n Ă­ch dĂ¹ng chung thuáº§n (pure utility) â€” khĂ´ng import gĂ¬ tá»« project. | `pytest unit`: test error format, test Page[T] generic, test fault-injection header parse. | Shared library sá»­ dá»¥ng bá»Ÿi 3 mock services á»Ÿ Phase 2. |
| **config_models** | `simulator/config.py` + `api/schemas/common.py`<br>â†³ `SimulatorConfig`, `PhoneNumber`, `ErrorResponse` | Cáº¥u hĂ¬nh dáº¡ng Dataclass vĂ  Pydantic model thuáº§n â€” zero external dependencies. | `pytest unit`: validate PhoneNumber chuáº©n E.164, validate SimulatorConfig bounds. | Type contracts dĂ¹ng xuyĂªn suá»‘t project. |
| **storage_migrations** | `storage/migrations/`<br>â†³ `001_init_schema.sql`, `002_indexes.sql`, `003_partitions.sql` | SQL DDL thuáº§n â€” viáº¿t vĂ  review offline, khĂ´ng cáº§n service nĂ o cháº¡y. | Cháº¡y trĂªn PostgreSQL Docker Ä‘Æ¡n láº», kiá»ƒm tra `EXPLAIN ANALYZE` cho 3 query pattern chĂ­nh. | Database Schema Ä‘Ă£ kiá»ƒm chá»©ng â€” táº¥t cáº£ module sau Ä‘á»u build trĂªn schema nĂ y. |

---

### Phase 2: Mock Services â€” Äá»™c láº­p vá»›i pipeline, test vá»›i dá»¯ liá»‡u tÄ©nh
*MĂ´ phá»ng cĂ¡c API bĂªn ngoĂ i cá»§a bĂªn thá»© ba, cháº¡y Ä‘á»™c láº­p vĂ  khĂ´ng phá»¥ thuá»™c vĂ o Kafka/Spark.*

| Module | ÄÆ°á»ng dáº«n / TĂªn file | Phá»¥ thuá»™c | LĂ½ do cĂ³ thá»ƒ code | CĂ¡ch test | Output cho module sau |
|---|---|---|---|---|---|
| **gsma_tac** | `mock_services/gsma_tac/`<br>â†³ `models.py`, `seed.py`, `router.py`, `app.py` | `shared` | Chá»‰ cáº§n shared utility + file CSV tÄ©nh. KhĂ´ng cáº§n database hay Kafka. | `pytest`: GET `/tac/{tac}` found/not-found, POST `/batch`, pagination, fault-injection. | HTTP API `:8100` â€” DĂ¹ng bá»Ÿi simulator (Phase 3) vĂ  pipeline/validation (Phase 4). |
| **itu_e164** | `mock_services/itu_e164/`<br>â†³ `models.py`, `seed.py`, `router.py`, `app.py` | `shared` | Chá»‰ cáº§n shared + 2 CSV tÄ©nh (country codes, operator prefixes). KhĂ´ng lÆ°u state. | `pytest`: POST `/validate` (valid/invalid/unknown CC, short number), POST `/validate/batch`. | HTTP API `:8300` â€” DĂ¹ng cho quy trĂ¬nh kiá»ƒm tra validation Rule R2 á»Ÿ Phase 4. |
| **hlr_hss** | `mock_services/hlr_hss/`<br>â†³ `models.py`, `seed.py`, `router.py`, `app.py` | `shared` | Chá»‰ cáº§n shared + `subscribers.csv`. Cáº§n dĂ¹ng `seed=42` Ä‘á»ƒ khá»›p bá»™ sinh dá»¯ liá»‡u. | `pytest`: by-imsi/by-msisdn found/404, imsi-history length, batch-lookup mixed. | HTTP API `:8200` â€” DĂ¹ng cho Rule R3 vĂ  luá»“ng conflict_resolution (Phase 4+). |

> [!WARNING]
> Ká»‹ch báº£n seed dá»¯ liá»‡u (`seed.py` cá»§a HLR/HSS) pháº£i Ä‘Æ°á»£c cháº¡y TRÆ¯á»C KHI khá»Ÿi Ä‘á»™ng bá»™ sinh dá»¯ liá»‡u cá»§a simulator Ä‘á»ƒ Ä‘áº£m báº£o chĂºng cĂ³ chung tá»‡p thuĂª bao (subscriber pool).

---

### Phase 3: Simulator â€” Bá»™ giáº£ láº­p sinh log
*Táº¡o láº­p log thĂ´ mĂ´ phá»ng cĂ¡c sá»± kiá»‡n máº¡ng GGSN RADIUS.*

| Module | ÄÆ°á»ng dáº«n / TĂªn file | Phá»¥ thuá»™c | LĂ½ do cĂ³ thá»ƒ code | CĂ¡ch test | Output cho module sau |
|---|---|---|---|---|---|
| **simulator** | `simulator/`<br>â†³ `generators.py`, `error_injectors.py`, `simulator.py` | `gsma_tac`, `config_models` | Cáº§n gá»i GET `/tac` cá»§a GSMA TAC Mock Ä‘á»ƒ táº£i danh sĂ¡ch mĂ£ TAC há»£p lá»‡ lĂºc khá»Ÿi Ä‘á»™ng. | **Integration test**: Cháº¡y vá»›i `--records 10000 --seed 42`, kiá»ƒm tra file CSV Ä‘áº§u ra cĂ³ Ä‘Ăºng schema, tá»· lá»‡ lá»—i náº±m trong khoáº£ng Â±1% so vá»›i config, má»i IMEI há»£p lá»‡ Ä‘á»u cĂ³ TAC trong mock. | `data/radius_log.csv` â€” File log thĂ´ lĂ m dá»¯ liá»‡u Ä‘áº§u vĂ o cho pipeline xá»­ lĂ½. |

> [!NOTE]
> Module `error_injectors.py` cĂ³ thá»ƒ Ä‘Æ°á»£c code Ä‘á»™c láº­p trÆ°á»›c `generators.py` vĂ¬ nĂ³ chá»‰ biáº¿n Ä‘á»•i báº£n ghi (record) Ä‘Ă£ Ä‘Æ°á»£c táº¡o sáºµn Ä‘á»ƒ nhá»“i lá»—i.

---

### Phase 4: Pipeline Core â€” Luá»“ng xá»­ lĂ½ dá»¯ liá»‡u Spark Streaming
*XĂ¢y dá»±ng vĂ  kiá»ƒm thá»­ tuáº§n tá»± tá»«ng stage cá»§a luá»“ng xá»­ lĂ½ chĂ­nh.*

| Module | ÄÆ°á»ng dáº«n / TĂªn file | Phá»¥ thuá»™c | LĂ½ do cĂ³ thá»ƒ code | CĂ¡ch test | Output cho module sau |
|---|---|---|---|---|---|
| **s1_ingestion** | `pipeline/ingestion/`<br>â†³ `csv_reader.py`, `producer.py` | `simulator` | Chá»‰ cáº§n Kafka Ä‘ang cháº¡y + file CSV tá»« simulator. KhĂ´ng cáº§n DB hay mock API. | **Integration**: Äáº©y 1,000 records â†’ consume tá»« topic `radius.raw` â†’ kiá»ƒm tra sá»‘ lÆ°á»£ng vĂ  phĂ¢n vĂ¹ng (partition key). | Topic Kafka `radius.raw` Ä‘Æ°á»£c Ä‘áº©y dá»¯ liá»‡u thĂ´ liĂªn tá»¥c. |
| **s2_validation** | `pipeline/validation/`<br>â†³ `rules.py`, `validator.py` | `s1_ingestion`, `gsma_tac`, `hlr_hss`, `itu_e164`, `storage_migrations` | Chá»©a logic kiá»ƒm thá»­ cĂ¡c luáº­t nghiá»‡p vá»¥ (Rules). `rules.py` gá»i 3 mock services, `validator.py` cáº§n Kafka + Spark. | **Unit test `rules.py`**: Mock HTTP client báº±ng `httpx.MockTransport` Ä‘á»ƒ kiá»ƒm tra cĂ¡c rule Ä‘á»™c láº­p mĂ  khĂ´ng cáº§n cháº¡y mock API tháº­t.<br>**Integration test `validator.py`**: Cáº§n cháº¡y Kafka + 3 mock APIs. | Dá»¯ liá»‡u chia luá»“ng vĂ  lÆ°u vĂ o database: `radius.valid`, `radius.invalid`, `invalid_log`. |
| **s3_dedup** | `pipeline/deduplication/`<br>â†³ `state_manager.py`, `dedup_job.py` | `s2_validation` | Chá»‰ xá»­ lĂ½ lá»c trĂ¹ng tá»« topic `radius.valid` qua Spark RocksDB State Store. KhĂ´ng gá»i API ngoĂ i. | **Integration**: Nhá»“i dá»¯ liá»‡u trĂ¹ng hoĂ n toĂ n vĂ  trĂ¹ng máº¥p mĂ© (near-duplicate) vĂ o `radius.valid` â†’ kiá»ƒm tra chá»‰ 1 record Ä‘i tiáº¿p vĂ o `radius.dedup`, báº£ng `duplicate_log` ghi Ä‘Ăºng sá»‘ lÆ°á»£ng. | Topic Kafka `radius.dedup` sáº¡ch dá»¯ liá»‡u trĂ¹ng láº·p. |
| **s4_conflict** | `pipeline/conflict_resolution/`<br>â†³ `resolver.py`, `swap_detector.py` | `s3_dedup`, `hlr_hss`, `storage_migrations` | XĂ¡c Ä‘á»‹nh cĂ¡c xung Ä‘á»™t SIM/Device. `swap_detector.py` cáº§n gá»i HLR/HSS mock láº¥y lá»‹ch sá»­ IMSI. | **Unit test `resolver.py`**: Táº¡o dá»¯ liá»‡u giáº£ láº­p conflict A/B/C â†’ xĂ¡c nháº­n káº¿t quáº£ Ä‘á»‹nh tuyáº¿n.<br>**Integration**: Cháº¡y kĂ¨m HLR/HSS mock + Kafka + Postgres. | Topic Kafka `radius.clean` + Báº£ng ghi nháº­n hoĂ¡n Ä‘á»•i thiáº¿t bá»‹ `swap_event` + `conflict_log`. |
| **s5_storage** | `pipeline/storage/`<br>â†³ `models.py`, `writer.py` | `s4_conflict`, `storage_migrations` | Ghi dá»¯ liá»‡u sáº¡ch cuá»‘i cĂ¹ng vĂ o cÆ¡ sá»Ÿ dá»¯ liá»‡u PostgreSQL Ä‘Ă£ Ä‘Æ°á»£c cháº¡y migration. | **Integration**: Consume tá»« `radius.clean` â†’ kiá»ƒm tra báº£ng `radius_sessions`, phĂ¢n vĂ¹ng thĂ¡ng chuáº©n, check cĂ¢u lá»‡nh query cĂ³ tá»‘i Æ°u. | Báº£ng dá»¯ liá»‡u `radius_sessions` sáºµn sĂ ng Ä‘á»ƒ API truy váº¥n. |

---

### Phase 5: API Layer â€” FastAPI phá»¥c vá»¥ CAMARA API
*Táº§ng phá»¥c vá»¥ á»©ng dá»¥ng, cung cáº¥p cĂ¡c endpoint SIM Swap, Device Swap vĂ  Number Verification.*

| Module | ÄÆ°á»ng dáº«n / TĂªn file | Phá»¥ thuá»™c | LĂ½ do cĂ³ thá»ƒ code | CĂ¡ch test | Output cho module sau |
|---|---|---|---|---|---|
| **api_deps** | `api/dependencies/` & `api/schemas/`<br>â†³ `auth.py`, `database.py`, `sim_swap.py`,... | `storage_migrations`, `config_models` | Thiáº¿t láº­p xĂ¡c thá»±c API key (auth) vĂ  pool káº¿t ná»‘i database (`asyncpg`), Ä‘á»‹nh nghÄ©a Pydantic schemas. | **Unit test**: Kiá»ƒm tra tĂ­nh há»£p lá»‡ cá»§a API Key, cĂ¡c Ä‘á»‹nh dáº¡ng Ä‘áº§u vĂ o (phoneNumber E.164, maxAge range). | Dependencies vĂ  schemas sáºµn sĂ ng nhĂºng vĂ o routers. |
| **api_routers** | `api/routers/` + `api/main.py`<br>â†³ `sim_swap.py`, `device_swap.py`, `health.py`,... | `s5_storage`, `api_deps` | CĂ¡c router truy váº¥n dá»¯ liá»‡u trá»±c tiáº¿p tá»« PostgreSQL (báº£ng `radius_sessions` & `swap_event`). | **Integration test**: Táº¡o trÆ°á»›c dá»¯ liá»‡u máº«u (mock data) trong Postgres test DB Ä‘á»ƒ cháº¡y test suite Ä‘á»™c láº­p mĂ  khĂ´ng cáº§n chá» pipeline cháº¡y thá»±c táº¿. | 3 endpoints CAMARA hoáº¡t Ä‘á»™ng táº¡i cá»•ng `:8000`. |

---

### Phase 6: Reporting & Observability â€” BĂ¡o cĂ¡o & GiĂ¡m sĂ¡t váº­n hĂ nh
*CĂ¡c cĂ´ng cá»¥ há»— trá»£ bĂ¡o cĂ¡o cháº¥t lÆ°á»£ng dá»¯ liá»‡u vĂ  giĂ¡m sĂ¡t há»‡ thá»‘ng thá»i gian thá»±c.*

| Module | ÄÆ°á»ng dáº«n / TĂªn file | Phá»¥ thuá»™c | MĂ´ táº£ hoáº¡t Ä‘á»™ng | CĂ¡ch test | Output / Káº¿t quáº£ |
|---|---|---|---|---|---|
| **reporting** | `reporting/`<br>â†³ `metrics_collector.py`, `quality_report.py`, Jinja2 template | `s5_storage` | Tá»•ng há»£p sá»‘ liá»‡u tá»« cĂ¡c báº£ng log (`invalid_log`, `duplicate_log`, `conflict_log`) Ä‘á»ƒ xuáº¥t bĂ¡o cĂ¡o. | Táº¡o dá»¯ liá»‡u log giáº£ láº­p trong Postgres â†’ Cháº¡y `quality_report.py` â†’ Kiá»ƒm tra file bĂ¡o cĂ¡o HTML. | BĂ¡o cĂ¡o HTML Data Quality Report hoĂ n chá»‰nh. |
| **infra_monitoring** | `infra/` (Prometheus & Grafana)<br>â†³ `prometheus.yml`, `pipeline_dashboard.json` | `api_routers`, `s5_storage` | Thu tháº­p metrics hiá»‡u nÄƒng tá»« FastAPI, Spark vĂ  PostgreSQL. | Kiá»ƒm tra tráº¡ng thĂ¡i Prometheus targets, verify giao diá»‡n Grafana hiá»ƒn thá»‹ Ä‘áº§y Ä‘á»§ biá»ƒu Ä‘á»“. | Dashboard hiá»ƒn thá»‹ lÆ°á»£ng throughput/latency trá»±c tiáº¿p. |

---

### Phase 7: End-to-End Integration & Load Test
*ÄĂ¡nh giĂ¡ toĂ n diá»‡n há»‡ thá»‘ng dÆ°á»›i táº£i trá»ng cao vĂ  kiá»ƒm thá»­ tĂ­ch há»£p Ä‘áº§u cuá»‘i.*

| Module | ÄÆ°á»ng dáº«n / TĂªn file | Phá»¥ thuá»™c | MĂ´ táº£ ká»‹ch báº£n | Káº¿t quáº£ mong Ä‘á»£i |
|---|---|---|---|---|
| **e2e_tests** | `tests/pipeline/` (TC23â€“TC33)<br>â†³ `test_deduplication.py`, `test_validation.py`,... | `s5_storage`, `gsma_tac`, `hlr_hss`, `itu_e164` | Cháº¡y toĂ n bá»™ stack há»‡ thá»‘ng, Ä‘áº©y báº£n ghi vĂ o Kafka raw, Ä‘á»£i Spark xá»­ lĂ½ vĂ  assert dá»¯ liá»‡u cuá»‘i trong Postgres. | Kiá»ƒm chá»©ng tĂ­nh chĂ­nh xĂ¡c cá»§a toĂ n bá»™ luá»“ng pipeline. |
| **load_test** | `scripts/run_load_test.sh`<br>â†³ Ká»‹ch báº£n k6 cho 3 endpoints | `api_routers`, `s5_storage` | Cháº¡y thá»­ nghiá»‡m vá»›i 100 Virtual Users (VU) trong vĂ²ng 60s, kiá»ƒm tra hiá»‡u nÄƒng khi PostgreSQL Ä‘Ă£ Ä‘Æ°á»£c náº¡p sáºµn ~2M dĂ²ng dá»¯ liá»‡u. | Äáº£m báº£o SLA: p95 SIM/Device Swap â‰¤ 200ms, Number Verification â‰¤ 100ms. |

---

## đŸ“ TĂ³m táº¯t kháº£ nÄƒng Test Ä‘á»™c láº­p (Mock / Dependency Table)

Báº£ng dÆ°á»›i Ä‘Ă¢y thá»‘ng kĂª má»©c Ä‘á»™ Ä‘á»™c láº­p khi kiá»ƒm thá»­ cá»§a tá»«ng module, giĂºp cĂ¡c ká»¹ sÆ° phĂ¡t triá»ƒn cĂ³ thá»ƒ xĂ¡c Ä‘á»‹nh cáº§n chuáº©n bá»‹ nhá»¯ng thĂ nh pháº§n háº¡ táº§ng (infrastructure) nĂ o khi phĂ¡t triá»ƒn module Ä‘Ă³:

| Module | Má»©c Ä‘á»™ kiá»ƒm thá»­ Ä‘á»™c láº­p | ThĂ nh pháº§n háº¡ táº§ng cáº§n thiáº¿t |
|---|---|---|
| **mock_services/shared/** | âœ” HoĂ n toĂ n Ä‘á»™c láº­p | KhĂ´ng cáº§n |
| **simulator/config.py + api/schemas/common.py** | âœ” HoĂ n toĂ n Ä‘á»™c láº­p | KhĂ´ng cáº§n |
| **storage/migrations/** | âœ” Gáº§n nhÆ° Ä‘á»™c láº­p | Chá»‰ cáº§n PostgreSQL (Docker Ä‘Æ¡n láº») |
| **mock_services/gsma_tac/** | âœ” Äá»™c láº­p | KhĂ´ng cáº§n (dĂ¹ng dá»¯ liá»‡u CSV) |
| **mock_services/itu_e164/** | âœ” Äá»™c láº­p | KhĂ´ng cáº§n (dĂ¹ng dá»¯ liá»‡u CSV) |
| **mock_services/hlr_hss/** | âœ” Äá»™c láº­p | KhĂ´ng cáº§n (dĂ¹ng dá»¯ liá»‡u CSV) |
| **simulator/** | â  Phá»¥ thuá»™c Mock API | Cáº§n cháº¡y **GSMA TAC mock** (`:8100`) |
| **pipeline/ingestion/** | â  Phá»¥ thuá»™c Kafka | Cáº§n cháº¡y **Kafka broker** vĂ  file CSV giáº£ láº­p |
| **pipeline/validation/ rules.py** | âœ” Unit test Ä‘á»™c láº­p Ä‘Æ°á»£c | Sá»­ dá»¥ng `httpx.MockTransport` (khĂ´ng cáº§n Mock API thá»±c) |
| **pipeline/validation/ (tĂ­ch há»£p)** | â  Cáº§n tĂ­ch há»£p | Cáº§n Kafka + 3 Mock APIs + PostgreSQL |
| **pipeline/deduplication/** | â  Cáº§n Spark + Kafka | Cáº§n Kafka + Spark Engine + PostgreSQL |
| **pipeline/conflict_resolution/** | â  Cáº§n tĂ­ch há»£p | Cáº§n Kafka + Spark + HLR/HSS mock + PostgreSQL |
| **pipeline/storage/** | â  Cáº§n Storage | Cáº§n Kafka + Spark + PostgreSQL |
| **api/ (unit test: auth & schemas)** | âœ” HoĂ n toĂ n Ä‘á»™c láº­p | KhĂ´ng cáº§n |
| **api/ (integration test)** | â  Cáº§n Database | Chá»‰ cáº§n PostgreSQL Ä‘Ă£ Ä‘Æ°á»£c náº¡p dá»¯ liá»‡u máº«u |
| **reporting/** | â  Cáº§n Database | Chá»‰ cáº§n PostgreSQL chá»©a dá»¯ liá»‡u log |
| **tests/pipeline/ (E2E)** | âœ— KhĂ´ng Ä‘á»™c láº­p | Pháº£i cháº¡y **toĂ n bá»™ stack há»‡ thá»‘ng** |
| **load test (k6)** | âœ— KhĂ´ng Ä‘á»™c láº­p | ToĂ n bá»™ stack Ä‘ang cháº¡y + náº¡p sáºµn ~2M dĂ²ng dá»¯ liá»‡u |
