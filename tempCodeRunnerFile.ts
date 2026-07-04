import { useState } from "react";

const PHASES = [
  {
    id: 1,
    label: "Phase 1",
    title: "Zero dependency — code & test hoàn toàn độc lập",
    color: "#1D6F42",
    bg: "#E8F5E9",
    border: "#1D6F42",
    modules: [
      {
        id: "shared",
        name: "mock_services/shared/",
        files: ["health.py", "pagination.py", "errors.py"],
        why: "Pure utility — không import gì từ project, không cần DB hay network.",
        test: "pytest unit: test error format, test Page[T] generic, test fault-injection header parse.",
        out: "Shared library dùng bởi 3 mock services ở Phase 2.",
      },
      {
        id: "config_models",
        name: "simulator/config.py  +  api/schemas/common.py",
        files: ["SimulatorConfig dataclass", "PhoneNumber type", "ErrorResponse model"],
        why: "Dataclass và Pydantic model thuần — zero external dep.",
        test: "pytest unit: validate PhoneNumber E.164, validate SimulatorConfig bounds.",
        out: "Type contracts dùng xuyên suốt project.",
      },
      {
        id: "storage_migrations",
        name: "storage/migrations/",
        files: ["001_init_schema.sql", "002_indexes.sql", "003_partitions.sql"],
        why: "SQL thuần — viết và review offline, không cần service nào chạy.",
        test: "Chạy trên PostgreSQL Docker đơn lẻ, kiểm tra EXPLAIN ANALYZE cho 3 query pattern chính.",
        out: "Schema đã validate — tất cả module sau đều build trên schema này.",
      },
    ],
  },
  {
    id: 2,
    label: "Phase 2",
    title: "Mock Services — độc lập với pipeline, test với data tĩnh",
    color: "#1F4E79",
    bg: "#EBF3FB",
    border: "#2E75B6",
    modules: [
      {
        id: "gsma_tac",
        name: "mock_services/gsma_tac/",
        files: ["models.py", "seed.py", "router.py", "app.py"],
        why: "Chỉ cần: shared/ (Phase 1) + file CSV tĩnh. Không cần DB, không cần Kafka.",
        test: "pytest: GET /tac/{tac} found/not-found, POST /batch, pagination, fault injection header.",
        out: "HTTP API :8100 — simulator và pipeline/validation dùng ở Phase 3+.",
        deps: ["shared"],
      },
      {
        id: "itu_e164",
        name: "mock_services/itu_e164/",
        files: ["models.py", "seed.py", "router.py", "app.py"],
        why: "Chỉ cần: shared/ + 2 CSV tĩnh (country_codes, operator_prefixes). Không có state.",
        test: "pytest: POST /validate — valid/invalid/unknown CC/short number, POST /validate/batch.",
        out: "HTTP API :8300 — pipeline/validation Rule R2 dùng ở Phase 4.",
        deps: ["shared"],
      },
      {
        id: "hlr_hss",
        name: "mock_services/hlr_hss/",
        files: ["models.py", "seed.py", "router.py", "app.py"],
        why: "Chỉ cần: shared/ + subscribers.csv. Seed phải dùng seed=42 khớp simulator.",
        test: "pytest: by-imsi/by-msisdn found/404, imsi-history length, batch-lookup mixed.",
        out: "HTTP API :8200 — pipeline/validation R3 và conflict_resolution dùng ở Phase 4+.",
        deps: ["shared"],
        note: "⚠ seed.py phải chạy TRƯỚC simulator/generators.py để đảm bảo cùng pool subscriber.",
      },
    ],
  },
  {
    id: 3,
    label: "Phase 3",
    title: "Simulator — phụ thuộc GSMA TAC mock",
    color: "#E8A317",
    bg: "#FFF8E1",
    border: "#E8A317",
    modules: [
      {
        id: "simulator",
        name: "simulator/",
        files: ["config.py (done)", "generators.py", "error_injectors.py", "simulator.py"],
        why: "generators.py gọi GET /tac để load TAC hợp lệ khi khởi động → cần GSMA TAC mock :8100 đang chạy.",
        test: "Integration test: chạy với --records 10000 --seed 42, kiểm tra CSV output có đủ schema, tỷ lệ lỗi trong ±1% so với config, mọi IMEI hợp lệ đều có TAC trong mock.",
        out: "data/radius_log.csv — input cho toàn bộ pipeline.",
        deps: ["gsma_tac", "config_models"],
        note: "error_injectors.py có thể viết trước generators.py vì chỉ biến đổi record đã có.",
      },
    ],
  },
  {
    id: 4,
    label: "Phase 4",
    title: "Pipeline core — từng stage độc lập, test tuần tự",
    color: "#7B2D8B",
    bg: "#F3E5F5",
    border: "#9C27B0",
    modules: [
      {
        id: "s1_ingestion",
        name: "pipeline/ingestion/",
        files: ["csv_reader.py", "producer.py"],
        why: "Chỉ cần: Kafka đang chạy + file CSV từ simulator. Không cần DB, không cần mock services.",
        test: "Integration: publish 1000 records → consume từ radius.raw → verify count và partition key distribution.",
        out: "Kafka topic radius.raw có data.",
        deps: ["simulator"],
        note: "csv_reader.py có thể unit test hoàn toàn không cần Kafka — chỉ cần file CSV mẫu nhỏ.",
      },
      {
        id: "s2_validation",
        name: "pipeline/validation/",
        files: ["rules.py", "validator.py"],
        why: "rules.py gọi 3 mock services (R2→ITU, R3→HLR, R4→GSMA TAC). validator.py cần Kafka + Spark.",
        test: "Unit test rules.py: mock HTTP responses bằng httpx MockTransport — test từng rule độc lập không cần mock service chạy thật. Integration test validator.py: cần 3 mock + Kafka.",
        out: "radius.valid + radius.invalid + invalid_log rows trong PostgreSQL.",
        deps: ["s1_ingestion", "gsma_tac", "hlr_hss", "itu_e164", "storage_migrations"],
        note: "Tách biệt unit test rules.py (không cần infra) khỏi integration test validator.py (cần full stack). Code rules.py trước validator.py.",
      },
      {
        id: "s3_dedup",
        name: "pipeline/deduplication/",
        files: ["state_manager.py", "dedup_job.py"],
        why: "Chỉ cần: Kafka radius.valid (từ S2) + Spark với RocksDB. Không gọi mock service nào.",
        test: "Integration: inject exact duplicate và near-duplicate vào radius.valid → verify chỉ 1 record qua radius.dedup, duplicate_log có đúng count.",
        out: "radius.dedup sạch duplicate.",
        deps: ["s2_validation"],
        note: "state_manager.py (RocksDB wrapper) có thể unit test độc lập không cần Kafka.",
      },
      {
        id: "s4_conflict",
        name: "pipeline/conflict_resolution/",
        files: ["resolver.py", "swap_detector.py"],
        why: "swap_detector.py gọi HLR/HSS mock GET /imsi-history để xác nhận conflict C. resolver.py cần Kafka + Spark.",
        test: "Unit test resolver.py: inject conflict A/B/C records thủ công → verify routing. Integration: cần HLR/HSS mock + Kafka + PostgreSQL (swap_event table).",
        out: "radius.clean + swap_event rows + conflict_log rows.",
        deps: ["s3_dedup", "hlr_hss", "storage_migrations"],
        note: "Viết resolver.py (không cần network) trước swap_detector.py (cần HLR/HSS mock).",
      },
      {
        id: "s5_storage",
        name: "pipeline/storage/",
        files: ["models.py", "writer.py"],
        why: "Cần: Kafka radius.clean (từ S4) + PostgreSQL schema đã migrate. Không gọi mock service.",
        test: "Integration: consume từ radius.clean → verify rows trong radius_sessions, partition đúng tháng, EXPLAIN ANALYZE query dưới 200ms.",
        out: "radius_sessions populated — API layer có thể query.",
        deps: ["s4_conflict", "storage_migrations"],
        note: "models.py (SQLAlchemy) có thể viết và unit test song song với storage/migrations/.",
      },
    ],
  },
  {
    id: 5,
    label: "Phase 5",
    title: "API Layer — phụ thuộc storage layer đã có data",
    color: "#C00000",
    bg: "#FCE4EC",
    border: "#C00000",
    modules: [
      {
        id: "api_deps",
        name: "api/dependencies/  +  api/schemas/",
        files: ["auth.py", "database.py", "sim_swap.py", "device_swap.py", "number_verification.py"],
        why: "auth.py: pure function kiểm tra header. database.py: asyncpg pool — chỉ cần PostgreSQL. Schemas: Pydantic models thuần.",
        test: "Unit test auth.py: valid/invalid/missing API key. Unit test schemas: validate phoneNumber format, maxAge range.",
        out: "Dependencies sẵn sàng cho routers.",
        deps: ["storage_migrations", "config_models"],
      },
      {
        id: "api_routers",
        name: "api/routers/  +  api/main.py",
        files: ["sim_swap.py", "device_swap.py", "number_verification.py", "health.py", "main.py"],
        why: "Routers query PostgreSQL trực tiếp — cần data trong radius_sessions và swap_event (từ Phase 4).",
        test: "Integration test (TC01–TC22, TC34–TC36): inject data thẳng vào PostgreSQL test DB, không qua pipeline. pytest + httpx.AsyncClient.",
        out: "3 CAMARA endpoints live tại :8000.",
        deps: ["s5_storage", "api_deps"],
        note: "Có thể test API với data inject thủ công TRƯỚC KHI pipeline hoàn chỉnh — không cần chờ Phase 4 xong toàn bộ.",
      },
    ],
  },
  {
    id: 6,
    label: "Phase 6",
    title: "Reporting & Observability — chạy sau khi có data",
    color: "#595959",
    bg: "#F5F5F5",
    border: "#9E9E9E",
    modules: [
      {
        id: "reporting",
        name: "reporting/",
        files: ["metrics_collector.py", "quality_report.py", "templates/report.html.jinja2"],
        why: "Query các log tables (invalid_log, duplicate_log, conflict_log) — cần PostgreSQL có data từ pipeline run.",
        test: "Seed log tables với dữ liệu mẫu → chạy quality_report.py → kiểm tra HTML có đủ 6 section và số liệu khớp.",
        out: "HTML Data Quality Report.",
        deps: ["s5_storage"],
      },
      {
        id: "infra_monitoring",
        name: "infra/  (Prometheus + Grafana)",
        files: ["prometheus.yml", "pipeline_dashboard.json"],
        why: "Scrape metrics từ FastAPI, Spark, PostgreSQL — cần tất cả service đang chạy.",
        test: "Kiểm tra Prometheus targets up, Grafana dashboard load đủ panels.",
        out: "Dashboard throughput/latency live.",
        deps: ["api_routers", "s5_storage"],
        note: "docker-compose.yml tự cấu hình — không cần code nhiều. Có thể làm song song Phase 5.",
      },
    ],
  },
  {
    id: 7,
    label: "Phase 7",
    title: "End-to-end integration & load test",
    color: "#3E2B60",
    bg: "#EDE7F6",
    border: "#673AB7",
    modules: [
      {
        id: "e2e_tests",
        name: "tests/pipeline/  (TC23–TC33)",
        files: ["test_deduplication.py", "test_conflict_resolution.py", "test_late_arrival.py", "test_validation.py"],
        why: "Cần toàn bộ pipeline đang chạy (Kafka + Spark + mock services + PostgreSQL).",
        test: "Chạy pytest tests/pipeline/ — inject records vào Kafka, đợi Spark xử lý, assert kết quả trong PostgreSQL.",
        out: "Xác nhận pipeline end-to-end đúng.",
        deps: ["s5_storage", "gsma_tac", "hlr_hss", "itu_e164"],
      },
      {
        id: "load_test",
        name: "scripts/run_load_test.sh  (k6)",
        files: ["k6 script cho 3 endpoints"],
        why: "Cần API đang live và PostgreSQL có đủ data (~2M records) để đo latency thực tế.",
        test: "k6: 100 VU, 60s. Assert p95 SIM/Device ≤200ms, Number Verify ≤100ms.",
        out: "Báo cáo hiệu năng — deliverable D7.",
        deps: ["api_routers", "s5_storage"],
      },
    ],
  },
];

const DEP_NAMES = {
  shared: "mock_services/shared/",
  config_models: "config.py + schemas/common.py",
  storage_migrations: "storage/migrations/",
  gsma_tac: "gsma_tac/",
  itu_e164: "itu_e164/",
  hlr_hss: "hlr_hss/",
  simulator: "simulator/",
  s1_ingestion: "ingestion/",
  s2_validation: "validation/",
  s3_dedup: "deduplication/",
  s4_conflict: "conflict_resolution/",
  s5_storage: "pipeline/storage/",
  api_deps: "api/dependencies/",
  api_routers: "api/routers/",
  reporting: "reporting/",
  infra_monitoring: "infra/",
  e2e_tests: "tests/pipeline/",
  load_test: "load_test",
};

const PHASE_COLORS = {
  1: "#1D6F42", 2: "#1F4E79", 3: "#B45309", 4: "#7B2D8B",
  5: "#C00000", 6: "#595959", 7: "#3E2B60",
};

export default function BuildOrder() {
  const [expanded, setExpanded] = useState({});
  const [activeModule, setActiveModule] = useState(null);

  const toggle = (key) => setExpanded(p => ({ ...p, [key]: !p[key] }));

  const allModules = PHASES.flatMap(p => p.modules);
  const getModule = (id) => allModules.find(m => m.id === id);
  const getPhaseOf = (id) => PHASES.find(p => p.modules.some(m => m.id === id));

  return (
    <div style={{ fontFamily: "Arial, sans-serif", maxWidth: 900, margin: "0 auto", padding: 24, color: "#1a1a1a" }}>

      {/* Header */}
      <div style={{ background: "#1F4E79", borderRadius: 8, padding: "20px 24px", marginBottom: 28, color: "#fff" }}>
        <div style={{ fontSize: 22, fontWeight: 700, marginBottom: 6 }}>
          Thứ tự code & Dependency Map
        </div>
        <div style={{ fontSize: 13, opacity: 0.85 }}>
          CAMARA Network API Data Pipeline — 7 phase · 20 modules
        </div>
        <div style={{ marginTop: 14, display: "flex", gap: 10, flexWrap: "wrap" }}>
          {[
            { dot: "#1D6F42", label: "Phase 1–2: Độc lập hoàn toàn" },
            { dot: "#E8A317", label: "Phase 3: Phụ thuộc mock" },
            { dot: "#7B2D8B", label: "Phase 4: Pipeline stages" },
            { dot: "#C00000", label: "Phase 5: API layer" },
            { dot: "#595959", label: "Phase 6–7: E2E & reporting" },
          ].map(({ dot, label }) => (
            <div key={label} style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 12, background: "rgba(255,255,255,0.12)", borderRadius: 20, padding: "3px 10px" }}>
              <div style={{ width: 9, height: 9, borderRadius: "50%", background: dot, flexShrink: 0 }} />
              {label}
            </div>
          ))}
        </div>
      </div>

      {/* Parallel tracks notice */}
      <div style={{ background: "#FFF8E1", border: "1px solid #E8A317", borderRadius: 6, padding: "10px 14px", marginBottom: 24, fontSize: 13, color: "#5D4037" }}>
        <strong>⚡ Có thể làm song song:</strong> Phase 1 modules chạy song song nhau · Phase 2 mock services chạy song song nhau ·
        storage/models.py (Phase 4 storage) có thể viết song song với storage/migrations/ (Phase 1) ·
        api/schemas/ (Phase 5) có thể viết song song Phase 4 · infra/Grafana có thể cấu hình song song Phase 5.
      </div>

      {/* Phases */}
      {PHASES.map(phase => (
        <div key={phase.id} style={{ marginBottom: 24 }}>

          {/* Phase header */}
          <div style={{
            background: phase.color, borderRadius: "8px 8px 0 0",
            padding: "10px 18px", display: "flex", alignItems: "center", gap: 12,
          }}>
            <div style={{
              background: "rgba(255,255,255,0.2)", borderRadius: 20,
              padding: "2px 12px", fontSize: 12, fontWeight: 700, color: "#fff",
            }}>
              {phase.label}
            </div>
            <div style={{ color: "#fff", fontWeight: 600, fontSize: 15 }}>{phase.title}</div>
          </div>

          {/* Modules */}
          <div style={{ border: `1px solid ${phase.border}`, borderTop: "none", borderRadius: "0 0 8px 8px", overflow: "hidden" }}>
            {phase.modules.map((mod, idx) => {
              const key = `${phase.id}-${mod.id}`;
              const isOpen = expanded[key];
              const isActive = activeModule === mod.id;
              return (
                <div key={mod.id} style={{
                  borderTop: idx > 0 ? `1px solid ${phase.border}30` : "none",
                  background: isActive ? phase.bg : (idx % 2 === 0 ? "#fff" : "#FAFAFA"),
                }}>

                  {/* Module row */}
                  <div
                    onClick={() => { toggle(key); setActiveModule(mod.id); }}
                    style={{
                      padding: "12px 18px", cursor: "pointer",
                      display: "flex", alignItems: "flex-start", gap: 12,
                    }}
                  >
                    <div style={{
                      background: phase.color, color: "#fff",
                      borderRadius: 4, padding: "2px 8px",
                      fontSize: 11, fontWeight: 700, flexShrink: 0, marginTop: 1,
                    }}>
                      P{phase.id}
                    </div>

                    <div style={{ flex: 1, minWidth: 0 }}>
                      <div style={{ fontWeight: 600, fontSize: 14, color: "#1a1a1a" }}>
                        {mod.name}
                      </div>
                      <div style={{ fontSize: 12, color: "#555", marginTop: 3 }}>
                        {mod.files.join("  ·  ")}
                      </div>
                    </div>

                    {/* Dep badges */}
                    <div style={{ display: "flex", gap: 4, flexWrap: "wrap", justifyContent: "flex-end", minWidth: 120 }}>
                      {!mod.deps || mod.deps.length === 0 ? (
                        <span style={{ fontSize: 11, background: "#E8F5E9", color: "#1D6F42", borderRadius: 10, padding: "2px 8px", fontWeight: 600 }}>
                          ✔ Độc lập
                        </span>
                      ) : mod.deps.map(d => {
                        const ph = getPhaseOf(d);
                        return (
                          <span key={d} style={{
                            fontSize: 10, borderRadius: 10, padding: "2px 7px",
                            background: `${PHASE_COLORS[ph?.id] || "#888"}18`,
                            color: PHASE_COLORS[ph?.id] || "#888",
                            border: `1px solid ${PHASE_COLORS[ph?.id] || "#888"}40`,
                            whiteSpace: "nowrap",
                          }}>
                            P{ph?.id} {DEP_NAMES[d]}
                          </span>
                        );
                      })}
                    </div>

                    <div style={{ color: phase.color, fontSize: 16, flexShrink: 0, marginTop: 2 }}>
                      {isOpen ? "▲" : "▼"}
                    </div>
                  </div>

                  {/* Expanded detail */}
                  {isOpen && (
                    <div style={{
                      padding: "0 18px 16px 50px",
                      borderTop: `1px dashed ${phase.border}40`,
                      background: phase.bg,
                    }}>
                      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12, marginTop: 12 }}>

                        <div style={{ gridColumn: "1 / -1" }}>
                          <div style={{ fontSize: 11, fontWeight: 700, color: phase.color, textTransform: "uppercase", marginBottom: 4 }}>Tại sao có thể code ở bước này</div>
                          <div style={{ fontSize: 13, color: "#333", lineHeight: 1.6 }}>{mod.why}</div>
                        </div>

                        <div>
                          <div style={{ fontSize: 11, fontWeight: 700, color: "#1F4E79", textTransform: "uppercase", marginBottom: 4 }}>Cách test</div>
                          <div style={{ fontSize: 13, color: "#333", lineHeight: 1.6 }}>{mod.test}</div>
                        </div>

                        <div>
                          <div style={{ fontSize: 11, fontWeight: 700, color: "#1D6F42", textTransform: "uppercase", marginBottom: 4 }}>Output cho module sau</div>
                          <div style={{ fontSize: 13, color: "#333", lineHeight: 1.6 }}>{mod.out}</div>
                        </div>

                        {mod.note && (
                          <div style={{
                            gridColumn: "1 / -1",
                            background: "#FFF3CD", border: "1px solid #E8A317",
                            borderRadius: 6, padding: "8px 12px",
                            fontSize: 12, color: "#5D4037",
                          }}>
                            <strong>⚠ Lưu ý:</strong> {mod.note}
                          </div>
                        )}

                        {/* Dep detail */}
                        {mod.deps && mod.deps.length > 0 && (
                          <div style={{ gridColumn: "1 / -1" }}>
                            <div style={{ fontSize: 11, fontWeight: 700, color: "#C00000", textTransform: "uppercase", marginBottom: 6 }}>
                              Phụ thuộc ({mod.deps.length})
                            </div>
                            <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
                              {mod.deps.map(d => {
                                const ph = getPhaseOf(d);
                                const dm = getModule(d);
                                return (
                                  <div key={d} style={{
                                    fontSize: 12, borderRadius: 6, padding: "5px 10px",
                                    background: "#fff",
                                    border: `1px solid ${PHASE_COLORS[ph?.id] || "#888"}`,
                                    color: "#1a1a1a",
                                  }}>
                                    <span style={{ fontWeight: 700, color: PHASE_COLORS[ph?.id] }}>Phase {ph?.id}</span>
                                    {" · "}{DEP_NAMES[d]}
                                  </div>
                                );
                              })}
                            </div>
                          </div>
                        )}
                      </div>
                    </div>
                  )}
                </div>
              );
            })}
          </div>
        </div>
      ))}

      {/* Summary table */}
      <div style={{ marginTop: 12, background: "#F8F9FA", border: "1px solid #DEE2E6", borderRadius: 8, overflow: "hidden" }}>
        <div style={{ background: "#1F4E79", padding: "10px 18px", color: "#fff", fontWeight: 600, fontSize: 14 }}>
          Tóm tắt: Module nào test độc lập được
        </div>
        <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 13 }}>
          <thead>
            <tr style={{ background: "#EBF3FB" }}>
              {["Module", "Test độc lập?", "Cần infra nào"].map(h => (
                <th key={h} style={{ padding: "8px 14px", textAlign: "left", color: "#1F4E79", fontWeight: 600, borderBottom: "1px solid #DEE2E6" }}>{h}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {[
              ["mock_services/shared/", "✔ Hoàn toàn", "Không cần gì"],
              ["simulator/config.py + api/schemas/common.py", "✔ Hoàn toàn", "Không cần gì"],
              ["storage/migrations/", "✔ Gần như", "PostgreSQL đơn lẻ (Docker)"],
              ["mock_services/gsma_tac/", "✔ Độc lập", "Không (data từ CSV)"],
              ["mock_services/itu_e164/", "✔ Độc lập", "Không (data từ CSV)"],
              ["mock_services/hlr_hss/", "✔ Độc lập", "Không (data từ CSV)"],
              ["simulator/", "⚠ Cần mock", "GSMA TAC mock :8100"],
              ["pipeline/ingestion/", "⚠ Cần Kafka", "Kafka + file CSV"],
              ["pipeline/validation/ rules.py", "✔ Unit test được", "httpx MockTransport (không cần mock chạy thật)"],
              ["pipeline/validation/ (integration)", "⚠ Cần full", "Kafka + 3 mock services + PostgreSQL"],
              ["pipeline/deduplication/", "⚠ Cần Kafka+Spark", "Kafka + Spark + PostgreSQL"],
              ["pipeline/conflict_resolution/", "⚠ Cần full", "Kafka + Spark + HLR/HSS mock + PostgreSQL"],
              ["pipeline/storage/", "⚠ Cần DB", "Kafka + Spark + PostgreSQL"],
              ["api/ (unit: auth + schemas)", "✔ Hoàn toàn", "Không cần gì"],
              ["api/ (integration)", "⚠ Cần DB", "PostgreSQL có data"],
              ["reporting/", "⚠ Cần DB", "PostgreSQL có data"],
              ["tests/pipeline/ (e2e)", "✗ Cần tất cả", "Full stack đang chạy"],
              ["load test (k6)", "✗ Cần tất cả + data", "Full stack + 2M records"],
            ].map(([mod, status, infra], i) => (
              <tr key={mod} style={{ background: i % 2 === 0 ? "#fff" : "#F8F9FA", borderBottom: "1px solid #F0F0F0" }}>
                <td style={{ padding: "7px 14px", fontFamily: "monospace", fontSize: 12 }}>{mod}</td>
                <td style={{ padding: "7px 14px", color: status.startsWith("✔") ? "#1D6F42" : status.startsWith("✗") ? "#C00000" : "#B45309", fontWeight: 600 }}>{status}</td>
                <td style={{ padding: "7px 14px", color: "#555" }}>{infra}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

    </div>
  );
}