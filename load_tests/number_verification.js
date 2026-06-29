/**
 * load_tests/number_verification.js
 *
 * k6 Load Test — Number Verification API
 * Endpoint: POST /number-verification/v0/verify
 *
 * SLA: p95 ≤ 100ms — NGHIÊM HƠN SIM/Device Swap (200ms)
 * Lý do: endpoint này dùng EXISTS query trên radius_sessions
 * với index idx_msisdn_ts, phải đủ nhanh để xác minh real-time.
 *
 * Cấu hình: 100 VU, ramp 30s + sustain 60s + ramp-down 10s
 *
 * Chạy:
 *   k6 run load_tests/number_verification.js \
 *     --env BASE_URL=http://localhost:8000 \
 *     --env API_KEY=dev-secret \
 *     --out json=reports/number_verification_load.json
 */

import http from "k6/http";
import { check, sleep } from "k6";
import { Trend, Rate, Counter } from "k6/metrics";

// ── Custom metrics ────────────────────────────────────────────────────────────
const verifyLatency = new Trend("number_verify_latency", true);
const successRate   = new Rate("number_verify_success_rate");
const errorCount    = new Counter("number_verify_errors");


// ── Cấu hình ─────────────────────────────────────────────────────────────────
export const options = {
    stages: [
        { duration: "30s", target: 100 },
        { duration: "60s", target: 100 },
        { duration: "10s", target: 0   },
    ],
    thresholds: {
        // SLA ≤ 100ms — nghiêm hơn 2 API kia
        "http_req_duration{endpoint:number_verify}": ["p(95)<100"],
        "number_verify_latency":  ["p(95)<100"],
        "number_verify_success_rate": ["rate>0.99"],
    },
};


// ── Env + helpers ─────────────────────────────────────────────────────────────
const BASE_URL = __ENV.BASE_URL || "http://localhost:8000";
const API_KEY  = __ENV.API_KEY  || "dev-secret";
const HEADERS  = {
    "Content-Type": "application/json",
    "X-API-Key": API_KEY,
};

const MSISDNS = Array.from({ length: 100 }, (_, i) =>
    `+8497${String(i).padStart(7, "0")}`
);
function randomMsisdn() {
    return MSISDNS[Math.floor(Math.random() * MSISDNS.length)];
}


// ── Main VU function ──────────────────────────────────────────────────────────
export default function () {
    const body = JSON.stringify({ phoneNumber: randomMsisdn() });

    const res = http.post(
        `${BASE_URL}/number-verification/v0/verify`,
        body,
        { headers: HEADERS, tags: { endpoint: "number_verify" } }
    );

    verifyLatency.add(res.timings.duration);

    const ok = check(res, {
        "verify: status 200": (r) => r.status === 200,
        "verify: has devicePhoneNumberVerified": (r) => {
            try {
                return typeof JSON.parse(r.body).devicePhoneNumberVerified === "boolean";
            } catch { return false; }
        },
    });

    successRate.add(ok);
    if (!ok) errorCount.add(1);

    // Think time nhỏ hơn vì SLA chặt hơn — test throughput cao hơn
    sleep(0.05);
}