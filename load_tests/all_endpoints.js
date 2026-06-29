/**
 * load_tests/all_endpoints.js
 *
 * k6 Load Test — Tất cả 3 CAMARA endpoints trong 1 lần chạy.
 *
 * Dùng k6 scenarios để chạy song song:
 *   - 40% VU → SIM Swap        (SLA p95 ≤ 200ms)
 *   - 30% VU → Device Swap     (SLA p95 ≤ 200ms)
 *   - 30% VU → Number Verify   (SLA p95 ≤ 100ms)
 *
 * Tổng: 100 VU, mô phỏng traffic mix thực tế.
 *
 * Chạy:
 *   k6 run load_tests/all_endpoints.js \
 *     --env BASE_URL=http://localhost:8000 \
 *     --env API_KEY=dev-secret \
 *     --out json=reports/all_endpoints_load.json
 *
 * Xem summary:
 *   k6 run ... --summary-export=reports/summary.json
 */

import http from "k6/http";
import { check, sleep } from "k6";
import { Trend, Rate } from "k6/metrics";

// ── Custom metrics per endpoint ───────────────────────────────────────────────
const simSwapLatency     = new Trend("latency_sim_swap",     true);
const deviceSwapLatency  = new Trend("latency_device_swap",  true);
const numberVerifyLatency= new Trend("latency_number_verify",true);
const overallSuccessRate = new Rate("overall_success_rate");


// ── Scenarios ─────────────────────────────────────────────────────────────────
export const options = {
    scenarios: {
        sim_swap: {
            executor: "constant-vus",
            vus: 20,
            duration: "30s",  // 30s ramp + 60s sustain + 10s cooldown
            exec: "simSwapScenario",
            startTime: "0s",
        },
        device_swap: {
            executor: "constant-vus",
            vus: 20,
            duration: "30s",
            exec: "deviceSwapScenario",
            startTime: "0s",
        },
        number_verify: {
            executor: "constant-vus",
            vus: 20,
            duration: "30s",
            exec: "numberVerifyScenario",
            startTime: "0s",
        },
    },

    thresholds: {
        // SIM Swap — p95 ≤ 200ms
        "latency_sim_swap":    ["p(95)<200"],
        "http_req_duration{scenario:sim_swap}": ["p(95)<200"],

        // Device Swap — p95 ≤ 200ms
        "latency_device_swap": ["p(95)<200"],
        "http_req_duration{scenario:device_swap}": ["p(95)<200"],

        // Number Verification — p95 ≤ 100ms (nghiêm hơn)
        "latency_number_verify": ["p(95)<100"],
        "http_req_duration{scenario:number_verify}": ["p(95)<100"],

        // Overall
        "overall_success_rate": ["rate>0.99"],
        "http_req_failed": ["rate<0.01"],
    },
};


// ── Env + shared helpers ──────────────────────────────────────────────────────
const BASE_URL = __ENV.BASE_URL || "http://localhost:8000";
const API_KEY  = __ENV.API_KEY  || "dev-secret";
const HEADERS  = {
    "Content-Type": "application/json",
    "X-API-Key": API_KEY,
};

const MSISDNS = Array.from({ length: 200 }, (_, i) =>
    `+8497${String(i).padStart(7, "0")}`
);
function randomMsisdn() {
    return MSISDNS[Math.floor(Math.random() * MSISDNS.length)];
}
function randomMaxAge() {
    return [1, 7, 14, 30][Math.floor(Math.random() * 4)];
}


// ── Scenario functions ────────────────────────────────────────────────────────

/**
 * SIM Swap scenario:
 * Mỗi VU luân phiên gọi /check và /retrieve-date.
 */
export function simSwapScenario() {
    const body = JSON.stringify({
        phoneNumber: randomMsisdn(),
        maxAge: randomMaxAge(),
    });

    // 50% check, 50% retrieve-date (mô phỏng mix thực tế)
    const endpoint = Math.random() < 0.5
        ? "/sim-swap/v0/check"
        : "/sim-swap/v0/retrieve-date";

    const res = http.post(`${BASE_URL}${endpoint}`, body, {
        headers: HEADERS,
        tags: { api: "sim_swap" },
    });

    simSwapLatency.add(res.timings.duration);

    const ok = check(res, {
        "sim_swap: 200": (r) => r.status === 200,
    });
    overallSuccessRate.add(ok);
    sleep(0.1);
}


/**
 * Device Swap scenario.
 */
export function deviceSwapScenario() {
    const body = JSON.stringify({
        phoneNumber: randomMsisdn(),
        maxAge: randomMaxAge(),
    });

    const endpoint = Math.random() < 0.5
        ? "/device-swap/v0/check"
        : "/device-swap/v0/retrieve-date";

    const res = http.post(`${BASE_URL}${endpoint}`, body, {
        headers: HEADERS,
        tags: { api: "device_swap" },
    });

    deviceSwapLatency.add(res.timings.duration);

    const ok = check(res, {
        "device_swap: 200": (r) => r.status === 200,
    });
    overallSuccessRate.add(ok);
    sleep(0.1);
}


/**
 * Number Verification scenario — think time nhỏ hơn vì SLA chặt hơn.
 */
export function numberVerifyScenario() {
    const body = JSON.stringify({ phoneNumber: randomMsisdn() });

    const res = http.post(
        `${BASE_URL}/number-verification/v0/verify`,
        body,
        { headers: HEADERS, tags: { api: "number_verify" } }
    );

    numberVerifyLatency.add(res.timings.duration);

    const ok = check(res, {
        "number_verify: 200": (r) => r.status === 200,
    });
    overallSuccessRate.add(ok);
    sleep(0.05);
}