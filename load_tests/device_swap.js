/**
 * load_tests/device_swap.js
 *
 * k6 Load Test — Device Swap API
 * Endpoints: POST /device-swap/v0/check và /device-swap/v0/retrieve-date
 *
 * Cấu hình: 100 VU, ramp 30s + sustain 60s + ramp-down 10s
 * SLA: p95 ≤ 200ms (giống SIM Swap)
 *
 * Chạy:
 *   k6 run load_tests/device_swap.js \
 *     --env BASE_URL=http://localhost:8000 \
 *     --env API_KEY=dev-secret \
 *     --out json=reports/device_swap_load.json
 */

import http from "k6/http";
import { check, sleep } from "k6";
import { Trend, Rate, Counter } from "k6/metrics";

// ── Custom metrics ────────────────────────────────────────────────────────────
const checkLatency    = new Trend("device_swap_check_latency",    true);
const retrieveLatency = new Trend("device_swap_retrieve_latency", true);
const successRate     = new Rate("device_swap_success_rate");
const errorCount      = new Counter("device_swap_errors");


// ── Cấu hình ─────────────────────────────────────────────────────────────────
export const options = {
    stages: [
        { duration: "10s", target: 20 },
        { duration: "20s", target: 20 },
        { duration: "5s", target: 0   },
    ],
    thresholds: {
        "http_req_duration{endpoint:device_swap_check}":    ["p(95)<200"],
        "http_req_duration{endpoint:device_swap_retrieve}": ["p(95)<200"],
        "device_swap_check_latency":    ["p(95)<200"],
        "device_swap_retrieve_latency": ["p(95)<200"],
        "device_swap_success_rate": ["rate>0.99"],
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
    const msisdn = randomMsisdn();
    const body   = JSON.stringify({ phoneNumber: msisdn, maxAge: 30 });

    // ── /device-swap/v0/check ─────────────────────────────────────────────────
    const checkRes = http.post(
        `${BASE_URL}/device-swap/v0/check`,
        body,
        { headers: HEADERS, tags: { endpoint: "device_swap_check" } }
    );

    checkLatency.add(checkRes.timings.duration);

    const checkOk = check(checkRes, {
        "check: status 200":             (r) => r.status === 200,
        "check: has deviceSwapped field": (r) => {
            try {
                return typeof JSON.parse(r.body).deviceSwapped === "boolean";
            } catch(e) { return false; }
        },
    });

    successRate.add(checkOk);
    if (!checkOk) errorCount.add(1);


    // ── /device-swap/v0/retrieve-date ─────────────────────────────────────────
    const retrieveRes = http.post(
        `${BASE_URL}/device-swap/v0/retrieve-date`,
        body,
        { headers: HEADERS, tags: { endpoint: "device_swap_retrieve" } }
    );

    retrieveLatency.add(retrieveRes.timings.duration);

    const retrieveOk = check(retrieveRes, {
        "retrieve: status 200":                (r) => r.status === 200,
        "retrieve: has latestDeviceChange key": (r) => {
            try {
                return "latestDeviceChange" in JSON.parse(r.body);
            } catch (e){ return false; }
        },
    });

    successRate.add(retrieveOk);
    if (!retrieveOk) errorCount.add(1);

    sleep(0.1);
}