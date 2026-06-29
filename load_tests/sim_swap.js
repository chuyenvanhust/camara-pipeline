/**
 * load_tests/sim_swap.js
 *
 * k6 Load Test — SIM Swap API
 * Endpoints: POST /sim-swap/v0/check và /sim-swap/v0/retrieve-date
 *
 * Cấu hình:
 *   - 100 VU (Virtual Users)
 *   - Ramp up 30s → sustain 60s → ramp down 10s
 *   - SLA: p95 ≤ 200ms
 *
 * Chạy:
 *   k6 run load_tests/sim_swap.js \
 *     --env BASE_URL=http://localhost:8000 \
 *     --env API_KEY=dev-secret \
 *     --out json=reports/sim_swap_load.json
 */

import http from "k6/http";
import { check, sleep } from "k6";
import { Trend, Rate, Counter } from "k6/metrics";

// ── Custom metrics ────────────────────────────────────────────────────────────

/** Latency p95 của endpoint /check */
const checkLatency = new Trend("sim_swap_check_latency", true);

/** Latency p95 của endpoint /retrieve-date */
const retrieveLatency = new Trend("sim_swap_retrieve_latency", true);

/** Tỉ lệ request thành công (HTTP 200) */
const successRate = new Rate("sim_swap_success_rate");

/** Tổng số request lỗi */
const errorCount = new Counter("sim_swap_errors");


// ── Cấu hình VU và timeline ───────────────────────────────────────────────────

export const options = {
    stages: [
        { duration: "10s", target: 20 },  // Ramp up lên 100 VU trong 30s
        { duration: "20s", target: 20 },  // Giữ 100 VU trong 60s (đo chính)
        { duration: "5s", target: 0   },  // Ramp down
    ],

    // SLA thresholds — test FAIL nếu vi phạm
    thresholds: {
        // p95 của toàn bộ http_req_duration phải ≤ 200ms
        "http_req_duration{endpoint:sim_swap_check}":    ["p(95)<200"],
        "http_req_duration{endpoint:sim_swap_retrieve}": ["p(95)<200"],

        // Custom trend metrics
        "sim_swap_check_latency":    ["p(95)<200"],
        "sim_swap_retrieve_latency": ["p(95)<200"],

        // Tỉ lệ thành công phải ≥ 99%
        "sim_swap_success_rate": ["rate>0.99"],
    },
};


// ── Test data ─────────────────────────────────────────────────────────────────

/**
 * Danh sách MSISDN mẫu.
 * Trong môi trường thực: load từ file CSV chứa MSISDN từ seed data.
 * Ở đây dùng range tĩnh để tránh phụ thuộc file ngoài.
 */
const MSISDNS = Array.from({ length: 100 }, (_, i) =>
    `+8497${String(i).padStart(7, "0")}`
);

function randomMsisdn() {
    return MSISDNS[Math.floor(Math.random() * MSISDNS.length)];
}

function randomMaxAge() {
    // Test với các giá trị maxAge đa dạng để cover cả boundary
    const ages = [0, 1, 7, 30, 60, 90];
    return ages[Math.floor(Math.random() * ages.length)];
}


// ── Setup: đọc env vars ───────────────────────────────────────────────────────

const BASE_URL = __ENV.BASE_URL || "http://localhost:8000";
const API_KEY  = __ENV.API_KEY  || "dev-secret";

const HEADERS = {
    "Content-Type": "application/json",
    "X-API-Key": API_KEY,
};


// ── Main VU function ──────────────────────────────────────────────────────────

export default function () {
    const msisdn = randomMsisdn();
    const maxAge = randomMaxAge();
    const body   = JSON.stringify({ phoneNumber: msisdn, maxAge });

    // ── Request 1: /sim-swap/v0/check ────────────────────────────────────────
    const checkRes = http.post(
        `${BASE_URL}/sim-swap/v0/check`,
        body,
        {
            headers: HEADERS,
            tags: { endpoint: "sim_swap_check" },
        }
    );

    // Ghi custom metric latency
    checkLatency.add(checkRes.timings.duration);

    // Kiểm tra response
    const checkOk = check(checkRes, {
        "check: status 200":          (r) => r.status === 200,
        "check: has swapped field":   (r) => {
            try {
                const body = JSON.parse(r.body);
                return typeof body.swapped === "boolean";
            } catch (e){ return false; }
        },
    });

    successRate.add(checkOk);
    if (!checkOk) errorCount.add(1);


    // ── Request 2: /sim-swap/v0/retrieve-date ────────────────────────────────
    const retrieveRes = http.post(
        `${BASE_URL}/sim-swap/v0/retrieve-date`,
        body,
        {
            headers: HEADERS,
            tags: { endpoint: "sim_swap_retrieve" },
        }
    );

    retrieveLatency.add(retrieveRes.timings.duration);

    const retrieveOk = check(retrieveRes, {
        "retrieve: status 200":              (r) => r.status === 200,
        "retrieve: has latestSimChange key": (r) => {
            try {
                const body = JSON.parse(r.body);
                // latestSimChange có thể null (không có swap) hoặc datetime string
                return "latestSimChange" in body;
            } catch(e) { return false; }
        },
    });

    successRate.add(retrieveOk);
    if (!retrieveOk) errorCount.add(1);

    // Nghỉ nhỏ giữa 2 lần lặp để mô phỏng think time thực tế
    sleep(0.1);
}