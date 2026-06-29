import http from 'k6/http';
import { check } from 'k6';
import { SharedArray } from 'k6/data';

// 1. Nhận API Key từ biến môi trường của k6
const API_KEY = __ENV.API_KEY || 'default_test_key';
const BASE_URL = 'http://localhost:8000';

// 2. Định nghĩa SLOs (p95 latency)
export const options = {
    thresholds: {
        'http_req_duration{endpoint:sim-swap}': ['p(95)<200'],
        'http_req_duration{endpoint:device-swap}': ['p(95)<200'],
        'http_req_duration{endpoint:number-verify}': ['p(95)<100'],
    },
};

export default function () {
    const headers = { 
        'Content-Type': 'application/json',
        'X-API-Key': API_KEY 
    };

    // Payload mẫu (có thể thay đổi tùy thuộc vào dữ liệu trong DB)
    const payload = JSON.stringify({ phoneNumber: '+84901234567', maxAge: 30 });

    // 3. Test các endpoints
    // SIM Swap
    const resSim = http.post(`${BASE_URL}/sim-swap/v0/check`, payload, { 
        headers, tags: { endpoint: 'sim-swap' } 
    });
    check(resSim, { 'sim-swap status 200': (r) => r.status === 200 });

    // Device Swap
    const resDev = http.post(`${BASE_URL}/device-swap/v0/check`, payload, { 
        headers, tags: { endpoint: 'device-swap' } 
    });
    check(resDev, { 'device-swap status 200': (r) => r.status === 200 });

    // Number Verification
    const resNum = http.post(`${BASE_URL}/number-verification/v0/verify`, 
        JSON.stringify({ phoneNumber: '+84901234567' }), { headers, tags: { endpoint: 'number-verify' } });
    check(resNum, { 'number-verify status 200': (r) => r.status === 200 });
}