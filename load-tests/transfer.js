import http from "k6/http";
import { check, sleep } from "k6";

export const options = {
    stages: [
        { duration: "10s", target: 10 },
        { duration: "20s", target: 25 },
        { duration: "20s", target: 50 },
        { duration: "10s", target: 0 },
    ],

    thresholds: {
        http_req_failed: ["rate<0.01"],
        http_req_duration: [
            "p(95)<500",
            "p(99)<1000",
        ],
    },
};

const BASE_URL = "http://127.0.0.1:8000";

export default function () {
    const payload = JSON.stringify({
        from_account_id: "1a7f66cf-5ae9-433c-933c-ff9d0706fec8",
        to_account_id: "feb79db2-5b02-4872-b06d-1445f3b278e9",
        amount: "1.00",
    });

    const params = {
        headers: {
            "Content-Type": "application/json",
            "Idempotency-Key": `load-${__VU}-${__ITER}`,
        },
    };

    const response = http.post(
        `${BASE_URL}/api/v1/ledger/transfer`,
        payload,
        params
    );

    check(response, {
        "transfer returned 200": (r) => r.status === 200,
    });

    sleep(0.1);
}