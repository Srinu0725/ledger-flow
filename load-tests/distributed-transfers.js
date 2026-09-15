import http from "k6/http";
import { check, sleep } from "k6";
import { uuidv4 } from "https://jslib.k6.io/k6-utils/1.4.0/index.js";

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
            "p(95)<1000",
            "p(99)<2000",
        ],
    },
};

const BASE_URL = "http://127.0.0.1:8000";

const accounts = [
    {
        from: "0138be79-1eb9-470a-a701-d1c2557b30a4",
        to: "413f5f45-2c68-4ee2-9762-4c2cbb6c88e9",
    },
    {
        from: "7c54926b-2fb0-461d-9e9f-401f81d1a44b",
        to: "d8d6619f-9728-41f1-8337-0d2d1c7fd7f9",
    },
    {
        from: "f4706481-5da8-4d2b-b548-f3016271379e",
        to: "3575d4b1-3f75-4318-b063-18c4508d27e9",
    },
    {
        from: "95d64049-64ac-4eeb-b21b-878018ad7567",
        to: "a7bae015-e01e-46c0-9076-71303ab61799",
    },
    {
        from: "7233f3fb-455a-410d-a871-bff74681ae79",
        to: "fcaacae4-c4b2-431c-b8a3-d9937f331db8",
    },
];

// Unique identifier for this entire k6 test run.
const RUN_ID = uuidv4();

export default function () {
    // Keep each VU assigned to the same account pair.
    const account =
        accounts[(__VU - 1) % accounts.length];

    /*
     * Alternate transfer direction on every iteration.
     *
     * Iteration 0: A -> B
     * Iteration 1: B -> A
     * Iteration 2: A -> B
     * Iteration 3: B -> A
     *
     * This prevents the load test from continuously
     * draining one side of an account pair.
     */
    const reverse = __ITER % 2 === 1;

    const from = reverse
        ? account.to
        : account.from;

    const to = reverse
        ? account.from
        : account.to;

    const payload = JSON.stringify({
        from_account_id: from,
        to_account_id: to,
        amount: "1.00",
    });

    /*
     * Include a unique RUN_ID so that every execution
     * of the load test gets fresh idempotency keys.
     *
     * __VU and __ITER reset when k6 is started again,
     * so they cannot be used alone across multiple runs.
     */
    const params = {
        headers: {
            "Content-Type": "application/json",
            "Idempotency-Key":
                `load-${RUN_ID}-${__VU}-${__ITER}`,
        },
    };

    const response = http.post(
        `${BASE_URL}/api/v1/ledger/transfer`,
        payload,
        params
    );

    check(response, {
        "transfer returned 200": (r) => {
            if (r.status !== 200) {
                console.log(
                    `Transfer failed: status=${r.status}, body=${r.body}`
                );
            }

            return r.status === 200;
        },
    });

    sleep(0.1);
}