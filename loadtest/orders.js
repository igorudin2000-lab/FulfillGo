import http from "k6/http";
import { check, sleep } from "k6";

export const options = {
  scenarios: {
    create_orders: {
      executor: "constant-arrival-rate",
      rate: 20,
      timeUnit: "1s",
      duration: "30s",
      preAllocatedVUs: 20,
      maxVUs: 100,
    },
  },
  thresholds: {
    http_req_failed: ["rate<0.01"],
    http_req_duration: ["p(95)<300"],
  },
};

const baseURL = __ENV.BASE_URL || "http://localhost:8080";

export default function () {
  const idempotencyKey = `k6-${__VU}-${__ITER}-${Date.now()}`;
  const response = http.post(
    `${baseURL}/v1/orders`,
    JSON.stringify({
      customer_id: `load-customer-${__VU}`,
      items: [{ sku: "CASE-001", quantity: 1 }],
    }),
    {
      headers: {
        "Content-Type": "application/json",
        "Idempotency-Key": idempotencyKey,
      },
    },
  );

  check(response, {
    "order created": (result) => result.status === 201,
    "response has id": (result) => Boolean(result.json("id")),
  });
  sleep(0.1);
}

