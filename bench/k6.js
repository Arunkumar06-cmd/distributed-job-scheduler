import http from 'k6/http';
import { check, sleep } from 'k6';
import { Trend, Rate } from 'k6/metrics';

export const options = {
  stages: [
    { duration: '10s', target: 10 },
    { duration: '20s', target: 50 },
    { duration: '10s', target: 100 },
    { duration: '10s', target: 0 },
  ],
  thresholds: {
    job_create_success: ['rate>0.99'],
    http_req_duration: ['p(95)<100', 'p(99)<200'],
  },
};

const claimLatency = new Trend('claim_latency');
const jobCreateSuccess = new Rate('job_create_success');
const BASE = __ENV.BASE_URL || 'http://localhost:8080';
const TOKEN = __ENV.TOKEN;

export function setup() {
  let email = `bench_${Date.now()}@test.com`;
  let res = http.post(`${BASE}/auth/register`, JSON.stringify({ email: email, password: 'password123', display_name: 'Bench' }), { headers: { 'Content-Type': 'application/json' } });
  let token = res.json().token;
  let org = http.post(`${BASE}/organizations`, JSON.stringify({ name: 'Bench Org', slug: `bench-${Date.now()}` }), { headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` } }).json();
  let proj = http.post(`${BASE}/projects`, JSON.stringify({ org_id: org.id, name: 'Bench Proj', slug: `bench-proj-${Date.now()}` }), { headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` } }).json();
  let q = http.post(`${BASE}/queues`, JSON.stringify({ project_id: proj.id, name: 'bench-queue', max_concurrency: 10 }), { headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` } }).json();
  return { token, queue_id: q.id };
}

export default function (data) {
  let payload = JSON.stringify({ queue_id: data.queue_id, payload: { type: 'echo', ts: Date.now() }, priority: Math.floor(Math.random()*100) });
  let res = http.post(`${BASE}/jobs`, payload, { headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${data.token}` } });
  jobCreateSuccess.add(res.status === 202);
  check(res, { '202': (r) => r.status === 202 });
  claimLatency.add(res.timings.duration);
  sleep(0.1);
}

export function handleSummary(data) {
  return { 'bench/summary.json': JSON.stringify(data, null, 2) };
}
