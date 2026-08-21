# Demo Guide — 7 Scenarios for Grader

<video src="../demo.mp4" controls width="100%"></video>

> `demo.mp4` 2:45 — same 7 scenarios below, recorded with OBS. Upload to YouTube unlisted and replace `../demo.mp4` link with `https://youtu.be/...` for GitHub preview.

## Prerequisites

```bash
nats-server -js -sd /tmp/nats-js -p 4222 -m 8222 &
DATABASE_URL=postgres:///job_scheduler cargo run -p api    # :8080 + outbox + scheduler
DATABASE_URL=postgres:///job_scheduler cargo run -p worker # :worker
cd frontend && npm run dev  # :3000
```

Seed: `POST /auth/register {"email":"admin@example.com","password":"password123","display_name":"Admin"}`

## Demo 1: Create Job (API → Postgres → Outbox → NATS → Worker)

```bash
TOKEN=$(curl -s :8080/auth/login -d '{"email":"admin@example.com","password":"password123"}' | jq -r .token)
ORG=$(curl -s :8080/organizations -H "Authorization: Bearer $TOKEN" | jq -r .[0].id)
PROJ=$(curl -s :8080/projects?org_id=$ORG -H "Authorization: Bearer $TOKEN" | jq -r .[0].id)
Q=$(curl -s :8080/queues?project_id=$PROJ -H "Authorization: Bearer $TOKEN" | jq -r .[0].id)
curl -s :8080/jobs -H "Authorization: Bearer $TOKEN" -H "Idempotency-Key: demo1" -d '{"queue_id":"'$Q'","payload":{"type":"echo","data":"hello"}}' | jq
# -> 202 Queued, then GET /jobs/:id -> Completed in <2s, execution history 1 attempt
```

## Demo 2: Pause Queue

```bash
curl -s :8080/queues/$Q/pause -H "Authorization: Bearer $TOKEN" -X POST | jq .is_paused # true
curl -s :8080/jobs -H "Authorization: Bearer $TOKEN" -d '{"queue_id":"'$Q'","payload":{"type":"echo"}}' | jq .status # Queued, stays Queued
# worker logs: "queue paused; NAK with delay"
curl -s :8080/queues/$Q/resume -H "Authorization: Bearer $TOKEN" -X POST
# -> job completes
```

## Demo 3: Concurrency 3, Flood 20

```bash
for i in {1..20}; do curl -s :8080/jobs -H "Authorization: Bearer $TOKEN" -d '{"queue_id":"'$Q'","payload":{"type":"sleep","secs":2}}' & done
watch -n1 'curl -s :8080/queues/$Q/stats -H "Authorization: Bearer $TOKEN" | jq .running' # never >3
```

## Demo 4: Kill Worker (Epoch Fencing)

```bash
ps aux | grep worker | grep -v grep
kill -9 <pid>  # mid sleep 10 job
# new worker claims epoch+1, old worker's complete -> 0 rows fenced, job completes with new epoch
psql -c "SELECT lease_epoch, status FROM jobs WHERE id='<job>'"
psql -c "SELECT * FROM job_executions WHERE job_id='<job>' ORDER BY attempt" # shows ABANDONED + COMPLETED
```

## Demo 5: Duplicate Idempotency

```bash
curl -s :8080/jobs -H "Authorization: Bearer $TOKEN" -H "Idempotency-Key: same" -d '{"queue_id":"'$Q'","payload":{"type":"echo"}}' | jq
curl -s :8080/jobs -H "Authorization: Bearer $TOKEN" -H "Idempotency-Key: same" -d '{"queue_id":"'$Q'","payload":{"type":"echo"}}' | jq # 409 Conflict
psql -c "SELECT count(*) FROM jobs WHERE idempotency_key='same'" # 1
```

## Demo 6: Retry → DLQ

```bash
curl -s :8080/jobs -H "Authorization: Bearer $TOKEN" -d '{"queue_id":"'$Q'","payload":{"type":"always_fail"},"max_attempts":3,"retry_strategy":"exponential","base_delay_secs":1}' | jq
# poll GET /jobs/:id -> RETRY_WAIT (next_retry_at), then QUEUED, then FAILED
curl -s :8080/dlq?queue_id=$Q -H "Authorization: Bearer $TOKEN" | jq
curl -s :8080/dlq/<dlq_id>/replay -H "Authorization: Bearer $TOKEN" -X POST | jq # new job
```

## Demo 7: Multiple Schedulers (Cron Dedup)

```bash
# Start 2 schedulers (2 terminals):
DATABASE_URL=postgres:///job_scheduler cargo run -p scheduler
# Create cron: POST /scheduled-jobs {"queue_id":"'$Q'","name":"c","payload":{"type":"echo"},"cron_expr":"* * * * * *","timezone":"UTC"}
# Both schedulers race to create occurrence, PK (scheduled_job_id, fire_time) ensures one job
psql -c "SELECT count(*) FROM scheduled_occurrences WHERE fire_time='2026-08-20 12:00:00+00'"
psql -c "SELECT count(*) FROM jobs WHERE type='recurring' AND scheduled_for='2026-08-20 12:00:00+00'" # 1
```
