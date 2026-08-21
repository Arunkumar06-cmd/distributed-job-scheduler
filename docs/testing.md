# Testing Strategy

## Unit (domain)

```bash
cargo test -p domain
```
- `retry::tests`: fixed (10,10,10), linear (10,20,30), exponential (5,10,20,40), max_delay cap.
- `schedule::tests`: hourly cron (`0 * * * * *`), reject bad cron/tz, Asia/Kolkata daily.
- `job::validate_transition`: `Scheduled->Queued`, `Queued->Claimed`, etc., rejects invalid like `Completed->Queued`.

## Integration (DB)

Run against the Compose PostgreSQL service:

```bash
DATABASE_URL=postgres://postgres:postgres@127.0.0.1:5432/job_scheduler cargo test -p db -- --test-threads=1
```

Manual SQL tests (also in `psql`):

- **Idempotency**: `INSERT jobs (queue_id, idempotency_key) ...` -> second with same key -> `unique_violation`.
- **Concurrency**: Two workers `SELECT ... FOR UPDATE NOWAIT` on same queue -> second gets `could not obtain lock` -> NAK with delay, `RUNNING` never exceeds `max_concurrency` (verified by flooding 20 sleep jobs with concurrency 3, polling `stats`).
- **Lease fencing**: Worker A claim `epoch=1`, worker B claim after `lease_expires_at` -> `epoch=2`, A's `UPDATE ... WHERE epoch=1` affects 0 rows.
- **Cron dedup**: Two schedulers `INSERT scheduled_occurrences (job_id, fire_time) ON CONFLICT DO NOTHING` -> one succeeds, other `0 rows`, only one job created.

## API lifecycle validation

The manual validation workflow covers:
- Auth register/login, org/project/queue CRUD
- Immediate job `Queueds->Completed` (2s)
- Delayed job `Scheduled->Queued` after `scheduled_for`
- Batch 5 jobs -> `batches` table + 5 jobs
- Always_fail 3x -> `RETRY_WAIT` with exponential delay -> `FAILED` + DLQ
- Pause/resume: `is_paused=true` -> job stays Queued, resume -> completes
- Concurrency: flood 10 sleep 3s with concurrency 3 -> `running <=3` invariant holds for 5 polls
- Cron: `* * * * * *` -> at least 1 occurrence in 8s
- Pagination: `page_size=2` returns 2, `total` correct

Run it against a clean Compose environment and record the resulting job states
through the dashboard, API, and `psql` counts.

## Chaos

**Worker crash**: `kill -9 worker` mid `sleep 10` job -> NATS AckWait (60s) expires -> redelivery -> new worker claims `epoch=2` -> old worker's `complete` fenced (0 rows) -> new worker completes. Verified by `lease_epoch` increment and `job_executions` shows `ABANDONED` + `COMPLETED`.

**Relay crash**: `kill relay` after `claim` but before `DELETE` -> `relay_locked_until` expires (30s) -> another relay reclaims same `Nats-Msg-Id` -> JetStream dedup suppresses duplicate within 2m, but even if delivered, handler idempotent.

**API crash**: `kill -9 api` during `BEGIN` -> `ROLLBACK` -> no job/outbox (verified by counts).

**DB outage**: `psql` down -> worker `renew_lease` fails, logs error, does not falsely mark `COMPLETED`.

## Observability & Manual

- `GET /metrics` -> `queued, running, completed, failed, dlq, pool_size`
- `GET /queues/:id/stats` per queue
- `GET /workers` -> `ONLINE/STALE/OFFLINE` based on `NOW - last_heartbeat`
- `EXPLAIN (ANALYZE, BUFFERS) SELECT ... WHERE status='QUEUED' ORDER BY priority` -> uses `idx_jobs_queued`
- Frontend: queue health cards, worker table, job explorer with filters, execution timeline, logs, and pause/resume controls.

## Load

The repository includes a controlled k6 admission-load test:
```
k6 run bench/k6.js
```

It ramps from 10 to 100 virtual users and requires more than 99% `202 Accepted`
responses. The 2026-08-21 local run completed 11,100 accepted submissions at
219 jobs/s, with p95 166 ms and p99 below 200 ms.

## Bonus-feature verification

- **RBAC**: add a viewer through `POST /organizations/:id/members`; read
  endpoints return `200`, while queue creation, pause/resume, and job creation
  return `403`. Repeat with a member and admin to verify the write/admin split.
- **Rate limiting**: create a queue with `rate_limit: 2` and
  `rate_window_secs: 60`; the third submission must return `429 RateLimited`.
- **Workflow DAG**: create `A,B -> C`; verify C stays `WAITING` until both A
  and B complete, then receives an outbox event and reaches `COMPLETED`.
- **Queue sharding**: create a queue with `shard_count: 4`, submit jobs with
  stable idempotency keys, and verify `jobs.shard_id` is 0–3 and NATS subjects
  contain the corresponding `shard.{id}` token.
- **Live updates**: connect with a Bearer token to `/events/stream?project_id=`
  or `/events/ws?project_id=`; a user outside that organization receives `403`.
- **AI summaries**: configure `AI_SUMMARIES_ENABLED=true` and `OPENAI_API_KEY`,
  generate a DLQ entry, then verify `GET /dlq/:id/summary` returns a stored
  provider model result. Do not use credentials in test fixtures or logs.

## Test Evidence (2026-08-20 run)

- Full workspace suite: 12/12 tests passed against PostgreSQL 18.
- Database integration: idempotency, cron deduplication, lease fencing, and
  capacity contention passed against a live Docker PostgreSQL service.
- API lifecycle: immediate, delayed, batch, retry/DLQ, pause/resume,
  concurrency, cron, and pagination were exercised against PostgreSQL + NATS.
- External-result safety: an `external_payment` timeout remains
  `UNKNOWN_EXTERNAL_RESULT`; the scheduler never guesses the downstream result.
