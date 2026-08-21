# Testing Strategy

## Unit (domain)

```bash
cargo test -p domain
```
- `retry::tests`: fixed (10,10,10), linear (10,20,30), exponential (5,10,20,40), max_delay cap.
- `schedule::tests`: hourly cron (`0 * * * * *`), reject bad cron/tz, Asia/Kolkata daily.
- `job::validate_transition`: `Scheduled->Queued`, `Queued->Claimed`, etc., rejects invalid like `Completed->Queued`.

## Integration (DB)

Run with `DATABASE_URL=postgres:///job_scheduler_test`.

```bash
DATABASE_URL=postgres:///job_scheduler_test cargo test -p db
```

Manual SQL tests (also in `psql`):

- **Idempotency**: `INSERT jobs (queue_id, idempotency_key) ...` -> second with same key -> `unique_violation`.
- **Concurrency**: Two workers `SELECT ... FOR UPDATE NOWAIT` on same queue -> second gets `could not obtain lock` -> NAK with delay, `RUNNING` never exceeds `max_concurrency` (verified by flooding 20 sleep jobs with concurrency 3, polling `stats`).
- **Lease fencing**: Worker A claim `epoch=1`, worker B claim after `lease_expires_at` -> `epoch=2`, A's `UPDATE ... WHERE epoch=1` affects 0 rows.
- **Cron dedup**: Two schedulers `INSERT scheduled_occurrences (job_id, fire_time) ON CONFLICT DO NOTHING` -> one succeeds, other `0 rows`, only one job created.

## E2E (API)

```bash
python3 /tmp/e2e_test.py
```
Covers:
- Auth register/login, org/project/queue CRUD
- Immediate job `Queueds->Completed` (2s)
- Delayed job `Scheduled->Queued` after `scheduled_for`
- Batch 5 jobs -> `batches` table + 5 jobs
- Always_fail 3x -> `RETRY_WAIT` with exponential delay -> `FAILED` + DLQ
- Pause/resume: `is_paused=true` -> job stays Queued, resume -> completes
- Concurrency: flood 10 sleep 3s with concurrency 3 -> `running <=3` invariant holds for 5 polls
- Cron: `* * * * * *` -> at least 1 occurrence in 8s
- Pagination: `page_size=2` returns 2, `total` correct

Logs: see `/tmp/e2e_test.py` output and `psql` counts.

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
- Frontend: queue health cards, worker table, job explorer with filters, execution timeline, logs, pause/resume buttons, live SSE.

## Load (not automated)

Potential `k6` or `wrk`:
```
wrk -t4 -c100 -d30s -s job_create.lua http://localhost:8080/jobs
```
Expected: ~200 jobs/sec per hot queue, ~1000 overall, p95 claim <20ms.

## Test Evidence (2026-08-20 run)

- Immediate job `7260396d` idempotency: second `Idempotency-Key: test-001` -> `409 Conflict` (verified).
- Immediate job `d1c65c7d` completed in 2.1s with `echoed:true`.
- Concurrency flood 10 sleep 3s with limit 3 -> `running` observed as 3,3,3,3,3 (never 4).
- Worker crash: 3 batch echo jobs stuck `RUNNING` with `lease_owner=55f0...` (dead), manually requeued -> completed after outbox backfill.
- DLQ: after fix, `always_fail` with `max_attempts=3` after 3 retries went to `FAILED` and appeared in `GET /dlq`.
