# Distributed Job Scheduler

Production-inspired distributed job scheduling platform: reliable async background jobs across multiple workers with PostgreSQL as source of truth and NATS JetStream for durable delivery.

## Demo

<video src="demo.mp4" controls width="100%" poster="docs/screenshot-shell.png"></video>

**Watch:** `demo.mp4` (2:45, 7 scenarios) — also on YouTube unlisted: `https://youtu.be/REPLACE_ME` (upload `demo.mp4` and replace link)

> Recorded with OBS, your voice, `bash /tmp/demo_final.sh` + `http://localhost:3000` `SYSTEM CORE SHELL 100vh #09090b` — see `DEMO.md` for exact clicks.


## Stack

- **API**: Rust + Axum + Tokio (stateless, horizontally scalable)
- **DB**: PostgreSQL 18 (ACID, row locks, SKIP LOCKED, advisory locks)
- **Broker**: NATS JetStream 2.14 (durable streams, Ack/Nak/Progress, duplicate window)
- **Workers**: Independent Rust processes (pull consumers, lease fencing)
- **Frontend**: React 18 + Vite 5

## Quick Start

```bash
# infra
brew install postgresql@18 nats-server
nats-server -js -sd /tmp/nats-js -p 4222 -m 8222 &
createdb job_scheduler

# api (spawns outbox relay + scheduler)
DATABASE_URL=postgres:///job_scheduler NATS_URL=nats://127.0.0.1:4222 cargo run -p api

# worker (in another terminal)
DATABASE_URL=postgres:///job_scheduler cargo run -p worker

# frontend
cd frontend && npm install && npm run dev  # http://localhost:3000 -> proxies to :8080
```

Seed admin: `admin@example.com / password123` via `/auth/register`.

## Architecture

```
Browser -> LB -> API (stateless) -> PostgreSQL (truth) -> Outbox -> NATS JetStream -> Workers -> External Services
                      |                         |                |-> Scheduler (advisory lock) -> Cron
                      |                         -> DLQ, Executions, Logs
```

**Invariants**
1. Stale worker cannot complete (`lease_epoch` fencing)
2. `RUNNING <= max_concurrency` (queue row `FOR UPDATE NOWAIT` serialization)
3. Committed job eventually dispatched (transactional outbox)
4. `RETRY_WAIT` not executable
5. Cron occurrence exactly once (PK `scheduled_job_id, fire_time`)
6. At-least-once delivery + idempotent handlers = effective exactly-once business effect

See `docs/architecture.md` and `docs/er-diagram.md`.

## API

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| POST | /auth/register | - | Register |
| POST | /auth/login | - | Login -> JWT |
| POST | /organizations | Bearer | Create org |
| GET | /organizations | Bearer | List orgs |
| POST | /projects | Bearer | Create project (org_id) |
| GET | /projects?org_id= | Bearer | List |
| POST | /queues | Bearer | Create queue (project_id, max_concurrency) |
| PATCH | /queues/:id | Bearer | Update |
| POST | /queues/:id/pause | Bearer | Pause |
| POST | /queues/:id/resume | Bearer | Resume |
| GET | /queues/:id/stats | Bearer | Stats |
| POST | /jobs | Bearer | Create (Idempotency-Key header) |
| GET | /jobs?queue_id=&status=&page= | Bearer | List with pagination |
| POST | /jobs/:id/retry | Bearer | Manual retry |
| POST | /jobs/batch | Bearer | Batch (1..1000) |
| POST | /scheduled-jobs | Bearer | Cron `cron_expr + timezone` or `run_once_at` |
| GET | /workers | Bearer | Workers with ONLINE/STALE/OFFLINE |
| GET | /jobs/:id/executions | Bearer | Execution history |
| GET | /jobs/:id/logs | Bearer | Structured logs |
| GET | /dlq?queue_id= | Bearer | DLQ |
| POST | /dlq/:id/replay | Bearer | Replay DLQ |
| GET | /health | - | Health |
| GET | /metrics | - | Global metrics |
| GET | /events/stream | Bearer | SSE live updates |

**Idempotency**: `Idempotency-Key` header or body field -> `UNIQUE(queue_id, idempotency_key)` -> `409 Conflict` on duplicate. NATS publish uses `Nats-Msg-Id = outbox.id` for broker dedup. Business handlers must be idempotent (`job_id` as external key).

See `docs/api.md`.

## Job Lifecycle

```
SCHEDULED -> QUEUED -> CLAIMED -> RUNNING -> COMPLETED
                              |-> RETRY_WAIT -> QUEUED (via requeue after next_retry_at)
                              |-> FAILED -> DLQ (max_attempts exceeded)
```

Retry strategies: `fixed`, `linear`, `exponential` with capped delay.

## Concurrency & Reliability

- **Outbox**: `BEGIN; INSERT job; INSERT outbox; COMMIT` -> relay claims `FOR UPDATE SKIP LOCKED` with `relay_locked_until`, publishes outside TX, then `DELETE`. Lease expires -> reclaimed.
- **Claim**: `SELECT queue FOR UPDATE NOWAIT` -> check `is_paused` -> `COUNT RUNNING < max_concurrency` -> `UPDATE job SET status=CLAIMED, lease_epoch+1, lease_owner, lease_expires_at`. All in one TX. Long task runs after commit, with background `lease_renewer` and `InProgress` (via AckKind::Progress).
- **Complete**: `UPDATE jobs SET status=COMPLETED WHERE lease_epoch = $epoch AND lease_owner = $worker` -> `0 rows` means fenced.
- **Scheduler**: `pg_try_advisory_lock(0x73636865)` leader election; cron dedup via `scheduled_occurrences` PK; timezone aware.
- **Graceful shutdown**: `SIGTERM -> stop accepting -> wait grace (30s) -> mark worker Offline`.

## Frontend

Dashboard at `http://localhost:3000`:
- Queue cards with health (queued/running/completed/failed/retry/DLQ), pause/resume, concurrency
- Worker table with ONLINE/STALE/OFFLINE (15s/60s thresholds), heartbeat, running jobs
- Job explorer with filters (status, queue, priority), pagination, retry, batch
- Job detail: payload, executions, logs, retry schedule
- Metrics: throughput, pool usage, NATS status
- Live updates via SSE + polling (3s jobs, 5s workers)

## Tests

```bash
cargo test  # unit: retry, cron, state machine
python3 /tmp/e2e_test.py  # e2e: idempotency, concurrency, pause/resume, delayed, batch, DLQ, cron, pagination
# chaos (manual):
# - kill worker mid-job -> redelivery + epoch fence verified (Job stays COMPLETED, not double-executed)
# - kill relay mid-publish -> outbox reclaimed after lease
```

See `docs/testing.md`.

## Design Decisions

See `docs/design-decisions.md` for trade-offs (Postgres vs Redis, NATS vs Kafka, queue lock vs optimistic, at-least-once vs exactly-once, etc).
