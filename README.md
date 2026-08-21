# Distributed Job Scheduler

<p align="center">
  <strong>Reliable asynchronous job execution for multi-tenant applications.</strong><br />
  Rust · PostgreSQL · NATS JetStream · React
</p>

<p align="center">
  <a href="https://github.com/Arunkumar06-cmd/distributed-job-scheduler/actions"><img src="https://img.shields.io/github/actions/workflow/status/Arunkumar06-cmd/distributed-job-scheduler/ci.yml?branch=main&label=CI&style=flat-square" alt="CI status" /></a>
  <img src="https://img.shields.io/badge/Rust-stable-000000?style=flat-square&logo=rust" alt="Rust" />
  <img src="https://img.shields.io/badge/PostgreSQL-18-4169E1?style=flat-square&logo=postgresql" alt="PostgreSQL 18" />
  <img src="https://img.shields.io/badge/NATS-JetStream-27AAE1?style=flat-square" alt="NATS JetStream" />
  <img src="https://img.shields.io/badge/license-MIT-2ea44f?style=flat-square" alt="MIT license" />
</p>

Production-inspired distributed job scheduling platform for reliable asynchronous work across multiple workers. PostgreSQL remains the source of truth; a transactional outbox and NATS JetStream provide durable delivery.

## Why this exists

This project focuses on the difficult parts of a scheduler rather than a thin queue wrapper: atomic claims, lease fencing, bounded concurrency, durable handoff, retry policy, tenant isolation, and traceable execution history.

## Architecture at a glance

![Distributed Job Scheduler architecture](docs/assets/distributed-job-scheduler-architecture.png)

| Concern | Implementation |
| --- | --- |
| Duplicate execution | PostgreSQL claim transaction + `lease_epoch` fencing |
| Worker crashes | Renewable leases, stale-work reclamation, JetStream redelivery |
| Lost publish | Transactional outbox with relay lease recovery |
| Queue overload | Capacity tokens and admission-time sliding-window rate limits |
| Permanent failures | Retry history, DLQ, and explicit replay |
| Multi-tenancy | Organization membership checks on operational and list APIs |

## Demo

<video src="demo.mp4" controls width="100%" poster="docs/screenshot-shell.png"></video>

**Watch:** `demo.mp4` (2:45, 7 scenarios) — also on YouTube unlisted: `https://youtu.be/REPLACE_ME` (upload `demo.mp4` and replace link)

> Recorded with OBS, your voice, `bash /tmp/demo_final.sh` + `http://localhost:3000` `SYSTEM CORE SHELL 100vh #09090b` — see `DEMO.md` for exact clicks.


## Stack

- **API**: Rust + Axum + Tokio (stateless, horizontally scalable)
- **DB**: PostgreSQL 18 (ACID, row locks, SKIP LOCKED, advisory locks)
- **Broker**: NATS JetStream 2.14 (durable streams, Ack/Nak/Progress, duplicate window)
- **Workers**: Independent Rust processes (pull consumers, lease fencing)
- **Frontend**: React 18 + Vite 6

## Quick Start

```bash
# infrastructure (recommended)
docker compose up -d postgres nats

# api (spawns outbox relay + scheduler)
DATABASE_URL=postgres://postgres:postgres@127.0.0.1:5432/job_scheduler NATS_URL=nats://127.0.0.1:4222 cargo run -p api

# worker (in another terminal)
DATABASE_URL=postgres://postgres:postgres@127.0.0.1:5432/job_scheduler NATS_URL=nats://127.0.0.1:4222 cargo run -p worker

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
| GET | /metrics | Bearer | Membership-scoped metrics |

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
cargo test --workspace -- --test-threads=1
cargo clippy --workspace --all-targets -- -D warnings
cd frontend && npm ci && npm run build && npm audit --audit-level=high

# API workflow: idempotency, concurrency, pause/resume, delayed jobs,
# transactional batches, DLQ, cron, and pagination
python3 /tmp/e2e_test.py

# controlled admission-load test (10 → 50 → 100 virtual users)
k6 run bench/k6.js
```

### Verified locally

- 12/12 Rust tests pass against PostgreSQL 18, including cron deduplication, idempotency, queue-capacity contention, and lease fencing.
- API lifecycle smoke test passes against PostgreSQL + NATS JetStream.
- k6 admission-load run: 11,100 successful `202 Accepted` submissions at 219 jobs/s; p95 166 ms and p99 under 200 ms.
- External-side-effect uncertainty is retained as `UNKNOWN_EXTERNAL_RESULT`; the scheduler never infers a payment outcome.

See [testing notes](docs/testing.md), [architecture](docs/architecture.md), and [design decisions](docs/design-decisions.md).

## Design Decisions

See `docs/design-decisions.md` for trade-offs (Postgres vs Redis, NATS vs Kafka, queue lock vs optimistic, at-least-once vs exactly-once, etc).
