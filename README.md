# Distributed Job Scheduler

<p align="center">
  <strong>A reliable, multi-tenant platform for executing asynchronous work.</strong><br />
  Built with Rust, PostgreSQL, NATS JetStream, and React.
</p>

<p align="center">
  🌐 <a href="https://jobflow-nlwj.onrender.com"><strong>Live Demo</strong></a> — no setup required, register and start submitting jobs
</p>

<p align="center">
  <a href="https://github.com/Arunkumar06-cmd/distributed-job-scheduler/actions"><img src="https://img.shields.io/github/actions/workflow/status/Arunkumar06-cmd/distributed-job-scheduler/ci.yml?branch=main&label=CI&style=flat-square" alt="CI status" /></a>
  <img src="https://img.shields.io/badge/Rust-stable-000000?style=flat-square&logo=rust" alt="Rust" />
  <img src="https://img.shields.io/badge/PostgreSQL-18-4169E1?style=flat-square&logo=postgresql" alt="PostgreSQL 18" />
  <img src="https://img.shields.io/badge/NATS-JetStream-27AAE1?style=flat-square" alt="NATS JetStream" />
  <img src="https://img.shields.io/badge/license-MIT-2ea44f?style=flat-square" alt="MIT license" />
  <a href="https://jobflow-nlwj.onrender.com"><img src="https://img.shields.io/badge/🟢_Live_Demo-jobflow--nlwj.onrender.com-00A8E8?style=flat-square" alt="Live Demo" /></a>
</p>

This is a production-inspired scheduler for applications that need more than a
background loop: durable handoff, bounded concurrency, retries, execution
history, dead-letter handling, and safe recovery after worker failures.

## The point of the project

| Durable handoff | Safe execution | Operational control |
| --- | --- | --- |
| A transactional outbox persists work before it reaches NATS. | Lease epochs fence stale workers from completing a reassigned job. | The dashboard exposes queues, workers, jobs, retries, logs, and the DLQ. |

```text
Dashboard / REST API
        │
        ▼
PostgreSQL ── transactional outbox ──► NATS JetStream ──► worker pool ──► external services
  source of truth     durable handoff       redelivery       leases + idempotency
        │
        └── queues · jobs · executions · logs · retries · DLQ · schedules
```

## Try it live

**[https://jobflow-nlwj.onrender.com](https://jobflow-nlwj.onrender.com)**

| Step | What to do |
|---|---|
| 1 | Register any email and password (≥8 chars) |
| 2 | Follow the wizard: create an organization → project → queue |
| 3 | Click **Create job**, paste `{"type": "echo"}`, submit |
| 4 | Watch it go **QUEUED → CLAIMED → RUNNING → COMPLETED** in seconds |
| 5 | Submit `{"type": "always_fail"}` with max_attempts 2 → watch retries → check the **Dead letters** tab → hit **Replay** or **✨AI** |

> Free tier spins down after 15 min idle. First request takes ~30 s to wake.

## Architecture at a glance

<p align="center">
  <img src="docs/assets/distributed-job-scheduler-architecture.png" alt="Architecture showing the React dashboard, Rust API, PostgreSQL, outbox relay, NATS JetStream, worker pool, and job lifecycle" width="100%" />
</p>

For the component-level design, data-flow narrative, and failure-recovery
paths, see the [architecture notes](docs/architecture.md).

## What is included

| Area | Capabilities |
| --- | --- |
| Tenancy | Users, organizations, projects, memberships, JWT access (1h) + rotating refresh (30d) tokens, per-user API rate limiting |
| Queues | Priority, pause/resume (PATCH or deprecated POST verbs), concurrency limits, retry policies, admission rate limits, statistics, sharding (1–128) |
| Jobs | Immediate, delayed, scheduled, recurring, batch, manual retry, idempotency keys, pagination, filtering by status/priority/batch/worker |
| Reliability | Atomic claims with epoch fencing, capacity tokens, lease renewal, stale-lease reaping, UNKNOWN-external-result reconciler, outbox relay with poison-pill backoff, scheduler leader election + NOTIFY wake, archive lifecycle |
| Operations | Worker heartbeats with outcome counters, execution attempts ledger, structured JSON logs, audit trail for privileged mutations, Prometheus metrics incl. duration histograms, dead-letter replay, AI failure summaries (any OpenAI-compatible endpoint) |
| Extensions | Workflow DAG dependencies with cycle rejection, scoped SSE/WebSocket snapshots, event-driven worker discovery, deterministic sharding, hot/cold job archival |

## Run locally

Prerequisites: Docker, Rust stable, and Node.js 22 or later.

```bash
# 1. Start PostgreSQL and NATS JetStream.
docker compose up -d postgres nats

# 2. Start the API. It starts the outbox relay and scheduler.
DATABASE_URL=postgres://postgres:postgres@127.0.0.1:5432/job_scheduler \
NATS_URL=nats://127.0.0.1:4222 \
cargo run -p api

# 3. In another terminal, start a worker.
DATABASE_URL=postgres://postgres:postgres@127.0.0.1:5432/job_scheduler \
NATS_URL=nats://127.0.0.1:4222 \
cargo run -p worker

# 4. In a third terminal, start the dashboard.
cd frontend && npm ci && npm run dev
```

Open `http://localhost:3000`, register a user, create an organization and a
project, then create a queue. The first created user owns the organization and
project it creates.

If PostgreSQL and NATS JetStream already run on your development machine, the
application can be started without Compose. Use a fresh local database to keep
its migration history isolated, then point each process at localhost:

```bash
createdb job_scheduler_local
nats-server -js -sd /tmp/job-scheduler-nats -p 4222 -m 8222

DATABASE_URL=postgres://$USER@127.0.0.1:5432/job_scheduler_local \
NATS_URL=nats://127.0.0.1:4222 \
JWT_SECRET=dev-secret-change-in-production-please-32bytes \
cargo run -p api

DATABASE_URL=postgres://$USER@127.0.0.1:5432/job_scheduler_local \
NATS_URL=nats://127.0.0.1:4222 \
cargo run -p worker
```

Run `npm run dev` from `frontend/` in a separate terminal. Do not reuse a
database whose SQLx migration history belongs to an older checkout: the API
correctly stops when an applied migration checksum no longer matches.

## Reliability model

The system deliberately provides **at-least-once delivery**. External handlers
must use `job_id` as their idempotency key; a distributed scheduler cannot make
an arbitrary third-party side effect exactly once.

| Failure mode | Protection |
| --- | --- |
| API commits but publish fails | The outbox relay retries the persisted event. |
| Relay publishes then crashes | A lease-expired relay retries with the same `Nats-Msg-Id`. |
| Worker crashes mid-job | Lease expiry and JetStream redelivery allow another worker to claim it. |
| Stale worker finishes late | `lease_epoch` fencing rejects its completion update. |
| Queue reaches capacity | An atomic capacity-token reservation prevents over-claiming. |
| Retries are exhausted | The job is retained in the DLQ and can be deliberately replayed. |

```text
SCHEDULED ──► QUEUED ──► CLAIMED ──► RUNNING ──► COMPLETED
    ▲                                │              │
    │                                ├─► RETRY_WAIT ─┤──► QUEUED (backoff)
    └── promotion                    ├─► UNKNOWN_EXTERNAL_RESULT ──► reconciler (dlq/retry/complete)
   (scheduler tick or                ├─► FAILED ──► DLQ              │
    NOTIFY wake)                     └─► CANCELLED                   └── replay as new job

WAITING (workflow DAG) ──► QUEUED when all parent edges are satisfied
```

Terminal states are `COMPLETED`, `FAILED`, and `CANCELLED`. Every transition is
validated against the state machine in `domain::job::validate_transition`; the
reconciler resolves jobs stuck in `UNKNOWN_EXTERNAL_RESULT` after a configurable
grace period (`UNKNOWN_GRACE_SECS`, default 900 s).

## Verification

```bash
# Rust unit + PostgreSQL integration tests (per-test throwaway databases)
DATABASE_URL=postgres://… cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings

# Full-stack pipeline e2e (needs Postgres + JetStream-enabled NATS)
cargo test -p worker --test pipeline_e2e -- --include-ignored
# Multi-worker race harness: 3 replicas × 40 jobs, zero double-execution proof
cargo test -p worker --test race_harness -- --include-ignored
# Archive throughput benchmark (~4,400 rows/s on laptop Postgres)
cargo test -p db --test archive_bench -- --include-ignored --nocapture

# Frontend unit tests + browser e2e + accessibility scans + visual baselines
cd frontend && npm run test && npx playwright test

# Controlled admission-load test (10 → 50 → 100 virtual users)
k6 run bench/k6.js
```

See [docs/testing.md](docs/testing.md) for the full layer map, CI topology,
and design decisions behind the isolation model.

Latest local evidence:

- **66 Rust tests** passed against live PostgreSQL 18: state-machine matrix,
  overflow-safe retry math, atomic claim fencing under 8-way contention,
  capacity-token limits across 12-way races, cron occurrence deduplication,
  DLQ replay inheritance, UNKNOWN reconciliation policies, subject-tier
  correctness, heartbeat pruning, audit writes.
- **Pipeline e2e**: outbox row → JetStream publish → durable consumer claim →
  handler execution → COMPLETED with stored result; failure path lands in DLQ.
- **Race harness**: 3 replicas × 40 jobs in ~32 s with zero double-execution.
- **13 Playwright browser e2e** including axe accessibility scans asserting
  zero critical/serious violations, plus 4 visual-regression baselines.
- **51 adversarial payload attacks** repelled with correct status codes,
  standard error envelopes, and no internal-detail leakage.
- The controlled load run submitted 11,100 jobs at 219/s; p95 166 ms on the
  development environment.

These figures are development-environment evidence, not a production capacity
guarantee.

## Bonus features

All eight are implemented and tested:

- **Workflow dependencies:** create DAGs of jobs; children wait until all
  parents complete. Cycles rejected at creation (app-level Kahn's check +
  database trigger).
- **Rate limiting:** per-user fixed-window limiter on every authenticated route
  (`API_RATE_LIMIT_PER_MIN`, 0 disables) and per-queue admission limits.
- **Distributed locking:** advisory-lock leader election ensures a single
  active scheduler; crash-safe (session-scoped lock auto-releases).
- **Queue sharding:** set `shard_count` (1–128) at creation; jobs are routed
  deterministically by FNV hash of their routing key.
- **Event-driven execution:** transactional outbox + NOTIFY wake means job
  promotion latency is bounded by the relay, not the poll interval.
- **WebSocket + SSE live updates:** authenticated, project-scoped snapshot
  streams at `/events/ws` and `/events/stream?project_id=…`.
- **RBAC:** owner/admin manage configuration and members; members submit work;
  viewers are read-only. Enforced per-route and audited.
- **AI failure summaries:** works with any OpenAI-compatible chat/completions
  endpoint (OpenAI, NVIDIA NIM, vLLM) via `AI_LLM_BASE_URL`,
  `OPENAI_API_KEY`, `OPENAI_MODEL`, and optional
  `AI_MODEL_FALLBACKS=m1,m2`. Set `AI_SUMMARIES_ENABLED=true`; entries appear
  in the DLQ panel's ✨AI viewer ~10 s after dead-lettering.

## API versioning

The stable contract is mounted at **`/api/v1/*`**. Unversioned aliases exist
for legacy clients but are frozen — new integrations must use the prefix.
Verb policy: `PATCH` mutates stored state; POST verb-subresources
(`/jobs/:id/retry`, `/dlq/:id/replay`) are actions only. Superseded endpoints
emit RFC-8594 `Deprecation`/`Sunset` headers (see `POST /queues/:id/pause`).

## Documentation

| Document | Purpose |
| --- | --- |
| [Architecture](docs/architecture.md) | Components, message flow, failure recovery, deployment model |
| [Data model](docs/er-diagram.md) | Rendered ER diagram, keys, relationships, and query indexes |
| [API reference](docs/api.md) | REST routes, authorization, errors, and idempotency |
| [OpenAPI](docs/openapi.json) | Machine-readable API surface |
| [Testing](docs/testing.md) | Test layers, chaos scenarios, and evidence |
| [Design decisions](docs/design-decisions.md) | Trade-offs and intentionally deferred work |
| [Contributing](CONTRIBUTING.md) | Local development and contribution conventions |
| [Security](SECURITY.md) | Vulnerability reporting policy |

## License

Released under the [MIT License](LICENSE).
