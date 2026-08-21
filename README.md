# Distributed Job Scheduler

<p align="center">
  <strong>A reliable, multi-tenant platform for executing asynchronous work.</strong><br />
  Built with Rust, PostgreSQL, NATS JetStream, and React.
</p>

<p align="center">
  <a href="https://github.com/Arunkumar06-cmd/distributed-job-scheduler/actions"><img src="https://img.shields.io/github/actions/workflow/status/Arunkumar06-cmd/distributed-job-scheduler/ci.yml?branch=main&label=CI&style=flat-square" alt="CI status" /></a>
  <img src="https://img.shields.io/badge/Rust-stable-000000?style=flat-square&logo=rust" alt="Rust" />
  <img src="https://img.shields.io/badge/PostgreSQL-18-4169E1?style=flat-square&logo=postgresql" alt="PostgreSQL 18" />
  <img src="https://img.shields.io/badge/NATS-JetStream-27AAE1?style=flat-square" alt="NATS JetStream" />
  <img src="https://img.shields.io/badge/license-MIT-2ea44f?style=flat-square" alt="MIT license" />
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

## Architecture at a glance

<p align="center">
  <img src="docs/assets/distributed-job-scheduler-architecture.png" alt="Architecture showing the React dashboard, Rust API, PostgreSQL, outbox relay, NATS JetStream, worker pool, and job lifecycle" width="100%" />
</p>

For the component-level design, data-flow narrative, and failure-recovery
paths, see the [architecture notes](docs/architecture.md).

## What is included

| Area | Capabilities |
| --- | --- |
| Tenancy | Users, organizations, projects, memberships, JWT authentication |
| Queues | Priority, pause/resume, concurrency limits, retry defaults, rate limits, statistics |
| Jobs | Immediate, delayed, scheduled, recurring, batch, manual retry, pagination, filtering |
| Reliability | Atomic claims, capacity tokens, lease renewal, fencing, outbox relay, scheduler leader lock |
| Operations | Worker heartbeats, execution attempts, structured logs, dead-letter replay, metrics |
| Extensions | Workflow dependencies, queue admission control, unknown external-result state |

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
                                      │
                                      ├──► RETRY_WAIT ──► QUEUED
                                      └──► FAILED ──► DLQ
```

## Verification

```bash
# Rust unit and PostgreSQL integration tests
cargo test --workspace -- --test-threads=1
cargo clippy --workspace --all-targets -- -D warnings

# Frontend build and dependency audit
cd frontend && npm ci && npm run build && npm audit --audit-level=high

# Controlled admission-load test (10 → 50 → 100 virtual users)
k6 run bench/k6.js
```

Latest local evidence:

- 12/12 Rust tests passed against PostgreSQL 18, including idempotency, cron
  deduplication, capacity contention, and lease fencing.
- The API lifecycle was exercised against PostgreSQL and NATS JetStream.
- The controlled load run accepted 11,100 submissions at 219 jobs/s; p95 was
  166 ms and p99 was below 200 ms on the local test environment.

These figures are development-environment evidence, not a production capacity
guarantee. Production readiness still requires deployment-specific load, soak,
failover, backup/restore, and external-side-effect testing.

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
