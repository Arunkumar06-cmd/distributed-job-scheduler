# Design Decisions

## PostgreSQL as Source of Truth vs Redis

**Chosen: PostgreSQL**
- Need ACID, FKs, `SKIP LOCKED`, advisory locks, JSONB, strong consistency for job state.
- Redis as primary would require AOF + replication for durability but still lacks transactions across job + outbox, and needs Lua for atomic claim. Postgres gives `FOR UPDATE`, partial indexes, and `EXPLAIN ANALYZE`.
- Trade-off: Postgres slower than Redis for pure queue throughput, but correctness and queryability (stats, logs, DLQ) matter more for assignment.

## NATS JetStream vs Kafka/RabbitMQ/SQS

**Chosen: NATS JetStream**
- Lightweight, file storage, built-in `Ack/Nak/Progress`, `MaxDeliver`, stream per subject, duplicate window. Single binary, no ZK.
- Kafka is heavier, needs schema, and pull semantics are similar but overkill. RabbitMQ `prefetch` + `ack` is similar but lacks hierarchical subjects and easy stream-per-queue.
- Trade-off: NATS duplicate window (2m) is not infinite; business idempotency required anyway. For huge scale, Kafka partitioning would be needed.

## Transactional Outbox vs Dual Write

**Outbox** avoids dual-write anomaly (DB commit + NATS publish not atomic). Alternative `publish then commit` risks lost jobs if DB fails; `commit then publish` risks orphan jobs if publish fails and no retry. Outbox makes publish retryable via relay lease.

**Why not `SELECT FOR UPDATE` + `DELETE` in same TX as publish?** Holding TX during network I/O blocks connections, holds row locks, inflates `pg_stat_activity`. Instead: `claim (TX) -> publish (network) -> clear (TX)`.

## Queue Concurrency: `FOR UPDATE NOWAIT` + Capacity Tokens vs Optimistic Counters

**Chosen: `FOR UPDATE NOWAIT` on queue row**
- Simple, provably serializes admission. The claim transaction locks the queue, reserves one available capacity token with `SKIP LOCKED`, and claims one job; the token is released only when the execution reaches a terminal state.
- `NOWAIT` fails fast instead of blocking; worker NAKs with delay and retries.
- Alternatives: `SELECT ... FOR UPDATE` (blocks, causes contention), optimistic `UPDATE ... WHERE running < limit` (needs counter table, race), or Postgres `advisory` per queue (more complexity).
- Trade-off: Hot queue serializes claims (throughput ~ 1 claim at a time per queue). Acceptable for assignment; production could use `pg_try_advisory_xact_lock(hashtext(queue_id))` or sharding.

## At-Least-Once vs Exactly-Once

**At-least-once delivery + idempotent handlers**
- True exactly-once requires distributed transaction across DB + broker + external service (e.g., Stripe), impossible. NATS can redeliver after AckWait, worker can crash after DB commit but before ACK.
- So: `job_id` as idempotency key for external calls, `UNIQUE` for API, `Nats-Msg-Id` for broker. Tests prove dedup: duplicate `Idempotency-Key` -> 409, relay retry with same `Nats-Msg-Id` -> JetStream dedup, handler re-execution detects external effect already done.

## Lease + Epoch Fencing vs Distributed Lock

**Lease epoch** is cheaper than Redis Redlock and avoids `TTL` drift. Each claim bumps `lease_epoch`, renewal updates `lease_expires_at` only if `epoch` matches. Stale worker's `UPDATE ... WHERE lease_epoch = :old` affects 0 rows -> fenced.
- Heartbeat for workers (5s) vs per-job lease (30s)分离: worker liveness ≠ job ownership. `InProgress` extends NATS AckWait separately.

## Scheduler: Advisory Lock vs Leader Election Service

**`pg_try_advisory_lock`** is zero-infra leader election for single-DC. Alternative `etcd`/`consul` would add operational complexity. Advisory lock is session-scoped, auto-released on crash, and we add DB PK dedup as safety net (`scheduled_occurrences` PK) so even if two schedulers race, only one succeeds. Cron uses `cron` + `chrono-tz`, stores `next_fire_at`, handles DST by using `Tz` and documenting ambiguous times as skip.

## State Machine Simplicity

Defined `Scheduled, Queued, Claimed, Running, RetryWait, Completed, Failed, Cancelled`, later extended with `WAITING` (workflow DAG gate) and `UNKNOWN_EXTERNAL_RESULT` (crash window between dispatching external work and observing its outcome). `UNKNOWN` is never a dead end: the reconciler resolves it after a grace period via `UNKNOWN_RESOLUTION_POLICY` — `dlq` by default because guessing an external outcome is unsafe; `retry` requires the idempotency contract. `Claimed` exists to make `QUEUED->CLAIMED->RUNNING` two-phase (claim in TX, then long task). Without `Claimed`, `RUNNING` would hold TX open.

## Batch & DLQ Retention

Batches via `batches` table + trigger `update_batch_on_job_complete` (atomic counters). DLQ `ON DELETE RESTRICT` preserves history; `replayed_to_job_id` links replay. Not `CASCADE` to avoid losing audit when job deleted.

## Frontend: Polling with Scoped Live Snapshots

The dashboard uses polling for its primary view. Authenticated clients can also
open project-scoped SSE or WebSocket snapshot streams. Both authorize the
project before upgrading/streaming and generate scoped database snapshots,
rather than exposing the process-wide event broadcast channel.

## Implemented Extensions

- **Workflow DAG**: Workflow creation records dependency edges; dependent jobs start in `WAITING` and are released only after their predecessors complete.
- **Rate limiting**: Queue-level admission uses a sliding-window policy before accepting a new job.
- **RBAC**: Organization roles are enforced: owner/admin control configuration
  and memberships, member can submit/retry work, and viewer is read-only.
- **Queue sharding**: A queue may define 1–128 deterministic NATS shards. The
  default remains one shard so existing deployments keep their routing.
- **AI failure summaries**: An opt-in, non-critical OpenAI Responses worker
  writes structured DLQ summaries only when `AI_SUMMARIES_ENABLED` and
  `OPENAI_API_KEY` are configured.

## API Versioning & Verb Policy

**Chosen: dual-mounted `/api/v1/*` + frozen unversioned aliases.**
New consumers get a stable contract; existing clients keep working with zero
migration cost. The OpenAPI document declares `/api/v1` as canonical.

**Verb convention**: `PATCH` mutates stored state; POST verb-subresources are
actions only. Queue pause/resume is *stored state* (`is_paused`), so it moved
to `PATCH /queues/:id`; the old `POST .../pause|resume` remain as documented
compatibility aliases emitting RFC-8594 `Deprecation`/`Sunset`/`Link` headers.

Trade-off: two paths for one operation during the deprecation window. Accepted
because hard-breaking the dashboard or third-party scripts buys nothing for an
internship-scale system.

## Token Model: Short Access + Rotating Refresh vs Long-Lived JWT

**Chosen: 1h access tokens rotated by 30-day refresh tokens.**
A stolen long-lived bearer was previously valid for 7 days with no revocation
path short of changing the JWT secret. Typed claims (`typ: access|refresh`)
make cross-use impossible at validation time; rotation gives a bounded blast
radius without per-request DB session checks.
Trade-off: refresh adds an endpoint and client wrapper. Reuse detection (one
refresh-token table) and Redis-backed limiters are named future work.

## Rate Limiting: In-Process Fixed Window vs Redis

**Chosen: in-process per-user fixed window** checked inside the auth
middleware after identity resolution. Zero new infrastructure, correct per
instance. Trade-off: N API replicas multiply the effective limit; acceptable
because the limiter's job is runaway-client containment, not billing-grade
metering. A Redis counter is the next tier up.

## Audit Trail: Best-Effort Writes vs Synchronous Blocking

**Chosen: best-effort `append_audit`.** Privileged mutations record actor,
org, action, target, details; failures log loudly but never fail the user's
operation. Rationale: audit completeness matters, but availability of the
scheduling control-plane matters more, and every audited action already has
authoritative state elsewhere (queue rows, memberships).

## Hot/Cold Separation: Archive Twins vs Declarative Partitioning

**Chosen: archive twin tables + batched moves.** Declarative partitioning of
`jobs` by time would force the partition key into the PK, break the global
idempotency unique constraint, and touch every query — high blast radius.
Instead, terminal jobs older than `ARCHIVE_AFTER_DAYS` move with their full
dependency family (executions, logs, replayed DLQ entries) into `*_archive`
twins via a bounded-batch function. Un-replayed DLQ rows block archival (they
are operational state). Measured ~4,500 rows/s on a laptop Postgres.
Trade-off: archived jobs leave the hot table (job lookups by id miss them);
acceptable because archival only touches completed history.

## Metrics: Scrape-Time Aggregation vs Client Instrumentation

**Chosen: compute histograms from `job_executions` at scrape time** (trailing
24h, standard cumulative buckets + sum/count). No worker-side metrics server,
no cardinality explosion from per-queue labels, and durations live in the DB
ledger anyway. Trade-off: heavier scrapes as volume grows and no sub-24h
window control — the point where a push-gateway or OTel collector earns its keep.

## Testing Isolation: Per-Test Databases vs Truncate/Rollback

**Chosen: throwaway `js_test_<uuid>` database per test, all migrations applied
twice.** Double application proves idempotency continuously (a drifted schema
converges); isolation removes truncate races entirely, so the suite runs fully
parallel. Trade-off: ~200ms setup per test — negligible next to correctness.

## Frontend Live Updates: SSE Change-Trigger + Slow Poll vs Poll-Only

**Chosen: project-scoped SSE stream drives instant refreshes; polling remains
as fallback.** EventSource cannot send Authorization headers, so the events
routes additionally accept `?access_token=` — safe specifically because access
tokens now expire hourly. Snapshots are change-detected client-side and debounced;
blind fast polling was removed rather than doubled up.

## Handler Safety: Panic Guard + Timeout at the Consumer Boundary

**Chosen: every handler runs under `catch_unwind` and a hard timeout**
(`HANDLER_TIMEOUT_SECS`). A panic converts to retryable `handler_panicked`
instead of killing the consumer task serving the subject; a hang becomes
`handler_timeout` instead of pinning it forever. Safe under the idempotency
contract. Additionally, handlers dispatch onto a semaphore-bounded pool
(`WORKER_CONCURRENCY`) — one slow job can no longer starve its queue.

## Deferred Extensions

- **Event-driven (webhooks)**: Would need `http` handler with retry + DLQ for webhook failures. Handler trait is extensible.

## Measured Load Snapshot

On the local Docker/PostgreSQL/NATS environment used for this repository, the
admission-load script submitted 11,100 jobs at 219 accepted jobs/s with p95
latency of 166 ms and p99 below 200 ms. This is a development-machine result,
not a production capacity promise. See `bench/results.md` for the test shape
and `docs/testing.md` for the verification boundary.
