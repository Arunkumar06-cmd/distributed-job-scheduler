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

Defined `Scheduled, Queued, Claimed, Running, RetryWait, Completed, Failed, Cancelled`. `Claimed` exists to make `QUEUED->CLAIMED->RUNNING` two-phase (claim in TX, then long task). Without `Claimed`, `RUNNING` would hold TX open.

## Batch & DLQ Retention

Batches via `batches` table + trigger `update_batch_on_job_complete` (atomic counters). DLQ `ON DELETE RESTRICT` preserves history; `replayed_to_job_id` links replay. Not `CASCADE` to avoid losing audit when job deleted.

## Frontend: Scoped Polling

The dashboard polls scoped API snapshots for jobs, queue health, and workers.
WebSocket/SSE fan-out is deliberately deferred until event subscriptions can be
enforced with the same organization and project authorization boundaries as
the REST API. This keeps the current dashboard simple and avoids accidental
cross-tenant broadcasts.

## Implemented Extensions

- **Workflow DAG**: Workflow creation records dependency edges; dependent jobs start in `WAITING` and are released only after their predecessors complete.
- **Rate limiting**: Queue-level admission uses a sliding-window policy before accepting a new job.

## Deferred Extensions

- **Queue sharding**: Would need consistent hash on `job_id` -> partition, and consumer per shard. Omitted (single stream per queue suffices).
- **Event-driven (webhooks)**: Would need `http` handler with retry + DLQ for webhook failures. Handler trait is extensible.
- **RBAC**: Currently `org_memberships` with `owner/admin/member/viewer` but not enforced per route beyond `is_member`. Full RBAC would add middleware `require_role`.

## Measured Load Snapshot

On the local Docker/PostgreSQL/NATS environment used for this repository, the
admission-load script submitted 11,100 jobs at 219 accepted jobs/s with p95
latency of 166 ms and p99 below 200 ms. This is a development-machine result,
not a production capacity promise. See `bench/results.md` for the test shape
and `docs/testing.md` for the verification boundary.
