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

## Queue Concurrency: `FOR UPDATE NOWAIT` vs Optimistic

**Chosen: `FOR UPDATE NOWAIT` on queue row**
- Simple, provably serializes capacity check. `COUNT RUNNING` + claim in same TX under queue lock guarantees `RUNNING <= max_concurrency`.
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

## Frontend: Polling + SSE

SSE (`text/event-stream`) via broadcast channel is simpler than WebSockets for one-way updates. Frontend polls every 3s for jobs, 5s for workers, and subscribes to SSE for push (keep-alive 15s). Choice avoids `socket.io` dependency.

## What Was Not Done (Intentionally)

- **Workflow DAG**: Would require topological sort + `depends_on` FKs + scheduler that checks predecessors. Omitted for time; schema could add `job_dependencies`.
- **Rate limiting**: Could add `token_bucket` per queue via `pg_advisory_lock` or `governor` crate; omitted.
- **Queue sharding**: Would need consistent hash on `job_id` -> partition, and consumer per shard. Omitted (single stream per queue suffices).
- **Event-driven (webhooks)**: Would need `http` handler with retry + DLQ for webhook failures. Handler trait is extensible.
- **RBAC**: Currently `org_memberships` with `owner/admin/member/viewer` but not enforced per route beyond `is_member`. Full RBAC would add middleware `require_role`.

## Performance Expectations

- Claim latency p50 <5ms, p95 <20ms (includes queue lock + count). Throughput per hot queue ~ 200 jobs/sec (due to serialization), overall ~ 1000 jobs/sec across queues (8 workers).
- Outbox poll 250ms, publish batch 100, NATS file storage fsync every 1s.
- Load test (not run in CI) would use `k6` or `cargo bench`.
