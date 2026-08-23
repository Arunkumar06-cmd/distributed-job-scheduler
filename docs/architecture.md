# Architecture

## High-Level

```
                         ┌─────────────┐
                         │   Browser   │
                         └──────┬──────┘
                                │  React + Vite (port 3000) -> /api proxy to 8080
                         ┌──────▼──────┐
                         │ Load Balancer │
                         └──────┬──────┘
                      ┌─────────┴─────────┐
                      ▼                   ▼
               ┌────────────┐       ┌────────────┐
               │  API Node 1 │       │ API Node 2 │  stateless, Axum + Tokio
               └─────┬──────┘       └──────┬─────┘
                     │                     │
                     └──────────┬──────────┘
                                │
               ┌────────────────┼────────────────┐
               ▼                ▼                ▼
        ┌────────────┐   ┌─────────────┐   ┌──────────┐
        │ PostgreSQL │   │ NATS JetStream │  │ Workers  │
        │ (truth)    │   │ (delivery)     │  │ W1 W2 W3 │
        └────────────┘   └─────────────┘   └──────────┘
               ▲                ▲                │
               └────────────────┴────────────────┘
                      Outbox, Scheduler, DLQ
```

**Two authorities:**
- PostgreSQL = "What is the truth?" (ACID, FKs, locks, JSONB)
- NATS = "Which worker gets it?" (durable, ack, redelivery, backpressure)

## Component Responsibilities

### API (Axum)
- Auth (argon2id; 1h access tokens rotated by 30-day refresh tokens via `/auth/refresh`; typed claims so a refresh token can never authenticate a request; org/project membership checks)
- Per-user fixed-window rate limiting (`API_RATE_LIMIT_PER_MIN`, 0 disables)
- Audit trail for privileged mutations (`audit_log`: queue config/pause/resume, DLQ replay, membership changes)
- Versioned contract at `/api/v1/*` with frozen unversioned legacy aliases
- Validation, pagination, filtering, structured errors `{error:{code,message}, request_id}`
- Transactional job creation + outbox
- Queue reconfiguration incl. `PATCH {is_paused}` (pause/resume POST verbs emit Deprecation/Sunset headers)
- Stats via aggregated counts; dashboard uses SSE snapshots as change-trigger plus slow-poll fallback
- Prometheus text exposition at `/metrics` (status gauges, DLQ depth, outbox pending, worker liveness, execution-duration histogram over trailing 24h) and JSON summary at `/stats`
- Spawns outbox relay + scheduler as background tasks (also runnable standalone)
- Spawns outbox relay + scheduler as background tasks (also runnable standalone)

### Outbox Relay
```
BEGIN -> SELECT ... FOR UPDATE SKIP LOCKED LIMIT 100 ORDER BY priority DESC, created_at
        UPDATE relay_owner, relay_locked_until -> COMMIT
        -- network --
        publish(subject, payload, headers Nats-Msg-Id=event.id) -> wait PubAck
        -- network --
BEGIN -> DELETE WHERE relay_owner = $self COMMIT
```
Lease `OUTBOX_LEASE_SECS`, poll `OUTBOX_POLL_INTERVAL_MS`, batch `OUTBOX_BATCH_SIZE`. Never holds TX during network. Failed events get `publish_attempts++` and exponential backoff (30s doubling, capped 10 min) instead of a fixed retry drumbeat — poison pills stop hammering the broker while staying eligible for redelivery.

### Worker
```
NATS batch (max 1, expires 5s) -> process_message
  -> claim_job (queue lock NOWAIT, capacity check, QUEUED->CLAIMED, execution STARTED)
  -> CLAIMED->RUNNING (outside claim TX, sets started_at)
  -> spawn lease_renewer (every heartbeat, UPDATE lease_expires_at WHERE epoch; 3 consecutive renewal failures fence the worker)
  -> dispatch to bounded pool (WORKER_CONCURRENCY semaphore; fetch backpressures when full)
  -> handler under panic-guard + HANDLER_TIMEOUT_SECS timeout (panic/hang => retryable failure, never kills the consumer)
  -> complete (fenced) or fail (RETRY_WAIT + next_retry or FAILED + DLQ)
  -> ACK (or Nak with delay for pause/capacity)
```
- Handler registry keyed by `payload.type`
- Queue concurrency via `FOR UPDATE NOWAIT` (serializes claims per queue; trades throughput for correctness)
- Lease fencing via `lease_epoch` (old worker's UPDATE affects 0 rows)

### Scheduler
```
loop every SCHEDULER_POLL_INTERVAL_SECS (woken instantly by LISTEN queue_events NOTIFY):
  try pg_try_advisory_lock(0x73636865) -> if not leader sleep
  tick:
    promote SCHEDULED->QUEUED where scheduled_for <= NOW() + outbox
    requeue RETRY_WAIT->QUEUED where next_retry_at <= NOW() + outbox (CTE WITH moved + INSERT outbox)
    for each due scheduled_jobs (next_fire_at <= NOW()):
        create_cron_occurrence (INSERT scheduled_occurrences ON CONFLICT DO NOTHING -> if 0 rows dedup)
        compute next occurrence via cron+tz, update next_fire_at
    reconcile UNKNOWN_EXTERNAL_RESULT older than UNKNOWN_GRACE_SECS per UNKNOWN_RESOLUTION_POLICY (dlq|retry|complete)
    housekeeping: prune job_logs + worker_heartbeats, archive terminal jobs to *_archive twins (ARCHIVE_AFTER_DAYS)
```
Leader lock is session-scoped, auto-released on disconnect. Dedup PK ensures exactly-once even if two schedulers race.

## NATS Subjects

```
org.{org_id}.proj.{project_id}.queue.{queue_id}.{tier}
tier = high (priority>=10) | standard (>0) | low (0)
```

Stream per queue: `JOBS_{org}_{proj}_{queue}` (dashes -> underscores). A
single-shard queue uses `org.{org}.proj.{project}.queue.{queue}.{tier}`;
sharded queues use `...queue.{queue}.shard.{shard_id}.{tier}`. Workers attach a **shared** durable pull consumer named by the hash of
stream+subject — stable across restarts and identical between replicas, so
workers are competing consumers and no durable is ever orphaned by a reboot.
Configured with `Explicit` ack, `AckWait = lease*2`, `max_deliver 10`; creation
retries until the lazily-provisioned stream exists. New queues reach workers
instantly via a `queue_created` NOTIFY trigger (10s sweep as fallback).

Duplicate window: `Nats-Msg-Id` = `outbox.id` (stable across retries).

## Failure Handling

| Failure | Detection | Recovery |
|---------|-----------|----------|
| Worker crash after claim | NATS AckWait expires -> redelivery; lease_expires_at passes | New worker claims with epoch+1, old fenced |
| Relay crash after publish before delete | Lease expires -> another relay reclaims same Nats-Msg-Id | JetStream dedup suppresses duplicate within window; business idempotent |
| API crash during BEGIN | ROLLBACK -> no job/outbox | Client retries with same Idempotency-Key |
| DB outage | Worker fails to commit -> does not ACK -> redelivery | - |
| Queue paused | Worker sees `is_paused` -> Nak with delay 5s | Resumes when unpaused |
| Handler panics or hangs | catch_unwind + HANDLER_TIMEOUT_SECS | Retryable failure (`handler_panicked`/`handler_timeout`); idempotency contract makes retry safe |
| Worker cannot renew lease 3x in a row | renew_lease returns false | Worker fences itself; result commit refused, job redriven after expiry |
| Scheduler leader crash | Session-scoped advisory lock auto-released | Another replica takes leadership on next tick |
| UNKNOWN external outcome | Reconciler after grace period | Policy-driven: DLQ (default) / redrive / complete |

## Data Flow Example

1. `POST /jobs` -> authz -> `BEGIN; INSERT job status=QUEUED queued_at=NOW(); INSERT outbox; COMMIT` -> `202 Accepted`
2. Relay polls, claims, publishes to `org.o1.proj.p1.queue.q1.high` with `Nats-Msg-Id=e123`, gets Ack, deletes outbox
3. Worker batch receives, `claim_job` -> `QUEUED->CLAIMED epoch1`, then `CLAIMED->RUNNING`
4. Lease renewer ticks every 5s, `InProgress` via `AckKind::Progress` (if long)
5. Handler `echo` returns `Ok(json)`, worker `complete_job` (fenced) -> `RUNNING->COMPLETED`, `execution Completed`, `ACK`
6. On failure, `fail_job` -> `RETRY_WAIT next_retry_at = now + delay(Exponential:5,10,20)` -> `ACK`, scheduler requeues later via new outbox
7. After max_attempts, `FAILED` + `dead_letter_entries` + `ACK`

## Deployment

- API: stateless, scale horizontally behind LB, share DB + NATS
- Workers: scale independently, each registers heartbeat (5s), supervisor handles graceful shutdown (30s)
- Scheduler: runs embedded in API or standalone, leader via advisory lock (no extra infra like etcd)
- NATS: single node for demo, can cluster; JetStream file storage, `duplicateWindow 2m`
- Observability: `tracing` JSON logs, `EXPLAIN ANALYZE` for indexes, `/metrics` endpoint, worker heartbeats
