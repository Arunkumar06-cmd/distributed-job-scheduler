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
- Auth (argon2 + JWT HS256, 7d expiry, org/project membership checks)
- Validation, pagination, filtering, structured errors `{error:{code,message}, request_id}`
- Transactional job creation + outbox
- Queue pause/resume, stats via aggregated counts
- Scoped dashboard polling for live operational views
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
Lease 30s, poll 250ms, batch 100. Never holds TX during network.

### Worker
```
NATS batch (max 1, expires 5s) -> process_message
  -> claim_job (queue lock NOWAIT, capacity check, QUEUED->CLAIMED, execution STARTED)
  -> CLAIMED->RUNNING (outside claim TX, sets started_at)
  -> spawn lease_renewer (every 5s, UPDATE lease_expires_at WHERE epoch)
  -> handler.handle (echo/sleep/always_fail, idempotent)
  -> complete (fenced) or fail (RETRY_WAIT + next_retry or FAILED + DLQ)
  -> ACK (or Nak with delay for pause/capacity)
```
- Handler registry keyed by `payload.type`
- Queue concurrency via `FOR UPDATE NOWAIT` (serializes claims per queue; trades throughput for correctness)
- Lease fencing via `lease_epoch` (old worker's UPDATE affects 0 rows)

### Scheduler
```
loop every 5s:
  try pg_try_advisory_lock(0x73636865) -> if not leader sleep
  tick:
    promote SCHEDULED->QUEUED where scheduled_for <= NOW() + outbox
    requeue RETRY_WAIT->QUEUED where next_retry_at <= NOW() + outbox (CTE WITH moved + INSERT outbox)
    for each due scheduled_jobs (next_fire_at <= NOW()):
        create_cron_occurrence (INSERT scheduled_occurrences ON CONFLICT DO NOTHING -> if 0 rows dedup)
        compute next occurrence via cron+tz, update next_fire_at
```
Leader lock is session-scoped, auto-released on disconnect. Dedup PK ensures exactly-once even if two schedulers race.

## NATS Subjects

```
org.{org_id}.proj.{project_id}.queue.{queue_id}.{tier}
tier = high (priority>=10) | standard (>0) | low (0)
```

Stream per queue: `JOBS_{org}_{proj}_{queue}` (dashes -> underscores), subjects `org.*.proj.*.queue.*.*` (or per-queue). Consumer per worker: `worker-{name}-{stream}` durable, `Explicit` ack, `AckWait = lease*2`, `max_deliver 10`.

Duplicate window: `Nats-Msg-Id` = `outbox.id` (stable across retries).

## Failure Handling

| Failure | Detection | Recovery |
|---------|-----------|----------|
| Worker crash after claim | NATS AckWait expires -> redelivery; lease_expires_at passes | New worker claims with epoch+1, old fenced |
| Relay crash after publish before delete | Lease expires -> another relay reclaims same Nats-Msg-Id | JetStream dedup suppresses duplicate within window; business idempotent |
| API crash during BEGIN | ROLLBACK -> no job/outbox | Client retries with same Idempotency-Key |
| DB outage | Worker fails to commit -> does not ACK -> redelivery | - |
| Queue paused | Worker sees `is_paused` -> Nak with delay 5s | Resumes when unpaused |

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
