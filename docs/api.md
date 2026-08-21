# API Documentation

Base URL: `http://localhost:8080`

## Authentication

`POST /auth/register` `{email, password, display_name}` -> `201 {token, user}`

`POST /auth/login` `{email, password}` -> `200 {token, user}`

`GET /auth/me` `Authorization: Bearer <jwt>` -> `200 {id,email,display_name}`

JWT HS256, 7d expiry, `sub = user_id`. Passwords `argon2` hashed.

**Authorization**: every queue/project/org endpoint checks `org_memberships` and `project_memberships`. A user can only access resources in orgs they belong to. Returns `403 Forbidden` otherwise, `401` for missing/invalid token.

## Organizations

`POST /organizations` `401/403` `{name, slug}` -> `201 Organization`

`GET /organizations` -> `[Organization]` (only caller's orgs)

`GET /organizations/:id` -> `Organization` (must be member)

## Projects

`POST /projects` `{org_id, name, slug, description?}` -> `201 Project` (must be org member)

`GET /projects?org_id=` -> `[Project]` or all caller's orgs if no param

`GET /projects/:id` -> `Project`

## Queues

`POST /queues` `{project_id, name, description?, max_concurrency? (1..1000), default_priority? (0..100), ack_wait_secs?, max_receives?, retry_policy_id?}` -> `201 Queue` + creates NATS stream `JOBS_{org}_{proj}_{queue}` with subjects `org.{org}.proj.{proj}.queue.{queue}.*`

`GET /queues?project_id=` -> `[Queue]`

`GET /queues/:id` -> `Queue`

`PATCH /queues/:id` `{max_concurrency?, default_priority?, ack_wait_secs?, max_receives?, description?}` -> `Queue`

`POST /queues/:id/pause` -> `Queue` (`is_paused=true`, workers Nak with delay)

`POST /queues/:id/resume` -> `Queue`

`GET /queues/:id/stats` -> `{queued, running, retry_wait, completed, failed, scheduled, claimed, dlq}`

## Jobs

`POST /jobs` `Idempotency-Key: <key>` (header beats body) `{queue_id, payload, priority?, max_attempts? (1..100), retry_strategy? (fixed|linear|exponential), base_delay_secs?, max_delay_secs?, scheduled_for? (RFC3339), idempotency_key?, type?/kind? (immediate|delayed|scheduled|recurring|batch)}` -> `202 Job` (transactionally inserts `job` + `outbox` if not scheduled). Duplicate idempotency -> `409 {code: Conflict}`.

`GET /jobs?queue_id=&status=&priority_min=&batch_id=&page=&page_size=` -> `{data, page, page_size, total, total_pages}` (status filter case-insensitive)

`GET /jobs/:id` -> `Job`

`POST /jobs/:id/retry` -> `Job` (moves `FAILED` or `RETRY_WAIT` -> `QUEUED` + new outbox, resets attempt)

`POST /jobs/batch` `{queue_id, name?, jobs: [{payload, priority?, idempotency_key?}] (1..1000), priority?, max_attempts?, retry_strategy?}` -> `202 {batch, jobs}`

## Scheduled Jobs

`POST /scheduled-jobs` `{queue_id, name, payload, priority?, cron_expr? (6-field with secs `0 * * * * *`), timezone? (IANA, default UTC), run_once_at?}` (one of cron or run_once required) -> `201 ScheduledJob` with `next_fire_at` computed. Cron parsed via `cron` crate + `chrono-tz`.

`GET /scheduled-jobs?queue_id=` -> `[ScheduledJob]`

`DELETE /scheduled-jobs/:id` -> `204` (deactivates + deletes)

## Workers

`GET /workers` -> `[Worker]` with `status: ONLINE (<15s) | STALE (<60s) | OFFLINE`, `running_jobs`

`GET /workers/:id` -> `Worker`

## Executions & Logs

`GET /jobs/:id/executions` -> `[Execution]` (`attempt, lease_epoch, status STARTED|COMPLETED|FAILED|ABANDONED, duration_ms`)

`GET /jobs/:id/logs?limit=` -> `[Log]` (`level, message, meta`)

## DLQ & Batches

`GET /dlq?queue_id=&page=&page_size=` -> `{data, page}`

`POST /dlq/:id/replay` -> `Job` (creates new `QUEUED` job from DLQ payload + outbox, marks DLQ `replayed_to_job_id`)

`GET /batches?project_id=` -> `[Batch]`

`GET /batches/:id` -> `{batch, jobs}`

## Health & Events

`GET /health` -> `{status: ok}`

`GET /metrics` -> `{jobs:{total,queued,running,completed,failed,retry_wait,dlq}, workers:{active}, db:{pool_size,idle}, nats:{connected}}`

## Errors

All errors: `status -> {error:{code, message}, request_id: uuid}`

Codes: `NotFound (404), Unauthorized (401), Forbidden (403), Conflict (409), Validation (400), QueuePaused/QueueAtCapacity/StaleLease (409), Internal (500)`

## Pagination & Filtering

`page` (1-indexed, default 1), `page_size` (default 20, max 100). Filtering via query params, `total` and `total_pages` returned.

## Idempotency Details

- API: `UNIQUE(queue_id, idempotency_key)` (NULL keys allow duplicates). Header `Idempotency-Key` overrides body.
- NATS: `Nats-Msg-Id = outbox.id` (stable), JetStream duplicate window 2m suppresses redelivered publishes from crashed relay.
- Business: handlers must be idempotent; use `job_id` as external idempotency key.
