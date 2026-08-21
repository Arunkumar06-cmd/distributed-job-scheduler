# ER Diagram

```mermaid
erDiagram
    users ||--o{ org_memberships : "member"
    organizations ||--o{ org_memberships : "has"
    organizations ||--o{ projects : "owns"
    users ||--o{ project_memberships : "member"
    projects ||--o{ project_memberships : "has"
    projects ||--o{ retry_policies : "has"
    projects ||--o{ queues : "owns"
    queues ||--o{ jobs : "contains"
    queues ||--o{ scheduled_jobs : "has"
    queues ||--o{ retry_policies : "uses"
    jobs ||--o{ job_executions : "attempts"
    jobs ||--o{ job_logs : "logs"
    jobs ||--o{ outbox_events : "dispatch"
    jobs ||--o{ dead_letter_entries : "failed"
    jobs }o--o{ batches : "part of"
    projects ||--o{ batches : "owns"
    workers ||--o{ job_executions : "executes"
    workers ||--o{ job_logs : "emits"
    workers ||--o{ worker_heartbeats : "heartbeats"
    scheduled_jobs ||--o{ scheduled_occurrences : "occurrences"
    scheduled_occurrences }o--|| jobs : "creates"

    users {
        uuid id PK
        text email UK
        text password_hash
        text display_name
        bool is_active
        timestamptz created_at
    }
    organizations {
        uuid id PK
        text slug UK
        text name
        uuid created_by FK
    }
    projects {
        uuid id PK
        uuid org_id FK
        text slug UK_with_org
        text name
    }
    queues {
        uuid id PK
        uuid project_id FK
        text name UK_with_project
        int max_concurrency
        bool is_paused
        int default_priority
        uuid retry_policy_id FK nullable
    }
    jobs {
        uuid id PK
        uuid queue_id FK restrict
        uuid batch_id FK nullable
        enum type
        enum status
        jsonb payload
        int priority
        int attempt
        int max_attempts
        enum retry_strategy
        bigint base_delay_secs
        bigint max_delay_secs
        bigint lease_epoch
        uuid lease_owner FK nullable
        timestamptz lease_expires_at
        timestamptz scheduled_for
        timestamptz next_retry_at
        text idempotency_key
        unique queue_id_idempotency_key
        jsonb result
        text error_message
    }
    job_executions {
        uuid id PK
        uuid job_id FK
        uuid worker_id FK nullable
        int attempt
        bigint lease_epoch
        enum status
        timestamptz started_at
        bigint duration_ms
        text nats_msg_id
    }
    workers {
        uuid id PK
        text worker_name UK
        text version
        text hostname
        int max_concurrency
        bool is_active
        timestamptz last_heartbeat_at
    }
    outbox_events {
        uuid id PK
        uuid job_id FK
        uuid queue_id
        uuid org_id
        uuid project_id
        text subject
        jsonb payload
        int priority
        text nats_msg_id
        text relay_owner_id
        timestamptz relay_locked_until
        timestamptz published_at
    }
    scheduled_jobs {
        uuid id PK
        uuid queue_id FK cascade
        text name
        text cron_expr nullable
        text timezone
        timestamptz run_once_at
        timestamptz next_fire_at
        bool is_active
    }
    scheduled_occurrences {
        uuid scheduled_job_id PK
        timestamptz fire_time PK
        uuid created_job_id FK nullable
    }
    dead_letter_entries {
        uuid id PK
        uuid job_id FK
        uuid queue_id
        enum reason
        int attempt
        jsonb payload
        text final_error
    }
    batches {
        uuid id PK
        uuid project_id FK
        uuid queue_id FK
        text name
        int total_jobs
        int completed_jobs
        int failed_jobs
        enum status
    }
```

## Keys & Normalization

- **PKs**: `gen_random_uuid()` everywhere except `scheduled_occurrences` (composite) and `job_logs` (bigserial). All FKs `uuid`.
- **FKs**: `ON DELETE CASCADE` where child should disappear with parent (org->projects, project->queues, queue->scheduled_jobs, outbox->job). `RESTRICT` for `jobs.queue_id` and history tables (`job_executions`, `dead_letter_entries`, `job_logs`) to preserve audit.
- **Unique**: `(org_id, slug)` for orgs/projects, `(project_id, name)` for queues, `(queue_id, idempotency_key)` for jobs, `worker_name` for workers, composite PK for occurrences.
- **Checks**: `priority 0..100`, `max_concurrency 1..1000`, `max_attempts 1..100`, `base_delay >=0`, `max_delay >= base`.
- **Normalization**: 3NF. Queues reference retry_policy instead of denormalizing; jobs duplicate `retry_strategy/base_delay` for immutability per job (so queue policy change doesn't affect in-flight jobs). `job_executions` is append-only, never update `jobs` to overwrite history.

## Indexes (query-driven)

```sql
idx_jobs_queue_status          ON jobs(queue_id, status)
idx_jobs_running               ON jobs(queue_id) WHERE status='RUNNING'          -- capacity check
idx_jobs_queued                ON jobs(queue_id, priority DESC, created_at) WHERE status='QUEUED' -- claim order
idx_jobs_retry                 ON jobs(next_retry_at) WHERE status='RETRY_WAIT'   -- requeue poll
idx_jobs_scheduled             ON jobs(scheduled_for) WHERE status='SCHEDULED'   -- promote
idx_jobs_batch                 ON jobs(batch_id) WHERE batch_id IS NOT NULL
idx_executions_job_started     ON job_executions(job_id, started_at DESC)
idx_logs_job_time              ON job_logs(job_id, created_at DESC)
idx_outbox_claim               ON outbox_events(priority DESC, created_at) WHERE published_at IS NULL -- relay SKIP LOCKED
idx_scheduled_active_next      ON scheduled_jobs(next_fire_at) WHERE is_active
```
All partial indexes keep hot paths small. Verified with `EXPLAIN (ANALYZE, BUFFERS)`.

## Performance Considerations

- `SKIP LOCKED` allows concurrent relay/worker claims without blocking.
- Queue lock `FOR UPDATE NOWAIT` serializes per hot queue (acceptable: correctness > max throughput; p50 claim <5ms).
- JSONB for `payload`, `result`, `meta` with GIN if needed (not yet, for demo).
- Connection pool 20, advisory lock avoids distributed coordinator.
- Stream per queue (vs global) isolates tenant blast radius; 100k max_messages limit.
