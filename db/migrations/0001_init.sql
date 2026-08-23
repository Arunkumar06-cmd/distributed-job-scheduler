-- 0001_init.sql: Core schema for distributed job scheduler
-- Hierarchy: organizations -> projects -> queues -> jobs
-- Plus: users, org_memberships, retry_policies, workers, worker_heartbeats,
--       job_executions, job_logs, scheduled_jobs, scheduled_occurrences,
--       outbox_events, dead_letter_entries, batches, audit_log

-- Guarded: parallel migration runners can race even IF NOT EXISTS.
DO $$ BEGIN
    CREATE EXTENSION IF NOT EXISTS "pgcrypto";
EXCEPTION WHEN duplicate_object THEN NULL; WHEN unique_violation THEN NULL; END $$;

-- =========================================================
-- ENUMS
-- =========================================================
DO $$ BEGIN
    CREATE TYPE job_status AS ENUM ('SCHEDULED','QUEUED','CLAIMED','RUNNING','RETRY_WAIT','COMPLETED','FAILED','CANCELLED');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
    CREATE TYPE job_type_enum AS ENUM ('immediate','delayed','scheduled','recurring','batch');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
    CREATE TYPE execution_status AS ENUM ('STARTED','COMPLETED','FAILED','ABANDONED');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
    CREATE TYPE batch_status AS ENUM ('QUEUED','RUNNING','PARTIALLY_COMPLETED','COMPLETED','FAILED');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
    CREATE TYPE retry_strategy AS ENUM ('fixed','linear','exponential');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
    CREATE TYPE worker_status AS ENUM ('ONLINE','STALE','OFFLINE');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
    CREATE TYPE dlq_reason AS ENUM ('max_attempts_exceeded','permanent_failure','cancelled');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
    CREATE TYPE org_role AS ENUM ('owner','admin','member','viewer');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
    CREATE TYPE project_role AS ENUM ('owner','admin','member','viewer');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

-- =========================================================
-- USERS
-- =========================================================
CREATE TABLE IF NOT EXISTS users (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email           TEXT NOT NULL UNIQUE,
    password_hash   TEXT NOT NULL,
    display_name    TEXT NOT NULL DEFAULT '',
    is_active       BOOLEAN NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- =========================================================
-- ORGANIZATIONS
-- =========================================================
CREATE TABLE IF NOT EXISTS organizations (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name            TEXT NOT NULL,
    slug            TEXT NOT NULL UNIQUE,
    created_by      UUID NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS org_memberships (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    org_id          UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    user_id         UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    role            org_role NOT NULL DEFAULT 'member',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(org_id, user_id)
);

-- =========================================================
-- PROJECTS
-- =========================================================
CREATE TABLE IF NOT EXISTS projects (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    org_id          UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    name            TEXT NOT NULL,
    slug            TEXT NOT NULL,
    description     TEXT NOT NULL DEFAULT '',
    is_archived     BOOLEAN NOT NULL DEFAULT FALSE,
    created_by      UUID NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(org_id, slug)
);

CREATE TABLE IF NOT EXISTS project_memberships (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id      UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    user_id         UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    role            project_role NOT NULL DEFAULT 'member',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(project_id, user_id)
);

-- =========================================================
-- RETRY POLICIES (shared templates, can be attached to queues)
-- =========================================================
CREATE TABLE IF NOT EXISTS retry_policies (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id      UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    name            TEXT NOT NULL,
    max_attempts    INTEGER NOT NULL CHECK (max_attempts > 0 AND max_attempts <= 100),
    strategy        retry_strategy NOT NULL DEFAULT 'exponential',
    base_delay_secs BIGINT NOT NULL CHECK (base_delay_secs >= 0) DEFAULT 5,
    max_delay_secs  BIGINT NOT NULL CHECK (max_delay_secs >= base_delay_secs) DEFAULT 3600,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(project_id, name)
);

-- =========================================================
-- QUEUES
-- =========================================================
CREATE TABLE IF NOT EXISTS queues (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id          UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    name                TEXT NOT NULL,
    description         TEXT NOT NULL DEFAULT '',
    max_concurrency     INTEGER NOT NULL CHECK (max_concurrency > 0 AND max_concurrency <= 1000) DEFAULT 5,
    is_paused           BOOLEAN NOT NULL DEFAULT FALSE,
    default_priority    INTEGER NOT NULL CHECK (default_priority >= 0 AND default_priority <= 100) DEFAULT 5,
    retry_policy_id     UUID REFERENCES retry_policies(id) ON DELETE SET NULL,
    ack_wait_secs       INTEGER NOT NULL CHECK (ack_wait_secs > 0) DEFAULT 60,
    max_receives        INTEGER NOT NULL CHECK (max_receives > 0) DEFAULT 3,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(project_id, name)
);

-- =========================================================
-- WORKERS
-- =========================================================
CREATE TABLE IF NOT EXISTS workers (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    worker_name         TEXT NOT NULL UNIQUE,
    version             TEXT NOT NULL DEFAULT '0.1.0',
    hostname            TEXT NOT NULL DEFAULT '',
    max_concurrency     INTEGER NOT NULL DEFAULT 8,
    is_active           BOOLEAN NOT NULL DEFAULT TRUE,
    last_heartbeat_at   TIMESTAMPTZ,
    started_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    stopped_at          TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS worker_heartbeats (
    id                  BIGSERIAL PRIMARY KEY,
    worker_id           UUID NOT NULL REFERENCES workers(id) ON DELETE CASCADE,
    heartbeat_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    running_jobs        INTEGER NOT NULL DEFAULT 0,
    processed_total     BIGINT NOT NULL DEFAULT 0,
    failed_total        BIGINT NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_worker_heartbeats_worker_time
    ON worker_heartbeats(worker_id, heartbeat_at DESC);

-- =========================================================
-- JOBS
-- =========================================================
CREATE TABLE IF NOT EXISTS jobs (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    queue_id            UUID NOT NULL REFERENCES queues(id) ON DELETE RESTRICT,
    batch_id            UUID,
    type                job_type_enum NOT NULL DEFAULT 'immediate',
    status              job_status NOT NULL DEFAULT 'QUEUED',
    payload             JSONB NOT NULL DEFAULT '{}'::jsonb,
    priority            INTEGER NOT NULL CHECK (priority >= 0 AND priority <= 100) DEFAULT 5,
    attempt             INTEGER NOT NULL DEFAULT 0,
    max_attempts        INTEGER NOT NULL CHECK (max_attempts > 0) DEFAULT 3,
    retry_strategy      retry_strategy NOT NULL DEFAULT 'exponential',
    base_delay_secs     BIGINT NOT NULL DEFAULT 5,
    max_delay_secs      BIGINT NOT NULL DEFAULT 3600,

    -- Lease / fencing
    lease_epoch         BIGINT NOT NULL DEFAULT 0,
    lease_owner         UUID REFERENCES workers(id) ON DELETE SET NULL,
    lease_expires_at    TIMESTAMPTZ,

    -- Scheduling
    scheduled_for       TIMESTAMPTZ,
    next_retry_at       TIMESTAMPTZ,

    -- Idempotency
    idempotency_key     TEXT,

    -- Result
    result              JSONB,
    error_message       TEXT,
    error_kind          TEXT,

    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    queued_at           TIMESTAMPTZ,
    claimed_at         TIMESTAMPTZ,
    started_at          TIMESTAMPTZ,
    completed_at        TIMESTAMPTZ,
    failed_at           TIMESTAMPTZ,
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Idempotency: one logical job per queue per key
    UNIQUE(queue_id, idempotency_key)
);

-- Indexes driven by actual query patterns
CREATE INDEX IF NOT EXISTS idx_jobs_queue_status
    ON jobs(queue_id, status);
CREATE INDEX IF NOT EXISTS idx_jobs_running
    ON jobs(queue_id) WHERE status = 'RUNNING';
CREATE INDEX IF NOT EXISTS idx_jobs_queued
    ON jobs(queue_id, priority DESC, created_at ASC) WHERE status = 'QUEUED';
CREATE INDEX IF NOT EXISTS idx_jobs_retry
    ON jobs(next_retry_at) WHERE status = 'RETRY_WAIT';
CREATE INDEX IF NOT EXISTS idx_jobs_scheduled
    ON jobs(scheduled_for) WHERE status = 'SCHEDULED';
CREATE INDEX IF NOT EXISTS idx_jobs_batch
    ON jobs(batch_id) WHERE batch_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_jobs_lease_owner
    ON jobs(lease_owner) WHERE lease_owner IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_jobs_created_at
    ON jobs(created_at DESC);

-- =========================================================
-- JOB EXECUTIONS (one row per attempt)
-- =========================================================
CREATE TABLE IF NOT EXISTS job_executions (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id              UUID NOT NULL REFERENCES jobs(id) ON DELETE RESTRICT,
    worker_id           UUID REFERENCES workers(id) ON DELETE SET NULL,
    attempt             INTEGER NOT NULL,
    lease_epoch         BIGINT NOT NULL,
    status              execution_status NOT NULL DEFAULT 'STARTED',
    started_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at         TIMESTAMPTZ,
    duration_ms         BIGINT,
    result              JSONB,
    error_message       TEXT,
    error_kind          TEXT,
    nats_msg_id         TEXT
);
CREATE INDEX IF NOT EXISTS idx_executions_job_started
    ON job_executions(job_id, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_executions_worker
    ON job_executions(worker_id, started_at DESC);

-- =========================================================
-- JOB LOGS (structured)
-- =========================================================
CREATE TABLE IF NOT EXISTS job_logs (
    id                  BIGSERIAL PRIMARY KEY,
    job_id              UUID NOT NULL REFERENCES jobs(id) ON DELETE RESTRICT,
    execution_id        UUID REFERENCES job_executions(id) ON DELETE SET NULL,
    worker_id           UUID REFERENCES workers(id) ON DELETE SET NULL,
    level               TEXT NOT NULL DEFAULT 'INFO',
    message             TEXT NOT NULL,
    meta                JSONB DEFAULT '{}'::jsonb,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_logs_job_time
    ON job_logs(job_id, created_at DESC);

-- =========================================================
-- SCHEDULED JOBS (cron / one-shot future)
-- =========================================================
CREATE TABLE IF NOT EXISTS scheduled_jobs (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    queue_id            UUID NOT NULL REFERENCES queues(id) ON DELETE CASCADE,
    name                TEXT NOT NULL,
    job_type            TEXT NOT NULL,
    payload             JSONB NOT NULL DEFAULT '{}'::jsonb,
    priority            INTEGER NOT NULL DEFAULT 5,
    cron_expr           TEXT,
    timezone            TEXT NOT NULL DEFAULT 'UTC',
    run_once_at         TIMESTAMPTZ,
    is_active           BOOLEAN NOT NULL DEFAULT TRUE,
    last_fired_at       TIMESTAMPTZ,
    next_fire_at        TIMESTAMPTZ,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (
        (cron_expr IS NOT NULL) OR (run_once_at IS NOT NULL)
    )
);
CREATE INDEX IF NOT EXISTS idx_scheduled_active_next
    ON scheduled_jobs(next_fire_at) WHERE is_active = TRUE;

-- Deterministic cron occurrence dedup
CREATE TABLE IF NOT EXISTS scheduled_occurrences (
    scheduled_job_id    UUID NOT NULL REFERENCES scheduled_jobs(id) ON DELETE CASCADE,
    fire_time           TIMESTAMPTZ NOT NULL,
    created_job_id      UUID REFERENCES jobs(id) ON DELETE SET NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (scheduled_job_id, fire_time)
);

-- =========================================================
-- TRANSACTIONAL OUTBOX
-- =========================================================
CREATE TABLE IF NOT EXISTS outbox_events (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id              UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    queue_id            UUID NOT NULL,
    org_id              UUID NOT NULL,
    project_id          UUID NOT NULL,
    subject             TEXT NOT NULL,
    payload             JSONB NOT NULL,
    priority            INTEGER NOT NULL DEFAULT 5,
    nats_msg_id         TEXT NOT NULL,
    relay_owner_id     TEXT,
    relay_locked_until  TIMESTAMPTZ,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at        TIMESTAMPTZ
);
-- NOTE: We cannot use NOW() in a partial index predicate (not immutable).
-- Instead we index relay_locked_until directly; the relay query filters
-- (relay_locked_until IS NULL OR relay_locked_until < NOW()) at runtime.
CREATE INDEX IF NOT EXISTS idx_outbox_claim
    ON outbox_events(priority DESC, created_at ASC)
    WHERE published_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_outbox_owner
    ON outbox_events(relay_owner_id) WHERE relay_owner_id IS NOT NULL;

-- =========================================================
-- DEAD LETTER QUEUE (permanent failures, kept for inspection)
-- =========================================================
CREATE TABLE IF NOT EXISTS dead_letter_entries (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id              UUID NOT NULL REFERENCES jobs(id) ON DELETE RESTRICT,
    queue_id            UUID NOT NULL,
    org_id              UUID NOT NULL,
    project_id          UUID NOT NULL,
    reason              dlq_reason NOT NULL,
    attempt             INTEGER NOT NULL,
    payload             JSONB NOT NULL,
    final_error         TEXT,
    error_kind          TEXT,
    moved_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    replayed_to_job_id  UUID REFERENCES jobs(id) ON DELETE SET NULL,
    replayed_at         TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_dlq_queue
    ON dead_letter_entries(queue_id, moved_at DESC);
CREATE INDEX IF NOT EXISTS idx_dlq_job
    ON dead_letter_entries(job_id);

-- =========================================================
-- BATCHES
-- =========================================================
CREATE TABLE IF NOT EXISTS batches (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id          UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    queue_id            UUID NOT NULL REFERENCES queues(id) ON DELETE CASCADE,
    name                TEXT NOT NULL,
    total_jobs          INTEGER NOT NULL DEFAULT 0,
    completed_jobs      INTEGER NOT NULL DEFAULT 0,
    failed_jobs         INTEGER NOT NULL DEFAULT 0,
    status              batch_status NOT NULL DEFAULT 'QUEUED',
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_batches_project
    ON batches(project_id, created_at DESC);

-- =========================================================
-- UPDATED_AT trigger
-- =========================================================
CREATE OR REPLACE FUNCTION touch_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DO $$ BEGIN
    CREATE TRIGGER trg_users_touch        BEFORE UPDATE ON users        FOR EACH ROW EXECUTE FUNCTION touch_updated_at();
EXCEPTION WHEN duplicate_object THEN NULL; END $$;
DO $$ BEGIN
    CREATE TRIGGER trg_orgs_touch         BEFORE UPDATE ON organizations FOR EACH ROW EXECUTE FUNCTION touch_updated_at();
EXCEPTION WHEN duplicate_object THEN NULL; END $$;
DO $$ BEGIN
    CREATE TRIGGER trg_projects_touch     BEFORE UPDATE ON projects     FOR EACH ROW EXECUTE FUNCTION touch_updated_at();
EXCEPTION WHEN duplicate_object THEN NULL; END $$;
DO $$ BEGIN
    CREATE TRIGGER trg_queues_touch       BEFORE UPDATE ON queues       FOR EACH ROW EXECUTE FUNCTION touch_updated_at();
EXCEPTION WHEN duplicate_object THEN NULL; END $$;
DO $$ BEGIN
    CREATE TRIGGER trg_jobs_touch         BEFORE UPDATE ON jobs         FOR EACH ROW EXECUTE FUNCTION touch_updated_at();
EXCEPTION WHEN duplicate_object THEN NULL; END $$;
DO $$ BEGIN
    CREATE TRIGGER trg_scheduled_touch    BEFORE UPDATE ON scheduled_jobs FOR EACH ROW EXECUTE FUNCTION touch_updated_at();
EXCEPTION WHEN duplicate_object THEN NULL; END $$;
DO $$ BEGIN
    CREATE TRIGGER trg_batches_touch      BEFORE UPDATE ON batches      FOR EACH ROW EXECUTE FUNCTION touch_updated_at();
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

-- =========================================================
-- BATCH COUNTER MAINTENANCE
-- =========================================================
CREATE OR REPLACE FUNCTION update_batch_on_job_complete()
RETURNS TRIGGER AS $$
DECLARE
    new_status job_status;
BEGIN
    IF NEW.status = OLD.status THEN RETURN NEW; END IF;
    IF NEW.batch_id IS NULL THEN RETURN NEW; END IF;

    IF NEW.status = 'COMPLETED' THEN
        UPDATE batches SET completed_jobs = completed_jobs + 1,
            status = CASE
                WHEN completed_jobs + 1 + failed_jobs >= total_jobs THEN 'COMPLETED'
                WHEN failed_jobs > 0 THEN 'PARTIALLY_COMPLETED'
                ELSE 'RUNNING'
            END
        WHERE id = NEW.batch_id;
    ELSIF NEW.status = 'FAILED' THEN
        UPDATE batches SET failed_jobs = failed_jobs + 1,
            status = CASE
                WHEN completed_jobs + failed_jobs + 1 >= total_jobs THEN 'FAILED'
                ELSE 'RUNNING'
            END
        WHERE id = NEW.batch_id;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DO $$ BEGIN
    CREATE TRIGGER trg_jobs_batch_counter
        AFTER UPDATE OF status ON jobs
        FOR EACH ROW
        WHEN (NEW.batch_id IS NOT NULL AND NEW.status IS DISTINCT FROM OLD.status)
        EXECUTE FUNCTION update_batch_on_job_complete();
EXCEPTION WHEN duplicate_object THEN NULL; END $$;
