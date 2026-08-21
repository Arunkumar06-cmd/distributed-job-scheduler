-- 0002_capacity_tokens_and_workflow.sql
-- HOT tokens, workflow DAG, rate limiting, UNKNOWN, AI summaries

-- Capacity tokens for global concurrency (spec §7)
CREATE TABLE IF NOT EXISTS capacity_tokens (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    queue_id        UUID NOT NULL REFERENCES queues(id) ON DELETE CASCADE,
    slot_index      INTEGER NOT NULL CHECK (slot_index >= 0),
    worker_id       UUID REFERENCES workers(id) ON DELETE SET NULL,
    job_id          UUID REFERENCES jobs(id) ON DELETE SET NULL,
    lease_until     TIMESTAMPTZ,
    epoch           BIGINT NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(queue_id, slot_index)
);
CREATE INDEX IF NOT EXISTS idx_tokens_queue_free ON capacity_tokens(queue_id) WHERE worker_id IS NULL;
CREATE INDEX IF NOT EXISTS idx_tokens_lease ON capacity_tokens(lease_until) WHERE lease_until IS NOT NULL;

-- Backfill tokens for existing queues
INSERT INTO capacity_tokens (queue_id, slot_index)
SELECT q.id, gs.slot
FROM queues q, generate_series(0, q.max_concurrency - 1) AS gs(slot)
ON CONFLICT DO NOTHING;

-- Keep tokens in sync with queue max_concurrency
CREATE OR REPLACE FUNCTION sync_capacity_tokens()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.max_concurrency > OLD.max_concurrency THEN
        INSERT INTO capacity_tokens (queue_id, slot_index)
        SELECT NEW.id, gs.slot FROM generate_series(OLD.max_concurrency, NEW.max_concurrency - 1) AS gs(slot)
        ON CONFLICT DO NOTHING;
    ELSIF NEW.max_concurrency < OLD.max_concurrency THEN
        DELETE FROM capacity_tokens WHERE queue_id = NEW.id AND slot_index >= NEW.max_concurrency AND worker_id IS NULL;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_queues_sync_tokens ON queues;
CREATE TRIGGER trg_queues_sync_tokens AFTER UPDATE OF max_concurrency ON queues FOR EACH ROW EXECUTE FUNCTION sync_capacity_tokens();

-- Insert trigger handled via separate function that seeds all tokens
CREATE OR REPLACE FUNCTION seed_capacity_tokens()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO capacity_tokens (queue_id, slot_index)
    SELECT NEW.id, gs.slot FROM generate_series(0, NEW.max_concurrency - 1) AS gs(slot)
    ON CONFLICT DO NOTHING;
    INSERT INTO queue_rate_buckets (queue_id) VALUES (NEW.id) ON CONFLICT DO NOTHING;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS trg_queues_seed_tokens ON queues;
CREATE TRIGGER trg_queues_seed_tokens AFTER INSERT ON queues FOR EACH ROW EXECUTE FUNCTION seed_capacity_tokens();

-- Rate limiting per queue (bonus §2)
ALTER TABLE queues ADD COLUMN IF NOT EXISTS rate_limit INTEGER CHECK (rate_limit IS NULL OR rate_limit > 0);
ALTER TABLE queues ADD COLUMN IF NOT EXISTS rate_window_secs INTEGER NOT NULL DEFAULT 60;

CREATE TABLE IF NOT EXISTS queue_rate_buckets (
    queue_id        UUID PRIMARY KEY REFERENCES queues(id) ON DELETE CASCADE,
    tokens          DOUBLE PRECISION NOT NULL DEFAULT 0,
    last_refill_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Workflow DAG (bonus §1, spec §26-28)
CREATE TABLE IF NOT EXISTS workflow_edges (
    parent_id       UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    child_id        UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (parent_id, child_id),
    CHECK (parent_id != child_id)
);

CREATE TABLE IF NOT EXISTS edge_satisfaction (
    parent_id       UUID NOT NULL,
    child_id        UUID NOT NULL,
    satisfied_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (parent_id, child_id),
    FOREIGN KEY (parent_id, child_id) REFERENCES workflow_edges(parent_id, child_id) ON DELETE CASCADE
);

-- Prevent cycles via trigger (simple check, deeper cycle detection in app)
CREATE OR REPLACE FUNCTION check_workflow_cycle()
RETURNS TRIGGER AS $$
BEGIN
    IF EXISTS (WITH RECURSIVE search(parent_id, child_id, depth, path, cycle) AS (
        SELECT parent_id, child_id, 1, ARRAY[parent_id], false FROM workflow_edges WHERE child_id = NEW.parent_id
        UNION ALL
        SELECT e.parent_id, e.child_id, s.depth+1, path || e.parent_id, e.parent_id = ANY(path) FROM workflow_edges e JOIN search s ON s.parent_id = e.child_id WHERE NOT cycle
    ) SELECT 1 FROM search WHERE child_id = NEW.child_id AND cycle) THEN
        RAISE EXCEPTION 'workflow cycle detected';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS trg_workflow_no_cycle ON workflow_edges;
CREATE TRIGGER trg_workflow_no_cycle BEFORE INSERT ON workflow_edges FOR EACH ROW EXECUTE FUNCTION check_workflow_cycle();

-- UNKNOWN external result (spec §25)
DO $$ BEGIN
    ALTER TYPE job_status ADD VALUE IF NOT EXISTS 'UNKNOWN_EXTERNAL_RESULT';
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

-- AI failure summaries (bonus §8)
CREATE TABLE IF NOT EXISTS failure_summaries (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    dlq_id          UUID NOT NULL REFERENCES dead_letter_entries(id) ON DELETE CASCADE,
    job_id          UUID NOT NULL,
    summary         TEXT NOT NULL,
    root_cause      TEXT,
    remediation     TEXT,
    model           TEXT NOT NULL DEFAULT 'mock-llm',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(dlq_id)
);

-- Hot/history separation helpers
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS token_id UUID REFERENCES capacity_tokens(id) ON DELETE SET NULL;

-- NOTIFY trigger for event-driven wakeup (spec §18)
CREATE OR REPLACE FUNCTION notify_queue_job()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.status = 'QUEUED' THEN
        PERFORM pg_notify('queue:' || NEW.queue_id::text, NEW.id::text);
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_jobs_notify ON jobs;
CREATE TRIGGER trg_jobs_notify AFTER INSERT OR UPDATE OF status ON jobs FOR EACH ROW WHEN (NEW.status = 'QUEUED') EXECUTE FUNCTION notify_queue_job();

-- No updated_at trigger for capacity_tokens (no updated_at column)
