-- 0003_dag_and_waiting.sql
-- Add WAITING status for DAG children
DO $$ BEGIN
    ALTER TYPE job_status ADD VALUE IF NOT EXISTS 'WAITING';
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

-- Add workflow metadata
CREATE TABLE IF NOT EXISTS workflows (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id      UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    name            TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'RUNNING',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE jobs ADD COLUMN IF NOT EXISTS workflow_id UUID REFERENCES workflows(id) ON DELETE SET NULL;

-- Ensure DAG waiting jobs have a way to be queued
CREATE INDEX IF NOT EXISTS idx_jobs_waiting ON jobs(queue_id) WHERE status = 'WAITING';
