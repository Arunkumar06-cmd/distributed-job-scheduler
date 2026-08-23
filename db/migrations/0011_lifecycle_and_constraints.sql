-- Hot/cold separation and remaining integrity constraints.
--
-- Archiving moves a terminal job AND its dependency family (executions, logs,
-- DLQ entries): those tables reference jobs with ON DELETE RESTRICT, so a
-- parent cannot leave the hot table behind its children.

CREATE TABLE IF NOT EXISTS jobs_archive (
    LIKE jobs INCLUDING DEFAULTS,
    PRIMARY KEY (id)
);
CREATE INDEX IF NOT EXISTS idx_jobs_archive_queue_created
    ON jobs_archive(queue_id, created_at DESC);

CREATE TABLE IF NOT EXISTS job_executions_archive (
    LIKE job_executions INCLUDING DEFAULTS,
    PRIMARY KEY (id)
);
CREATE INDEX IF NOT EXISTS idx_executions_archive_job
    ON job_executions_archive(job_id, started_at DESC);

CREATE TABLE IF NOT EXISTS job_logs_archive (
    LIKE job_logs INCLUDING DEFAULTS,
    PRIMARY KEY (id)
);
CREATE INDEX IF NOT EXISTS idx_logs_archive_job
    ON job_logs_archive(job_id, created_at DESC);

-- DLQ rows keep their own identity in the archive (no FK back to jobs).
CREATE TABLE IF NOT EXISTS dead_letter_entries_archive (
    LIKE dead_letter_entries INCLUDING DEFAULTS,
    PRIMARY KEY (id)
);

-- Moves up to $2 terminal jobs older than $1, plus their executions/logs/DLQ
-- rows, into the archive twins. Single transaction; returns jobs moved.
-- Callers pass small limits so each invocation stays short.
CREATE OR REPLACE FUNCTION archive_terminal_jobs(older_than INTERVAL, batch_size INT)
RETURNS INT AS $$
DECLARE
    r RECORD;
    moved INT := 0;
BEGIN
    -- Per-row processing keeps parent/child move order explicit; no temp
    -- tables, so behavior is identical across pooled sessions.
    FOR r IN
        SELECT id FROM jobs
        WHERE status IN ('COMPLETED', 'FAILED', 'CANCELLED')
          AND updated_at < NOW() - older_than
          AND NOT EXISTS (
              SELECT 1 FROM dead_letter_entries d
              WHERE d.job_id = jobs.id AND d.replayed_to_job_id IS NULL
          )
        ORDER BY updated_at ASC
        LIMIT batch_size
    LOOP
        INSERT INTO jobs_archive SELECT * FROM jobs WHERE id = r.id;
        INSERT INTO job_executions_archive SELECT * FROM job_executions WHERE job_id = r.id;
        INSERT INTO job_logs_archive SELECT * FROM job_logs WHERE job_id = r.id;
        INSERT INTO dead_letter_entries_archive SELECT * FROM dead_letter_entries WHERE job_id = r.id;

        DELETE FROM job_logs WHERE job_id = r.id;
        DELETE FROM job_executions WHERE job_id = r.id;
        DELETE FROM dead_letter_entries WHERE job_id = r.id;
        DELETE FROM jobs WHERE id = r.id;

        moved := moved + 1;
    END LOOP;
    RETURN moved;
END; $$ LANGUAGE plpgsql;

-- scheduled_jobs.job_type was free text; constrain new writes to the job-kind
-- vocabulary. NOT VALID: legacy rows (e.g. demo 'echo') stay readable while
-- every future insert/update must conform.
DO $$ BEGIN
    ALTER TABLE scheduled_jobs ADD CONSTRAINT chk_scheduled_jobs_job_type
        CHECK (job_type IN ('immediate', 'delayed', 'scheduled', 'recurring', 'batch')) NOT VALID;
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

-- Same treatment for the workflows.status flag.
DO $$ BEGIN
    ALTER TABLE workflows ADD CONSTRAINT chk_workflows_status
        CHECK (status IN ('RUNNING', 'COMPLETED', 'FAILED')) NOT VALID;
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

-- Job-log retention helper (mirrors prune_worker_heartbeats).
CREATE OR REPLACE FUNCTION prune_job_logs(older_than INTERVAL)
RETURNS BIGINT AS $$
DECLARE n BIGINT;
BEGIN
    DELETE FROM job_logs WHERE created_at < NOW() - older_than;
    GET DIAGNOSTICS n = ROW_COUNT;
    RETURN n;
END;
$$ LANGUAGE plpgsql;
