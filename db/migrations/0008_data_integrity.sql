-- 0008_data_integrity.sql
-- Referential integrity + index gaps found in audit.

-- jobs.batch_id previously had no foreign key: deleting a batch orphaned its
-- jobs and batch counters could drift from reality. SET NULL so history
-- survives batch deletion.
DO $$ BEGIN
    ALTER TABLE jobs ADD CONSTRAINT fk_jobs_batch
        FOREIGN KEY (batch_id) REFERENCES batches(id) ON DELETE SET NULL;
EXCEPTION WHEN duplicate_object THEN NULL; WHEN duplicate_table THEN NULL; END $$;

-- Rate limiting counts jobs created inside the window per queue; this composite
-- index turns that COUNT into an index-only range scan.
CREATE INDEX IF NOT EXISTS idx_jobs_queue_created_at
    ON jobs(queue_id, created_at DESC);

-- Backfill's NOT EXISTS anti-join probes outbox_events by job_id.
CREATE INDEX IF NOT EXISTS idx_outbox_job
    ON outbox_events(job_id);

-- Emails are unique case-insensitively; the plain UNIQUE constraint is replaced
-- so Foo@ex.com and foo@ex.com cannot be two accounts.
DROP INDEX IF EXISTS idx_users_email_lower;
CREATE UNIQUE INDEX IF NOT EXISTS idx_users_email_lower
    ON users (LOWER(email));
DO $$
DECLARE c text;
BEGIN
    SELECT conname INTO c FROM pg_constraint
      WHERE conrelid = 'users'::regclass AND contype = 'u' AND conname = 'users_email_key';
    IF c IS NOT NULL THEN
        ALTER TABLE users DROP CONSTRAINT users_email_key;
    END IF;
END $$;

-- Heartbeat retention helper: worker_heartbeats grows unbounded otherwise.
CREATE OR REPLACE FUNCTION prune_worker_heartbeats(older_than INTERVAL)
RETURNS BIGINT AS $$
DECLARE n BIGINT;
BEGIN
    DELETE FROM worker_heartbeats WHERE heartbeat_at < NOW() - older_than;
    GET DIAGNOSTICS n = ROW_COUNT;
    RETURN n;
END;
$$ LANGUAGE plpgsql;
