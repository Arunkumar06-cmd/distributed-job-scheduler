CREATE INDEX IF NOT EXISTS idx_jobs_waiting
    ON jobs(queue_id) WHERE status = 'WAITING';
