-- 0004_notify_fix.sql
-- Fix NOTIFY to use single channel queue_events per spec §18 (coalesced)
DROP TRIGGER IF EXISTS trg_jobs_notify ON jobs;
CREATE OR REPLACE FUNCTION notify_queue_job()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.status = 'QUEUED' THEN
        PERFORM pg_notify('queue_events', NEW.queue_id::text);
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER trg_jobs_notify AFTER INSERT OR UPDATE OF status ON jobs FOR EACH ROW WHEN (NEW.status = 'QUEUED') EXECUTE FUNCTION notify_queue_job();
