-- Queue-created events let workers attach consumers immediately instead of
-- polling the queues table on an interval.
CREATE OR REPLACE FUNCTION notify_queue_created()
RETURNS TRIGGER AS $$
BEGIN
    PERFORM pg_notify('queue_created', NEW.id::text);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_queues_created ON queues;
CREATE TRIGGER trg_queues_created AFTER INSERT ON queues
    FOR EACH ROW EXECUTE FUNCTION notify_queue_created();
