-- Track per-event delivery failures so the relay can exponentially back off
-- poison-pill events instead of retrying them on a fixed 30s drumbeat forever.
ALTER TABLE outbox_events ADD COLUMN IF NOT EXISTS publish_attempts INT NOT NULL DEFAULT 0;
