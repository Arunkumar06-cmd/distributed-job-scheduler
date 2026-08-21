-- Deterministic queue sharding. Existing queues and jobs remain on shard zero.
ALTER TABLE queues ADD COLUMN IF NOT EXISTS shard_count INTEGER NOT NULL DEFAULT 1
    CHECK (shard_count >= 1 AND shard_count <= 128);
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS shard_id INTEGER NOT NULL DEFAULT 0
    CHECK (shard_id >= 0);
CREATE INDEX IF NOT EXISTS idx_jobs_queue_shard_status
    ON jobs(queue_id, shard_id, status);
