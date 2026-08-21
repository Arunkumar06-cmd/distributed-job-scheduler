-- PostgreSQL requires this enum value to commit before another statement can
-- reference it. Keep it in its own migration transaction.
ALTER TYPE job_status ADD VALUE IF NOT EXISTS 'WAITING';
