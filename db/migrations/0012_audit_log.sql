-- Privileged-mutation audit trail + nothing else; token revocation arrives
-- via short-lived access tokens rotated by refresh tokens (no schema needed).

CREATE TABLE IF NOT EXISTS audit_log (
    id          BIGSERIAL PRIMARY KEY,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    actor_id    UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    org_id      UUID,
    action      TEXT NOT NULL,
    target      TEXT NOT NULL,
    details     JSONB NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS idx_audit_actor_time ON audit_log(actor_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_audit_org_time ON audit_log(org_id, created_at DESC)
    WHERE org_id IS NOT NULL;
