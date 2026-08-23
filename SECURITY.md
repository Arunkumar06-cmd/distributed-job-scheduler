# Security Policy

## Authentication model

- Passwords are hashed with Argon2id (per-hash random salt); verification
  failures never reveal whether the email exists.
- Access tokens are JWTs (HS256) with a **1-hour expiry**; refresh tokens
  (30-day) rotate them via `POST /auth/refresh`. A refresh token presented as
  an access token is rejected at validation time (`typ` claim check).
- All privileged mutations require an org-scoped role: **owner/admin** for
  configuration and membership changes, **member** for job submission, retry,
  DLQ replay, and schedules; **viewer** is read-only everywhere.
- Organization-owner memberships cannot be modified through the membership
  endpoint, preventing admins from locking out the last owner.

## Input handling

- All request bodies are deserialized through a typed extractor that converts
  malformed JSON, wrong types, depth bombs, and oversize payloads into a
  standard error envelope (400/413) — never plain-text rejections or 500s.
- Payload objects are capped at 256 KiB; string fields have per-field length
  limits; control characters (including NUL) are rejected.
- Idempotency keys are trimmed to ≤200 chars; empty keys become absent.
- SQL injection is structurally prevented by parameterized queries (sqlx);
  identifiers that reach NATS subjects are validated enums or UUIDs.

## Transport & runtime

- Internal errors (SQL, IO, driver) are logged server-side with full detail;
  clients receive only `{error:{code,message:"internal server error"},request_id}`
  with no stack traces, SQL text, or file paths.
- A per-user fixed-window rate limiter (`API_RATE_LIMIT_PER_MIN`, default 600)
  bounds runaway clients; 429 responses carry the standard envelope.
- CORS origins must be explicitly configured in production
  (`CORS_ALLOWED_ORIGIN`); `JWT_SECRET` must be set (≥32 bytes) when
  `RUST_ENV=production`.
- Privileged mutations (queue config/pause/resume, DLQ replay, membership
  upsert, manual retry) write an entry to `audit_log`.

## Known scope boundaries

- Rate limiting is per-instance; multi-replica deployments need a shared
  counter (Redis) for global enforcement.
- Refresh-token reuse detection is not implemented; stolen refresh tokens are
  valid until expiry (≤30 days). Short access tokens bound the blast radius of
  any single theft to one hour.

## Reporting a vulnerability

Email the maintainers via the address on your GitHub profile or open a
draft security advisory on this repository. Do not open public issues for
exploitable findings. Please include reproduction steps and affected routes.
