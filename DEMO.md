# Dashboard Demo Recording Runbook

This is a repeatable 4–6 minute walkthrough for a GitHub release or project
submission. Record a real session; do not use synthetic placeholder clips.

## 1. Prepare a clean environment

Use three terminals from the repository root.

```bash
docker compose up -d postgres nats

DATABASE_URL=postgres://postgres:postgres@127.0.0.1:5432/job_scheduler \
NATS_URL=nats://127.0.0.1:4222 \
cargo run -p api
```

```bash
DATABASE_URL=postgres://postgres:postgres@127.0.0.1:5432/job_scheduler \
NATS_URL=nats://127.0.0.1:4222 \
cargo run -p worker
```

```bash
cd frontend
npm ci
npm run dev
```

Before recording, verify `http://localhost:8080/health` returns `200` and open
`http://localhost:3000` at 1440×900 or larger. Hide terminals and browser
bookmarks. Use a fresh email address so the recording contains no personal
data.

## 2. Recording outline

### 0:00–0:30 — orient the viewer

Show the login/register screen, then register a new user. Create one
organization and project. State plainly that PostgreSQL is the system of
record and NATS JetStream delivers work to independently running workers.

### 0:30–1:15 — create a controlled queue

Create a queue named `orders-demo` with concurrency `2`. Show its queue health
card and explain that pause/resume and the concurrency limit are queue-scoped.
If the UI exposes retry configuration, select exponential backoff with a small
attempt limit suitable for the demo.

### 1:15–2:00 — immediate and delayed work

Create one immediate echo job. In the job explorer, show its transition from
`QUEUED` through execution to `COMPLETED`; then open its detail view and show
the execution record and log. Create a second job with a near-future schedule
and show that it stays `SCHEDULED` until due.

### 2:00–2:45 — queue control and concurrency

Pause `orders-demo`, submit a job, and show that it remains queued. Resume the
queue and show it complete. Submit several short-running jobs and point out
that the running count never exceeds `2`.

### 2:45–3:35 — failure, retry, and DLQ

Create a job that intentionally fails using the demo handler supported by the
running worker. Show one retry attempt, then the terminal failed state and its
DLQ entry. Open the DLQ view and demonstrate replay only if the replayed job is
safe to run again; explain that handlers must use the job ID as their external
idempotency key.

### 3:35–4:15 — operations view

Show the workers page with heartbeat status, then return to the queue card and
job explorer. Call out that jobs retain timestamps, worker assignment,
execution history, and logs for diagnosis.

### 4:15–4:45 — close with reliability boundaries

Finish on the architecture diagram in the README. Mention the transactional
outbox, lease-epoch fencing, and at-least-once delivery. Do not claim exactly
once execution or production certification; external effects remain
idempotency-sensitive and deployment/load/soak evidence belongs in CI and
operations documentation.

## 3. Capture and publish

1. Record at 1080p, 30 fps, with system notifications disabled.
2. Trim dead time and redact tokens, email addresses, hostnames, and terminal
   history. Aim for 4–6 minutes.
3. Export H.264 MP4 with a descriptive filename such as
   `distributed-job-scheduler-demo.mp4`.
4. Upload the video to a GitHub Release or YouTube/Vimeo as unlisted, then add
   the permanent link to the README's **Demo** section.
5. Keep the source recording out of Git history unless it is intentionally
   small and useful to clone; release assets are preferred for repository
   hygiene.

## 4. Pre-publish checklist

- [ ] No credentials, JWTs, database URLs, or personal information appear.
- [ ] The displayed states match the actual API and dashboard behavior.
- [ ] Queue concurrency, retry/DLQ, and worker status are visible.
- [ ] The description states at-least-once delivery and idempotent handlers.
- [ ] The README links to the final, working recording.
