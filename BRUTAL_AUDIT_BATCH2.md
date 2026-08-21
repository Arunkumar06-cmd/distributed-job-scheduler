# BRUTAL AUDIT — BATCH 2: Central Grid + Job Inspector + Log Terminal

## Scope
- `frontend/src/App.jsx` lines 203-398 (CentralGrid, InspectorStepper, LogTerminal)
- `api/src/routes/jobs.rs` (list, get, retry, parse_status)
- `db/src/queries.rs` lines 1605-1652 (manual_retry_job)
- `worker/src/consumer.rs:319-324` (execute_handler job_type extraction)
- `worker/src/handler.rs` (EchoHandler, AlwaysFailHandler, SleepHandler, ExternalPaymentHandler)

---

## CRITICAL BUGS (found + fixed)

### BUG 7: `manual_retry_job` returns `500 Internal` on non-FAILED job
**Severity:** P0 — retry button crashes with `500 Internal Server Error`
**Root cause:** `db/src/queries.rs:1629` used `fetch_one()` which panics with `"no rows returned by a query that expected to return at least one row"` when the job is not in `FAILED` or `RETRY_WAIT` status. The `WHERE status IN ('FAILED','RETRY_WAIT')` clause filters out COMPLETED jobs, so `fetch_one` gets 0 rows → `sqlx::Error::RowNotFound` → `500 Internal`.
**Fix:** Changed `fetch_one()` → `fetch_optional()`, then `ok_or_else(|| AppError::Conflict(...))`. Now returns `409 Conflict: "job is not in FAILED or RETRY_WAIT status"`.
**Status:** FIXED in `db/src/queries.rs:1629-1635`

### BUG 8: Status filter case mismatch — `RetryWait` returns ALL jobs
**Severity:** P1 — filter dropdown silently ignored
**Root cause:** `api/src/routes/jobs.rs:144` `parse_status()` did `s.to_uppercase()` but `RetryWait` → `RETRYWAIT` (no underscore) ≠ `RETRY_WAIT`. The match arm `"RETRY_WAIT"` didn't match, so `parse_status` returned `None`, and `list_jobs` with `status=None` returned ALL jobs unfiltered.
**Fix:** Added `"RETRYWAIT"` as alternate match arm. Also added `"WAITING"` and `"UNKNOWN"` / `"UNKNOWN_EXTERNAL_RESULT"` for completeness. Added `.replace('-', "_")` for hyphenated variants.
**Status:** FIXED in `api/src/routes/jobs.rs:144-158`

### BUG 9: Frontend `stateMap` keys don't match API PascalCase status
**Severity:** P1 — all job status badges show fallback gray
**Root cause:** API returns `"status": "Completed"` (PascalCase from serde default). Frontend `stateMap` used SCREAMING_SNAKE keys (`COMPLETED`, `RETRY_WAIT`, `UNKNOWN_EXTERNAL_RESULT`). `stateMap[j.status]` → `stateMap["Completed"]` → `undefined` → fallback `{bg:'#27272a', fg:'#a1a1aa', label:j.status}`. All badges were gray with raw PascalCase label.
**Fix:** Added `statusKey = (j.status||'').toUpperCase().replace('-','_')` normalization before lookup. Added both `RETRY_WAIT` and `RETRYWAIT` keys. Same for `UNKNOWNEXTERNALRESULT`.
**Status:** FIXED in `App.jsx:301-312`

### BUG 10: `localStorage` job-lock guardrail is fake complexity
**Severity:** P2 — confusing UX, blocks legitimate retries
**Root cause:** `CentralGrid.open()` set `localStorage['job-lock:{id}']` on every job click. `retry()` checked this and showed `alert('🔒 Locked in Parallel Tab — wait 5s')` if within 5s of clicking. This is not a real distributed lock — it's a localStorage trick that blocks the user from retrying a job they just clicked on. `InspectorStepper` also disabled the Evict button based on this fake lock.
**Fix:** Removed all `localStorage` job-lock logic from `open()`, `retry()`, `evict()`, and `InspectorStepper`. The Evict button is now always enabled. Real concurrency safety comes from the DB `SELECT FOR UPDATE SKIP LOCKED` + lease fencing, not browser localStorage.
**Status:** FIXED in `App.jsx:240-272, 342-377`

### BUG 11: Orphan detection case mismatch
**Severity:** P2 — orphaned jobs not flagged red
**Root cause:** `j.status==='RUNNING'` checked PascalCase but API returns `Running`. Orphan detection never triggered.
**Fix:** Changed to `(j.status||'').toUpperCase()==='RUNNING'`.
**Status:** FIXED in `App.jsx:314`

### BUG 12: InspectorStepper status checks case mismatch
**Severity:** P2 — stepper labels show raw PascalCase
**Root cause:** `job.status==='COMPLETED'` / `job.status==='FAILED'` / `job.status==='UNKNOWN_EXTERNAL_RESULT'` all checked SCREAMING_SNAKE but API returns PascalCase. The DLQ badge never showed. The final step label showed raw `Completed` instead of `COMPLETED`.
**Fix:** All status comparisons normalized with `.toUpperCase()`.
**Status:** FIXED in `App.jsx:348, 363`

---

## REMAINING FLAWS (not fixed in this batch)

### FLAW 8: No "Create Job" button in CentralGrid
The empty state says `"No jobs — create one via "Create Job" or wait for cron."` but there is no Create Job button anywhere in the UI. Jobs can only be created via `curl POST /jobs`.
**Impact:** High — user can't create jobs from the dashboard.

### FLAW 9: `execute_handler` defaults to `"echo"` when no handler found
`worker/src/consumer.rs:329` falls back to `EchoHandler` when `job_type` doesn't match any registered handler. This means `{"task":"fail"}` silently succeeds as echo instead of failing. The `AlwaysFailHandler` is registered as `"always_fail"` but the frontend sends `{"task":"fail"}` (no `type` field).
**Impact:** Medium — `{"task":"fail"}` succeeds instead of failing. Demo of retry/DLQ requires `{"type":"always_fail"}`.

### FLAW 10: `1.5s` polling interval on CentralGrid
`App.jsx:237` polls `/jobs?queue_id=...&page_size=100` every 1.5s. With 100 jobs, that's 100 rows re-rendered every 1.5s even if nothing changed.
**Impact:** Medium — CPU waste, should use WebSocket or diff-based updates.

### FLAW 11: `frozen` mode merge logic is O(n²)
`App.jsx:226` does `data.forEach(j=>{ if(!merged.find(m=>m.id===j.id)) merged.push(j) })` — `find()` inside `forEach` is O(n²) with 100 jobs = 10,000 comparisons per poll.
**Impact:** Low for 100 jobs, high for 10,000.

### FLAW 12: Log terminal `slice(-100)` on every render
`App.jsx:389` does `logs.slice(-100)` on every render. If `logs` is 10,000 entries, this creates a new 100-element array every render.
**Impact:** Low — should memoize with `useMemo`.

### FLAW 13: `InspectorStepper` time delta calculation can be `NaN`
`App.jsx:360` does `Math.round((new Date(steps[i+1].ts)-new Date(s.ts)))` — if either `ts` is null (filtered out by `.filter(s=>s.ts)` but `steps[i+1].ts` could be undefined), this produces `NaN`.
**Impact:** Low — shows `NaNms` in the arrow.

### FLAW 14: No job creation form
The entire CentralGrid has no way to create a job from the UI. The `Create Job` mentioned in the empty state doesn't exist. This is the biggest UX gap.
**Impact:** Critical for demo — must use `curl` to create jobs.

---

## VERIFICATION

After all fixes:
- `POST /jobs/{id}/retry` on COMPLETED job → `409 Conflict: "job is not in FAILED or RETRY_WAIT status"` (was `500 Internal`)
- `GET /jobs?status=RetryWait` → `total: 0` (was `total: 3` — returned all jobs)
- `GET /jobs?status=RETRY_WAIT` → `total: 0` (correct, no RETRY_WAIT jobs)
- `GET /jobs?status=FAILED` → `total: 1` (correct)
- Status badges now show colored labels (`🟢 COMPLETED`, `🔴 DLQ_FAULT`, etc.) instead of gray fallback
- Orphan detection works with PascalCase `Running`
- InspectorStepper DLQ badge shows for `Failed` status
- No more `localStorage` fake lock — Evict button always enabled
- Frontend build: `169KB` (was `167KB` — added status normalization)
- `vite ready 407ms`
