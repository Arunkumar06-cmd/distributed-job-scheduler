# BRUTAL AUDIT — BATCH 1: Project Context Switcher + Queue Topology

## Scope
- `frontend/src/App.jsx` lines 146-201 (LeftPanel), 400-431 (CreateOrg/Project/Queue)
- `frontend/vite.config.js` (proxy)
- `api/src/routes/organizations.rs`, `api/src/routes/projects.rs`
- `db/src/queries.rs` lines 51-91 (create_organization, list_organizations_for_user)
- `domain/src/worker.rs` (WorkerStatus serde)

---

## CRITICAL BUGS (found + fixed)

### BUG 1: Vite proxy missing `/auth` route → `Not Found` on Register
**Severity:** P0 — blocks all auth
**Root cause:** `vite.config.js` only proxied `/api`, `/health`, `/metrics`. Frontend does `fetch('/auth/register')` → Vite returns `404 Not Found` → `Error: Not Found`.
**Fix:** Added `/auth`, `/organizations`, `/projects`, `/queues`, `/jobs`, `/workers`, `/dlq`, `/batches`, `/scheduled-jobs`, `/workflows`, `/events`, `/ws` to proxy config.
**Status:** FIXED in `vite.config.js`

### BUG 2: `WorkerStatus` serde mismatch → `Workers: 0/42 Online`
**Severity:** P1 — dashboard shows 0 workers even when worker is running
**Root cause:** `domain/src/worker.rs` had `#[sqlx(rename_all="UPPERCASE")]` but no `#[serde(rename_all="UPPERCASE")]`. SQL returned `ONLINE` (correct), but serde serialized as `Online` (PascalCase). Frontend checked `w.status==='ONLINE'` → never matched → `0`.
**Fix:** Added `#[serde(rename_all = "UPPERCASE")]` to `WorkerStatus` enum.
**Status:** FIXED in `domain/src/worker.rs`

### BUG 3: `Error: {}` on Register (empty error message)
**Severity:** P1 — user sees blank error
**Root cause:** `api()` function in `App.jsx:15` threw `new Error(JSON.stringify(j).slice(0,200))` — when server returned `{error:{message:"..."}}`, it stringified the whole object as `{}` or `[object Object]`.
**Fix:** Rewrote `api()` to extract `j.error.message` properly and attach `err.status` to the Error. Updated `AuthScreen.submit` catch to show real server message.
**Status:** FIXED in `App.jsx:11-18`

### BUG 4: `+ Org`/`+ Project`/`+ Queue` invisible (hidden in `<details>`)
**Severity:** P2 — user can't find create buttons
**Root cause:** Used `<details><summary>+ Org</summary>` — collapsed by default, tiny text, looks like a label not a button. User couldn't find them.
**Fix:** Replaced with visible `<button>` that expands to inline form on click. Added error handling, cancel button, disabled state when no org/project selected.
**Status:** FIXED in `App.jsx:400-431`

### BUG 5: `onRefresh` callback chain broken — new orgs/projects/queues not loaded after create
**Severity:** P2 — create succeeds but UI doesn't update
**Root cause:** All three Create components called `onRefresh` which was `loadMetrics` (workers/metrics only, not orgs/projects/queues). No separate refresh functions existed.
**Fix:** Added `refreshOrgs`, `refreshProjects`, `refreshQueues` useCallback functions. Wired `CreateOrg.onDone={onRefreshOrgs}`, `CreateProject.onDone={onRefreshProjects}`, `CreateQueue.onDone={onRefreshQueues}`.
**Status:** FIXED in `App.jsx:46-84, 108`

### BUG 6: `admin@example.com` has 0 org memberships → `GET /organizations` returns `[]`
**Severity:** P2 — dashboard shows "no org" for seed user
**Root cause:** 23 orgs exist in DB but all created by test users. `admin@example.com` (seed user) has 0 rows in `org_memberships`. `list_organizations_for_user` correctly filters by membership.
**Fix:** Inserted `Demo Org` + `Demo Project` + `default` queue directly into DB with `created_by=admin@example.com`'s UUID + `org_memberships` row.
**Status:** FIXED via DB insert (not code — code was correct)

---

## REMAINING FLAWS (not fixed in this batch)

### FLAW 1: `useEffect` double-fetch on mount
`App.jsx:86-92` has both `loadOrgs()` in the mount effect AND `refreshProjects()`/`refreshQueues()` as separate effects. On first render, `loadOrgs` fires, sets `selOrg`, which triggers `refreshProjects`, which sets `selProj`, which triggers `refreshQueues`. Three sequential fetches that could be one.
**Impact:** Minor — 3 requests instead of 1 on page load. Not user-visible.

### FLAW 2: `qStats` polling creates N intervals per queue
`App.jsx:93-101` creates a `setInterval` that fetches `/queues/{id}/stats` for EVERY queue every 3s. With 27 queues, that's 27 HTTP requests every 3s = 9 req/s just for stats.
**Impact:** Medium — wastes bandwidth, could overwhelm API with many queues. Should batch into single `/queues/stats` endpoint.

### FLAW 3: `togglePause` uses `alert()` for errors
`App.jsx:151` catches errors with `alert(String(e))` — blocks UI, ugly, not dismissible.
**Impact:** Minor UX — should use inline error state like Create components do.

### FLAW 4: No loading states anywhere
No spinners, no "Loading..." text. When orgs/projects/queues are fetching, dropdowns show "— no org —" which looks like an error.
**Impact:** Medium UX — user thinks nothing exists during the 200ms fetch window.

### FLAW 5: `CreateOrg` slug not auto-generated from name
User must type both `name` and `slug` manually. Should auto-slug from name (e.g. "Demo Org" → "demo-org").
**Impact:** Minor UX — extra typing, risk of slug conflicts.

### FLAW 6: `500 Internal` on duplicate org slug
`create_organization` returns `duplicate key value violates unique constraint` → API returns `500 Internal` instead of `409 Conflict`.
**Impact:** Medium — frontend can't distinguish "slug taken" from "server broken". Should catch `23505` unique violation and return `409`.

### FLAW 7: `LeftPanel` is 55 lines of inline styles
Every element has `style={{...}}` with hardcoded hex colors. No CSS classes, no theme. Changing `#09090b` requires find-replace across 50+ locations.
**Impact:** Low for demo, high for maintainability.

---

## VERIFICATION

After all fixes:
- `GET /organizations` → `[{name:"Demo Org",...}]` (1 org, owned by admin)
- `GET /projects?org_id=...` → `[{name:"Demo Project",...}]` (1 project)
- `GET /queues?project_id=...` → `[{name:"default",max_concurrency:5,...}]` (1 queue)
- `GET /workers` → `[{status:"ONLINE",...}]` (serde now UPPERCASE)
- `POST /auth/register` via `:3000` → `201` (Vite proxy fixed)
- `POST /auth/login` via `:3000` → `200` (Vite proxy fixed)
- Frontend build: `167KB → 169KB` (new Create components)
- `vite ready 420ms` — proxy config loaded
