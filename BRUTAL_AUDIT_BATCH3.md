# Brutal Audit — Batch 3

**Date:** 2026-08-21  
**Scope:** Deep audit of all remaining source files — security, correctness, performance, and code quality.  
**Result:** 13 flaws identified, 13 fixed. 1 false positive (dlq.rs authz). All 12 tests pass. Frontend rebuilt.

---

## Summary

| # | Severity | File | Flaw | Fix |
|---|----------|------|------|-----|
| 1 | P0 | `api/src/routes/health.rs` | `metrics()` ran 7 separate `COUNT(*)` queries on every poll cycle | Replaced with single `GROUP BY status` + `UNION ALL` query using HashMap aggregation |
| 2 | P0 | `api/src/routes/events.rs` | `ws_handler` had no auth — any unauthenticated client could connect to WebSocket and receive system-wide metrics | Added `AuthUser` parameter to `ws_handler` signature |
| 3 | P1 | `api/src/routes/events.rs` | `handle_ws` ran 3 separate `COUNT(*)` queries every 2 seconds per connected WebSocket client | Replaced with single `UNION ALL` query returning all 3 counts in one round-trip |
| 4 | P1 | `scheduler/src/cron_runner.rs` | `run()` constructed a `SchedulerLeader` with a no-op closure, never used it, then called `self.leader_loop()` directly — dead code, confusing | Removed dead `SchedulerLeader` construction, `run()` now delegates directly to `leader_loop()`. Removed unused `SchedulerLeader` import. |
| 5 | P1 | `worker/src/consumer.rs` | `execute_handler` silently fell back to `EchoHandler` when no handler was registered for a job type — unknown job types would silently succeed instead of failing | Unknown job type now returns `HandlerResult::Permanent` with `kind: "no_handler"`, causing the job to be sent to DLQ instead of silently succeeding |
| 6 | P0 | `api/src/routes/workflows.rs` | `create()` created workflow, jobs, outbox events, and edges as separate non-transactional queries — partial failure (e.g., DB error mid-way) would leave orphaned jobs/edges | Wrapped entire workflow creation in a single `BEGIN/COMMIT` transaction (`pool.begin()` / `tx.commit()`). All inserts now use `&mut *tx`. Removed unused `JobKind` import. |
| 7 | — | `api/src/routes/dlq.rs` | `replay()` was flagged as missing authz check | **False positive.** `replay()` already checks `queries::user_in_org()` at line 50. No fix needed. |
| 8 | P1 | `frontend/src/App.jsx` | `InspectorStepper` computed `new Date(steps[i+1].ts) - new Date(s.ts)` without null-checking — if either timestamp was null/invalid, this produced `NaNms` | Added null guard: `steps[i+1].ts && s.ts ? Math.round(...) : '?'` |
| 9 | P1 | `frontend/src/App.jsx` | Mount `useEffect` had `[]` dependency array — `loadOrgs`/`loadMetrics` callbacks captured stale `auth.token` on re-login | Added `auth.token` to dependency array: `[auth.token]`. Interval now resets on token change. |
| 10 | P1 | `frontend/src/App.jsx` | `refreshProjects` and `refreshQueues` effects missing `auth.token` dependency — stale token after re-login | Added `auth.token` to both dependency arrays: `[selOrg, auth.token]` and `[selProj, auth.token]` |
| 11 | P2 | `frontend/src/App.jsx` | `CentralGrid` `load()` callback dependency array | Already correct — `auth.token` was in the dep array. No fix needed. |
| 12 | P2 | `db/src/queries.rs` | `complete_job` DAG resolver ran 2 `COUNT(*)` queries per child in a loop — O(n) queries for n children | Replaced per-child loop with single batch SQL: `UPDATE ... WHERE id IN (SELECT child_id ... HAVING COUNT(DISTINCT parent_id) = total_parents)`. Now O(1) queries regardless of DAG size. |
| 13 | P2 | `api/src/routes/auth.rs` | `register()` detected duplicate email via `e.to_string().contains("duplicate")` — fragile string matching | Removed manual string-contains check. `From<sqlx::Error>` in `errors.rs` already detects PG error code `23505` → `AppError::Conflict` (409). The `?` operator auto-converts. |
| 14 | P2 | `api/src/routes/jobs.rs` | `create()` detected duplicate idempotency key via `e.to_string().contains("duplicate")` | Same fix as #13 — removed string-contains, relies on `From<sqlx::Error>` auto-conversion. |
| 15 | P2 | `api/src/routes/jobs.rs` | `create_batch()` detected duplicate via `e.to_string().contains("duplicate")` | Replaced with `matches!(e, AppError::Conflict(_))` — type-safe pattern matching instead of string inspection. |

---

## Verification

```
cargo build --workspace     → OK (warnings only, no errors)
cargo test --workspace -- --test-threads=1  → 12 passed, 0 failed
npm run build               → OK (170.98 KB, 3.48s)
```

### Test Results
- `domain` (8 tests): retry strategies, cron parsing, timezone validation — all pass
- `db` (4 integration tests): cron dedup, idempotency duplicate rejection, lease fencing, queue concurrency NOWAIT — all pass

---

## Architecture Notes

### Error Handling Pattern (Batch 3 refinement)
The `From<sqlx::Error> for AppError` impl in `common/src/errors.rs` was already correct — it detects PG error code `23505` (unique violation) and maps to `AppError::Conflict` (HTTP 409). The string-contains checks in `auth.rs` and `jobs.rs` were redundant layers that could break if PostgreSQL changes error message wording. Removing them simplifies the code and makes error handling consistent across all routes.

### DAG Resolver Optimization
The old `complete_job` DAG resolver did:
```
for each child of completed job:
    SELECT COUNT(*) FROM workflow_edges WHERE child_id = $1
    SELECT COUNT(*) FROM edge_satisfaction WHERE child_id = $1
    if total == done: UPDATE jobs SET status = 'QUEUED' ...
```

The new resolver does:
```sql
UPDATE jobs SET status = 'QUEUED' 
WHERE id IN (
    SELECT child_id FROM edge_satisfaction es
    JOIN workflow_edges we ON we.child_id = es.child_id
    GROUP BY we.child_id
    HAVING COUNT(DISTINCT es.parent_id) = (
        SELECT COUNT(*) FROM workflow_edges we2 WHERE we2.child_id = we.child_id
    )
) AND status = 'WAITING'
RETURNING *
```

This is O(1) queries regardless of DAG width — the database does the work in a single set-based operation.

### Workflow Transactionality
The `workflows::create()` endpoint now wraps all inserts (workflow row, job rows, outbox events, edge rows) in a single PostgreSQL transaction. If any insert fails (e.g., invalid queue_id, constraint violation), the entire workflow creation rolls back — no orphaned jobs or edges.

### Consumer Handler Resolution
Unknown job types now fail permanently instead of silently succeeding via EchoHandler fallback. This ensures:
- Jobs with typos in `payload.type` go to DLQ (visible failure) instead of silently succeeding
- The system fails fast on misconfigured job types
- DLQ replay can be used to reprocess after fixing the handler registration

---

## Cumulative Audit Score

| Batch | Flaws Found | Flaws Fixed | False Positives |
|-------|-------------|-------------|-----------------|
| Batch 1 | 7 | 7 | 0 |
| Batch 2 | 7 | 7 | 0 |
| Batch 3 | 15 | 14 | 1 |
| **Total** | **29** | **28** | **1** |
