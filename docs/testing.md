# Testing Strategy

The suite is layered so that cheap tests run on every save and the most
expensive proofs run in dedicated CI jobs. Every layer is automated; nothing
in this document is a manual step.

## Layers

| Layer | Location | Count | What it proves | Runtime |
|---|---|---|---|---|
| Domain unit | `domain/src` | 28 | Retry math (overflow-safe), lifecycle state-machine matrix (terminal states are absorbing), cron semantics incl. timezone + catch-up, slug/tier/shard helpers | < 1 s |
| Shared unit | `common/src`, `api/src/auth.rs`, `worker/src/handler.rs` | 26 | Token kind-separation (refresh ≠ access), tamper/expiry rejection, argon2 round-trip, rate-limiter windows, panic/timeout handler guards, error redaction (internal causes never reach clients) | < 2 s |
| DB integration | `db/src/integration_tests.rs` | 12 | **Real Postgres.** Atomic claim fencing, idempotency conflicts, capacity-token limits under contention, parallel-claim single-winner, cron occurrence dedup, DLQ replay inheritance + double-replay rejection, retry-policy resolution layering, UNKNOWN reconciliation policies, heartbeat pruning, audit writes, subject-tier correctness | ~ 5–9 s |
| Worker race harness | `worker/tests/race_harness.rs` | 1 | **3 consumer replicas race 40 jobs through one shared durable.** Asserts zero double-execution (one ledger row per job), distribution across replicas, and no leaked capacity tokens | ~ 32 s |
| Archive throughput bench | `db/tests/archive_bench.rs` | 1 | Seeds 5,000 terminal jobs, drains via `archive_terminal_jobs` in 500-row batches. Measured: **~4,400–4,500 rows/s** on laptop Postgres | ~ 2 s |
| Worker pipeline e2e | `worker/tests/pipeline_e2e.rs` | 1 | **Full stack:** outbox row → JetStream publish → durable pull consumer → claim → handler → COMPLETED with result + execution ledger; failure path lands in DLQ; heartbeat counters move | ~ 5 s |
| Frontend unit/component | `frontend/src/**/*.test.*` | 6 | Refresh-token rotation semantics (retry-once with new access token, session-expired dispatch), format helpers, ErrorBoundary fallback | ~ 4 s |
| Browser e2e | `frontend/tests/e2e/dashboard.spec.js` | 8 | Real browser against real API: register → onboarding wizard → create org/project/queue via modals → submit job (listed + pagination) → payload validation → pause/resume badge → DLQ empty state → sign-out. Includes **axe accessibility scans** asserting zero critical/serious violations (contrast included) | ~ 25 s |
| Visual regression | `frontend/tests/e2e/visual.spec.js` | 4 | Pixel baselines for auth / welcome / workspace / DLQ surfaces. Deterministic via fixed viewport, reduced-motion gating of animations, masked dynamic regions (refresh timestamp), fresh empty workspace per shot | ~ 10 s |
| Load benchmark | `bench/k6.js` | — | Sustained job-submission throughput profile (`k6 run bench/k6.js`) | manual |

CI totals: **66 Rust lib tests + 3 service-backed proofs (pipeline, race harness, archive bench) + 6 vitest + 12 Playwright** — every job in CI gates merges.

## Running locally

```bash
# fast loop
cargo test -p common -p domain
npm --prefix frontend run test

# database integration (per-test throwaway databases; needs Postgres)
DATABASE_URL=postgres://postgres@127.0.0.1:5433/job_scheduler_test cargo test -p db

# full pipeline e2e (needs Postgres + JetStream-enabled NATS)
DATABASE_URL=postgres:///job_scheduler_test NATS_URL=nats://127.0.0.1:4222 \
  cargo test -p worker --test pipeline_e2e -- --include-ignored

# browser e2e (boots the api itself; override E2E_* to target existing services)
cd frontend && npx playwright install chromium && npx playwright test

# coverage
cargo llvm-cov --workspace --lib --summary-only     # Rust
npm --prefix frontend run test:coverage             # vitest v8 coverage
```

## Design decisions worth knowing

* **Per-test databases.** Each integration test creates a throwaway
  `js_test_<uuid>` database and applies all migrations *twice*. That both
  isolates parallel tests completely and continuously proves every migration
  is idempotent — a drifted schema converges without manual surgery.
* **Migrations are applied by the code under test**, not by fixtures: the
  pipeline e2e runs `sqlx::migrate!` exactly like the API does at startup.
* **No mocks below the unit layer.** The db integration tests and pipeline e2e
  speak to real Postgres and real JetStream; the only stubs anywhere are
  fetch-level doubles in the refresh-wrapper unit test.
* **Determinism knobs for screenshots** live in config/CSS (fixed viewport,
  `prefers-reduced-motion` gated pulse animation, masked timestamp regions),
  not in ad-hoc sleeps.
* **Flake policy**: a failing test is a bug — either in product code or in the
  test's isolation model. The historical TRUNCATE race was eliminated by the
  per-test-database harness rather than by retries.

## CI topology (`.github/workflows/ci.yml`)

| Job | Services | Runs |
|---|---|---|
| `test` | postgres, nats(-js) | fmt check · build · `cargo test --workspace` (parallel-safe) · clippy `-D warnings` · vitest · dashboard build · npm audit |
| `pipeline-e2e` | postgres, nats(-js) | worker pipeline e2e with `--include-ignored` |
| `frontend-e2e` | postgres, nats(-js) | builds dashboard, installs Chromium, runs Playwright suite |
| `coverage` | postgres | `cargo llvm-cov` summary across workspace libs |

All jobs gate merges; none are allow-failure.
