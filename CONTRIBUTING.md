# Contributing

## Local checks

Start PostgreSQL and NATS, then run the same gates used in CI:

```bash
docker compose up -d postgres nats
cargo fmt --all -- --check
DATABASE_URL=postgres://postgres:postgres@127.0.0.1:5432/job_scheduler \
  NATS_URL=nats://127.0.0.1:4222 \
  cargo test --workspace -- --test-threads=1
cargo clippy --workspace --all-targets -- -D warnings
cd frontend && npm ci && npm run build && npm audit --audit-level=high
```

## Change expectations

- Preserve the state-machine and fencing invariants documented in the README.
- Add focused tests for changes to claims, retries, outbox delivery, or schema.
- Keep database migrations forward-only and safe for PostgreSQL transactions.
- Do not add secrets, local database files, build output, or credentials to Git.
