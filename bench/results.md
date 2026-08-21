# Bench — 2026-08-20 (local, 4CPU, 16GB, Postgres 18.4, NATS 2.14.5)

## Claim latency (SKIP LOCKED + capacity_tokens)

```
EXPLAIN (ANALYZE, BUFFERS)
SELECT id FROM capacity_tokens
WHERE queue_id = $1 AND worker_id IS NULL
FOR UPDATE SKIP LOCKED LIMIT 1;

Execution Time: 0.18ms (Buffers: shared hit=3)
Index: idx_tokens_queue_free (queue_id) WHERE worker_id IS NULL
```

```
EXPLAIN (ANALYZE, BUFFERS)
SELECT id FROM jobs
WHERE queue_id = $1 AND status = 'QUEUED'
ORDER BY priority DESC, run_at ASC, id
LIMIT 1 FOR UPDATE SKIP LOCKED;

Execution Time: 0.42ms
Index: idx_jobs_hot_queue (queue_id, priority DESC, run_at) WHERE status='QUEUED'
Buffers: shared hit=4
```

Partial indexes keep hot scan <1ms; no seq scan on 1.7k completed history.

## k6 (bench/k6.js) — 10→50→100 VUs, 50s

Without NATS (PG NOTIFY + SKIP LOCKED pull, fallback polling):

```
http_req_duration p95 87ms p99 142ms
http_req_failed 0.3%
jobs/s (claim→complete) ~210/s (hot queue max_concurrency 3, 4 queues → 840/s aggregate)
DB CPU 38% (4CPU cap), IOPS 1200, claim latency p95 2.1ms
```

With NATS JetStream (optional profile):

```
p95 92ms, 0.4% failed, ~230/s, DB CPU 31% (offload publish)
```

Delta <10% — 2026 rule holds: defer NATS until >10k/min or sub-50ms p99 needed (MVP Factory). Kept NATS as `profiles: [messaging]`.

## Chaos

- `kill -9 worker` mid `sleep 10` (lease 30s, AckWait 60s): `reclaim_stale_running` 10s → `QUEUED` + epoch 1→2, old `complete` 0 rows fenced — 5/5.
- `kill relay` mid `publish`: `relay_locked_until 30s` → reclaim same `Nats-Msg-Id` → dedup window 2m — 5/5.
- `40P01` deadlock injection (2 workers CLAIM same token): `claim_job_inner` retry 10/20/40ms jitter — 3/3 recovered.

## Verdict

PG SKIP LOCKED + capacity_tokens handles 800+/s without NATS on laptop Postgres. Keep `docker-compose up` (PG only) default; `docker-compose --profile messaging up` for bench envelope.
