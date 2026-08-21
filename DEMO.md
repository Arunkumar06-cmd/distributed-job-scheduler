# Demo Guide — 7 Scenarios for Grader (COPY-PASTE, no typing)

<video src="../demo.mp4" controls width="100%"></video>

> `demo.mp4` 2:45 — same 7 scenarios below. Upload to YouTube unlisted and replace `../demo.mp4` with `https://youtu.be/...`.

## 0. Start — COPY-PASTE IN ORDER, keep 3 terminals open

**Terminal 1 — NATS + API (keep open):**
```bash
cd /Users/arunkumar/distributed-job-scheduler
nats-server -js -sd /tmp/nats-js -p 4222 -m 8222 &
sleep 2 && ps aux | grep nats-server | grep -v grep
DATABASE_URL=postgres:///job_scheduler cargo run -p api
# wait until you see: listening addr: 0.0.0.0:8080
# DO NOT close this terminal. Open new tab for test: Cmd+T
```

**Terminal 2 — Worker (new window, keep open):**
```bash
cd /Users/arunkumar/distributed-job-scheduler
DATABASE_URL=postgres:///job_scheduler cargo run -p worker
# wait until: found existing stream + worker registered + consumer started
```

**Terminal 3 — Frontend (new window, keep open):**
```bash
cd /Users/arunkumar/distributed-job-scheduler/frontend
npm install
npm run dev
# wait: VITE v5.4.21 ready in 420 ms  Local: http://localhost:3000/
```

**Check all up — new tab Cmd+T, COPY-PASTE:**
```bash
curl -s http://localhost:8080/health | python3 -m json.tool
curl -s http://localhost:8080/metrics | python3 -m json.tool | head -20
ps aux | grep -E "api|worker|nats" | grep -v grep | head -5
```

## 1. Create tenant — COPY-PASTE ONE BLOCK (no manual IDs)

```bash
cd /Users/arunkumar/distributed-job-scheduler
TOKEN=$(curl -s -X POST http://localhost:8080/auth/register -H 'Content-Type: application/json' -d '{"email":"demo_'$(date +%s)'@test.com","password":"password123","display_name":"Demo"}' | python3 -c "import sys,json; print(json.load(sys.stdin)['token'])")
echo "TOKEN $TOKEN" | cut -c1-30
ORG=$(curl -s -X POST http://localhost:8080/organizations -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' -d '{"name":"Demo Org","slug":"demo-'$(date +%s)'"}' | python3 -c "import sys,json; print(json.load(sys.stdin)['id'])")
echo "ORG $ORG"
PROJ=$(curl -s -X POST http://localhost:8080/projects -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' -d '{"org_id":"'"$ORG"'","name":"Demo Proj","slug":"demo-proj-'$(date +%s)'"}' | python3 -c "import sys,json; print(json.load(sys.stdin)['id'])")
echo "PROJ $PROJ"
Q=$(curl -s -X POST http://localhost:8080/queues -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' -d '{"project_id":"'"$PROJ"'","name":"demo-queue","max_concurrency":3}' | python3 -c "import sys,json; print(json.load(sys.stdin)['id'])")
echo "Q $Q"
# IMPORTANT: wait 12s for worker SKIP LOCKED discovery (poll 10s)
sleep 12
echo "Ready — queue $Q discovered"
```

**Website clicks (no typing):**
- Open `http://localhost:3000` → `Register` → `email: demo@test.com` `password: password123` `display_name: Demo` → `Register`
- Left `+ Org` → `Demo Org` `demo-org` → `Create` → select `Demo Org` dropdown
- `+ Project` → `Demo Proj` `demo-proj` → select `Demo Proj`
- `+ Queue` → `demo-queue` `3` → select `demo-queue` → left card shows `Q:0 R:0 C:0`

## 2. Demo 1 — Create Job (COPY-PASTE)

```bash
J=$(curl -s -X POST http://localhost:8080/jobs -H "Authorization: Bearer $TOKEN" -H "Idempotency-Key: demo1" -H 'Content-Type: application/json' -d '{"queue_id":"'"$Q"'","payload":{"type":"echo","data":"hello"}}' | python3 -c "import sys,json; print(json.load(sys.stdin)['id'])")
echo "Job $J Queued"
sleep 3
curl -s http://localhost:8080/jobs/$J -H "Authorization: Bearer $TOKEN" | python3 -m json.tool | grep -E "status|result"
# Expected: "status": "Completed", "result": {"echoed": true}
```

## 3. Demo 2 — Pause / Resume (COPY-PASTE)

```bash
curl -s -X POST http://localhost:8080/queues/$Q/pause -H "Authorization: Bearer $TOKEN" | python3 -c "import sys,json; print('paused', json.load(sys.stdin)['is_paused'])"
# true
J2=$(curl -s -X POST http://localhost:8080/jobs -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' -d '{"queue_id":"'"$Q"'","payload":{"type":"echo"}}' | python3 -c "import sys,json; print(json.load(sys.stdin)['id'])")
sleep 2
curl -s http://localhost:8080/jobs/$J2 -H "Authorization: Bearer $TOKEN" | python3 -c "import sys,json; print('paused job', json.load(sys.stdin)['status'])"
# Queued (stays)
curl -s -X POST http://localhost:8080/queues/$Q/resume -H "Authorization: Bearer $TOKEN" > /dev/null; echo "resumed"
sleep 3
curl -s http://localhost:8080/jobs/$J2 -H "Authorization: Bearer $TOKEN" | python3 -c "import sys,json; print('resumed', json.load(sys.stdin)['status'])"
# Completed
```

**Website:** Left `demo-queue` `⏸️ Pause Queue` → click → `⚠️ Confirm Pause?` → click again → `PAUSED` → `Jobs` row stays `QUEUED` → `▶ Resume Queue` → row `Completed`.

## 4. Demo 3 — Concurrency 3, Flood 20 (COPY-PASTE)

```bash
for i in {1..20}; do curl -s -X POST http://localhost:8080/jobs -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' -d '{"queue_id":"'"$Q"'","payload":{"type":"sleep","secs":2}}' & done; wait; echo flooded
for i in 1 2 3 4 5 6; do S=$(curl -s http://localhost:8080/queues/$Q/stats -H "Authorization: Bearer $TOKEN" | python3 -c "import sys,json; print(json.load(sys.stdin)['running'])"); echo "running $S"; if [ "$S" -gt 3 ]; then echo "FAIL"; exit 1; fi; sleep 1; done; echo "concurrency OK never >3"
```

Website left bar `[■■■□□□□□] 3/3` solid = leases.

## 5. Demo 4 — Kill Worker Epoch Fencing (COPY-PASTE)

```bash
ps aux | grep "target/debug/worker" | grep -v grep | awk '{print $2}' | head -1
# copy PID, then:
kill -9 <paste-PID>
# In new terminal, restart worker:
# cd /Users/arunkumar/distributed-job-scheduler
# DATABASE_URL=postgres:///job_scheduler cargo run -p worker
# Then:
psql -d job_scheduler -c "SELECT left(id::text,8), lease_epoch, status FROM jobs WHERE queue_id='$Q' ORDER BY created_at DESC LIMIT 3;"
# lease_epoch 1→2
psql -d job_scheduler -c "SELECT status,attempt FROM job_executions WHERE job_id='<paste-sleep10-id>' ORDER BY attempt"
# ABANDONED + COMPLETED
```

## 6. Demo 5 — Idempotency (COPY-PASTE)

```bash
curl -s -X POST http://localhost:8080/jobs -H "Authorization: Bearer $TOKEN" -H "Idempotency-Key: same" -H 'Content-Type: application/json' -d '{"queue_id":"'"$Q'","payload":{"type":"echo"}}' | python3 -m json.tool | grep -E "status|id"
# Queued
curl -s -X POST http://localhost:8080/jobs -H "Authorization: Bearer $TOKEN" -H "Idempotency-Key: same" -H 'Content-Type: application/json' -d '{"queue_id":"'"$Q'","payload":{"type":"echo"}}' | python3 -m json.tool | grep -E "error|code"
# Conflict 409
psql -d job_scheduler -c "SELECT count(*) FROM jobs WHERE idempotency_key='same'"
# 1
```

## 7. Demo 6 — Retry → DLQ + AI (COPY-PASTE)

```bash
JD=$(curl -s -X POST http://localhost:8080/jobs -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' -d '{"queue_id":"'"$Q"'","payload":{"type":"always_fail"},"max_attempts":3}' | python3 -c "import sys,json; print(json.load(sys.stdin)['id'])")
echo $JD
for i in 1 2 3 4 5 6; do sleep 2; curl -s http://localhost:8080/jobs/$JD -H "Authorization: Bearer $TOKEN" | python3 -c "import sys,json; print(json.load(sys.stdin).get('status'))"; done
# RETRY_WAIT → FAILED
curl -s "http://localhost:8080/dlq?queue_id=$Q" -H "Authorization: Bearer $TOKEN" | python3 -m json.tool | head -20
DLQ=$(curl -s "http://localhost:8080/dlq?queue_id=$Q" -H "Authorization: Bearer $TOKEN" | python3 -c "import sys,json; print(json.load(sys.stdin)['data'][0]['id'])")
curl -s -X POST http://localhost:8080/dlq/$DLQ/replay -H "Authorization: Bearer $TOKEN" | python3 -m json.tool | head -10
sleep 11; psql -d job_scheduler -c "SELECT left(summary,60) FROM failure_summaries ORDER BY created_at DESC LIMIT 1"
# Downstream service repeatedly...
```

Website `DLQ` tab → `Replay` button.

## 8. Demo 7 — Workflow A,B→C (COPY-PASTE)

```bash
WF=$(curl -s -X POST http://localhost:8080/workflows -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' -d '{"project_id":"'"$PROJ"'","name":"wf-demo","jobs":[{"queue_id":"'"$Q"'","payload":{"type":"echo","dag":"A"}},{"queue_id":"'"$Q"'","payload":{"type":"echo","dag":"B"}},{"queue_id":"'"$Q"'","payload":{"type":"echo","dag":"C"}}],"edges":[{"parent":0,"child":2},{"parent":1,"child":2}]}' | python3 -c "import sys,json; print(json.load(sys.stdin)['workflow_id'])")
echo $WF
sleep 6
curl -s http://localhost:8080/workflows/$WF -H "Authorization: Bearer $TOKEN" | python3 -m json.tool | grep -E "dag|status" | head -10
# A Completed, B Completed, C Completed
```

Website `Jobs` filter `WAITING` → `C` flips.

## 9. Checks (COPY-PASTE)

```bash
curl -s http://localhost:8080/metrics | python3 -m json.tool | head -20
curl -s http://localhost:8080/workers -H "Authorization: Bearer $TOKEN" | python3 -m json.tool | head -20
psql -d job_scheduler -c "SELECT status, count(*) FROM jobs GROUP BY status;"
nats stream ls
```

## 10. Stop (COPY-PASTE)

```bash
ps aux | grep "target/debug/api\|target/debug/worker" | grep -v grep | awk '{print $2}' | xargs kill
lsof -i :8080 | head
# Cmd+Shift+5 → Stop Recording (square) or Cmd+Ctrl+Esc
```

**All above are pure copy-paste — no typing, no manual ID paste (uses $Q, $TOKEN, $J vars). For recording, run `bash /tmp/demo_final.sh` (does all 7).**
