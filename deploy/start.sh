#!/bin/bash
set -e

# ── Render.com provides PORT (default 10000); we bind the API to it ──
export API_PORT="${PORT:-8080}"
export API_HOST="0.0.0.0"

# ── NATS runs inside this container ──
export NATS_URL="nats://127.0.0.1:4222"

# ── Defaults for optional vars ──
export AI_SUMMARIES_ENABLED="${AI_SUMMARIES_ENABLED:-false}"
export RUST_LOG="${RUST_LOG:-info}"

echo "=== Jobflow starting on port $API_PORT ==="
echo "DATABASE_URL is set: $([ -n "$DATABASE_URL" ] && echo yes || echo NO — THIS WILL FAIL)"
echo "AI summaries: $AI_SUMMARIES_ENABLED"

# ── Start NATS JetStream in background ──
nats-server -js -p 4222 &
NATS_PID=$!
sleep 1
echo "NATS started (pid $NATS_PID)"

# ── Run migrations via the api binary's sqlx::migrate! at startup ──
# The api handles migration + serving, so we just need to start it.
# Worker and scheduler run as separate processes.

echo "Starting API on :$API_PORT..."
api &
API_PID=$!

echo "Starting worker..."
worker &
WORKER_PID=$!

# Scheduler is embedded in the API process.

# ── Trap SIGTERM (Render sends this during deploys/scaling) ──
cleanup() {
    echo "Shutting down..."
    kill "$API_PID" "$WORKER_PID" "$NATS_PID" 2>/dev/null
    wait 2>/dev/null
    exit 0
}
trap cleanup SIGTERM SIGINT

# Wait for any child to exit; if one dies unexpectedly, shut everything down.
wait -n "$API_PID" "$WORKER_PID" "$NATS_PID"
EXIT_CODE=$?
echo "A process exited unexpectedly ($EXIT_CODE), shutting down all."
kill "$API_PID" "$WORKER_PID" "$NATS_PID" 2>/dev/null
exit "$EXIT_CODE"
