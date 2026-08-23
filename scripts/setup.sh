#!/bin/bash
set -e
echo "Setting up Distributed Job Scheduler..."
if ! command -v psql &> /dev/null; then echo "postgres not found, install via brew"; exit 1; fi
if ! command -v nats-server &> /dev/null; then brew install nats-server; fi
createdb job_scheduler 2>/dev/null || echo "DB exists"
createdb job_scheduler_test 2>/dev/null || echo "test DB exists"
# Schema is applied by the api on startup via sqlx::migrate (tracked in _sqlx_migrations).
echo "Starting NATS..."
nats-server -js -sd /tmp/nats-js -p 4222 -m 8222 > /tmp/nats.log 2>&1 &
echo "Building..."
cargo build --workspace
echo "Run: DATABASE_URL=postgres:///job_scheduler cargo run -p api"
echo "     DATABASE_URL=postgres:///job_scheduler cargo run -p worker"
echo "Frontend: cd frontend && npm install && npm run dev"
