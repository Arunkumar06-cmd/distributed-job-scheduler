FROM node:20-alpine AS frontend
WORKDIR /app/frontend
COPY frontend/package.json frontend/package-lock.json* ./
RUN npm install
COPY frontend/ ./
RUN npm run build

FROM rust:1.96 AS builder
WORKDIR /app
COPY Cargo.toml Cargo.lock* ./
COPY common ./common
COPY domain ./domain
COPY db ./db
COPY outbox ./outbox
COPY worker ./worker
COPY scheduler ./scheduler
COPY api ./api
RUN cargo build --release -p api

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates libssl3 && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/api /usr/local/bin/api
COPY --from=frontend /app/frontend/dist /app/frontend/dist
COPY db/migrations /app/db/migrations
WORKDIR /app
EXPOSE 8080
CMD ["api"]
