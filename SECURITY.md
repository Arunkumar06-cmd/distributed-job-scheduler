# Security Policy

## Supported version

Security fixes are applied to the latest `main` branch.

## Reporting a vulnerability

Please do not open a public issue for a suspected vulnerability. Email the
repository owner with a concise reproduction, affected component, and impact.
You should receive an acknowledgement within five business days.

## Operational baseline

- Set a long, unique `JWT_SECRET` and `CORS_ALLOWED_ORIGIN` in production.
- Terminate TLS at the ingress/load balancer; do not expose PostgreSQL or NATS
  directly to the public internet.
- Rotate credentials and external-service idempotency keys regularly.
- Keep `UNKNOWN_EXTERNAL_RESULT` jobs under a real downstream reconciliation
  process; never auto-complete them based on inference.
