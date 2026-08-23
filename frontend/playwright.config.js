import { defineConfig } from '@playwright/test'

const PORT = process.env.E2E_PORT || 18084
const BASE = `http://localhost:${PORT}`

export default defineConfig({
  testDir: 'tests/e2e',
  timeout: 60_000,
  retries: process.env.CI ? 1 : 0,
  use: {
    baseURL: BASE,
    headless: true,
    trace: 'retain-on-failure',
    // Deterministic rendering for screenshot baselines.
    viewport: { width: 1440, height: 900 },
    reducedMotion: 'reduce',
  },
  // Serves the built dashboard through the api's static fallback; the same
  // process hosts the REST + SSE surface, so tests exercise the real stack.
  webServer: process.env.E2E_BASE_URL
    ? undefined
    : {
        command: 'cargo run -p api',
        cwd: '..',
        url: `${BASE}/health`,
        reuseExistingServer: true,
        timeout: 180_000,
        env: {
          DATABASE_URL: process.env.E2E_DATABASE_URL || 'postgres://postgres@127.0.0.1:5433/job_scheduler_test',
          JWT_SECRET: process.env.JWT_SECRET || 'e2e-secret-value-0123456789abcdef',
          NATS_URL: process.env.E2E_NATS_URL || 'nats://127.0.0.1:4422',
          RUST_LOG: 'warn',
          API_PORT: String(PORT),
        },
      },
})
