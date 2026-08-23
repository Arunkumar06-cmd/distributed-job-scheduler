import { defineConfig } from 'vitest/config'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  test: {
    environment: 'jsdom',
    globals: true,
    // Unit/component tests live in src/; Playwright owns tests/e2e.
    include: ['src/**/*.{test,spec}.{js,jsx}'],
    coverage: { provider: 'v8', reporter: ['text'], include: ['src/**'], exclude: ['src/main.jsx'] },
  },
})
