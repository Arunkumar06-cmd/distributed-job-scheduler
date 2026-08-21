import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      '/api': {
        target: 'http://localhost:8080',
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/api/, ''),
      },
      '/auth': 'http://localhost:8080',
      '/organizations': 'http://localhost:8080',
      '/projects': 'http://localhost:8080',
      '/queues': 'http://localhost:8080',
      '/jobs': 'http://localhost:8080',
      '/workers': 'http://localhost:8080',
      '/dlq': 'http://localhost:8080',
      '/batches': 'http://localhost:8080',
      '/scheduled-jobs': 'http://localhost:8080',
      '/workflows': 'http://localhost:8080',
      '/health': 'http://localhost:8080',
      '/metrics': 'http://localhost:8080',
      '/events': 'http://localhost:8080',
      '/ws': { target: 'ws://localhost:8080', ws: true },
    }
  }
})
