import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// MERGE: proxy /api/* to FastAPI at localhost:8000 in dev.
// FastAPI CORS is already open (allow_origins=["*"]) — proxy is a dev convenience.
// In production set VITE_API_URL to the full API base URL instead.
export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      '/api': {
        target: 'http://localhost:8000',
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/api/, ''),
      },
    },
  },
})
