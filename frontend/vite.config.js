import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  server: {
    host: '0.0.0.0',
    port: 5173,
    watch: {
  usePolling: true,
  interval: 100,
  binaryInterval: 300,
  awaitWriteFinish: {
    stabilityThreshold: 100,
    pollInterval: 100,
  },
},
    hmr: {
      clientPort: 3000,
    },
    proxy: {
      '/api':   { target: 'http://host.docker.internal:8082', changeOrigin: true },
      '/admin': { target: 'http://host.docker.internal:8082', changeOrigin: true },
      '/agent': { target: 'http://host.docker.internal:8082', changeOrigin: true },
      '/auth':  { target: 'http://host.docker.internal:8082', changeOrigin: true },
    },
  },
})