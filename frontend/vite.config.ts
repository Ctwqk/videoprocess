import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// https://vite.dev/config/
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      '/api': 'http://localhost:8080',
      '/ws': {
        target: 'http://localhost:8080',
        ws: true,
      },
      '/platforms/api/platforms/xiaohongshu': {
        target: 'http://localhost:8897',
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/platforms/, ''),
      },
      '/youtube': {
        target: 'http://localhost:3001',
        changeOrigin: true,
      },
      '/platforms': {
        target: 'http://localhost:8898',
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/platforms/, ''),
      },
    },
  },
})
