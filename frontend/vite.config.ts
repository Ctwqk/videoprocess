import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

const apiProxyTarget = process.env.VP_API_PROXY_TARGET ?? 'http://localhost:8080'
const youtubeProxyTarget = process.env.VP_YOUTUBE_PROXY_TARGET ?? 'http://localhost:3001'
const xiaohongshuProxyTarget = process.env.VP_XIAOHONGSHU_PROXY_TARGET ?? 'http://localhost:8897'
const platformProxyTarget = process.env.VP_PLATFORM_PROXY_TARGET ?? 'http://localhost:8898'

// https://vite.dev/config/
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      '/api': apiProxyTarget,
      '/ws': {
        target: apiProxyTarget,
        ws: true,
      },
      '/platforms/api/platforms/xiaohongshu': {
        target: xiaohongshuProxyTarget,
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/platforms/, ''),
      },
      '/youtube': {
        target: youtubeProxyTarget,
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/youtube/, ''),
      },
      '/platforms': {
        target: platformProxyTarget,
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/platforms/, ''),
      },
    },
  },
})
