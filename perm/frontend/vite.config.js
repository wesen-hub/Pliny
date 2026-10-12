import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'

// 开发代理：PDP（8081）与管理面（8082）共挂 /api/v1 前缀，按路径细分转发
const pdpTarget = process.env.PDP_TARGET || 'http://localhost:8081'
const adminTarget = process.env.ADMIN_TARGET || 'http://localhost:8082'

export default defineConfig({
  plugins: [vue()],
  server: {
    port: 5173,
    host: true,
    proxy: {
      // PDP 判定与 PDP 管理面 → 8081
      '/api/v1/authorize': { target: pdpTarget, changeOrigin: true },
      '/api/v1/admin': { target: pdpTarget, changeOrigin: true },
      // 其余 /api/v1（策略 CRUD / 状态流转 / 发布历史）→ 管理面 8082
      '/api/v1': { target: adminTarget, changeOrigin: true }
    }
  }
})
