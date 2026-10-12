<template>
  <div class="app-layout">
    <aside class="sidebar">
      <div class="logo">
        <div class="logo-icon">权</div>
        <div>
          <div class="logo-title">统一数据权限框架</div>
          <div class="logo-sub">前端演示工程 v1.1</div>
        </div>
      </div>
      <nav class="nav">
        <router-link
          v-for="item in navs"
          :key="item.path"
          :to="item.path"
          class="nav-item"
          :class="{ active: isActive(item) }"
        >
          <span class="nav-icon">{{ item.icon }}</span>
          <span>{{ item.label }}</span>
        </router-link>
      </nav>
      <div class="sidebar-footer mono">
        设计文档 v1.1 · 规则定义 v1.0<br>perm-admin :8082 · perm-pdp :8081
      </div>
    </aside>
    <main class="main">
      <router-view />
    </main>
  </div>
</template>

<script setup>
import { useRoute } from 'vue-router'

const route = useRoute()
const navs = [
  { path: '/policies', icon: '📋', label: '策略管理' },
  { path: '/playground', icon: '🎯', label: '判定演示台' },
  { path: '/flow', icon: '🔀', label: '全链路可视化' }
]
const isActive = (item) => route.path.startsWith(item.path)
</script>

<style scoped>
.app-layout { display: flex; height: 100%; }
.sidebar {
  width: 220px;
  flex-shrink: 0;
  background: #0f172a;
  color: #cbd5e1;
  display: flex;
  flex-direction: column;
}
.logo {
  display: flex;
  gap: 10px;
  align-items: center;
  padding: 20px 16px 16px;
  border-bottom: 1px solid #1e293b;
}
.logo-icon {
  width: 36px; height: 36px;
  border-radius: 8px;
  background: linear-gradient(135deg, #2563eb, #7c3aed);
  color: #fff;
  display: flex; align-items: center; justify-content: center;
  font-size: 18px; font-weight: 700;
}
.logo-title { color: #f1f5f9; font-size: 14px; font-weight: 600; }
.logo-sub { color: #64748b; font-size: 11px; margin-top: 2px; }
.nav { padding: 12px 8px; flex: 1; }
.nav-item {
  display: flex; align-items: center; gap: 10px;
  padding: 10px 12px;
  border-radius: 8px;
  color: #94a3b8;
  text-decoration: none;
  font-size: 14px;
  margin-bottom: 4px;
  transition: all .15s;
}
.nav-item:hover { background: #1e293b; color: #e2e8f0; }
.nav-item.active { background: #1d4ed8; color: #fff; }
.nav-icon { font-size: 15px; }
.sidebar-footer {
  padding: 12px 16px;
  border-top: 1px solid #1e293b;
  color: #475569;
  font-size: 11px;
  line-height: 1.6;
}
.main { flex: 1; overflow-y: auto; }
</style>
