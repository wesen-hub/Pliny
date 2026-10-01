<script setup>
import { ref } from 'vue'
import Overview from './views/Overview.vue'
import Namespaces from './views/Namespaces.vue'
import Tasks from './views/Tasks.vue'
import Scaling from './views/Scaling.vue'
import Monitor from './views/Monitor.vue'
import Ops from './views/Ops.vue'

const menus = [
  { key: 'overview', label: '总览', icon: '◈' },
  { key: 'namespaces', label: 'Namespace', icon: '▤' },
  { key: 'tasks', label: '任务与分区', icon: '▦' },
  { key: 'scaling', label: '扩缩容', icon: '⇅' },
  { key: 'monitor', label: '监控', icon: '◉' },
  { key: 'ops', label: '运维操作', icon: '⚙' },
]
const current = ref('overview')
</script>

<template>
  <div class="app">
    <aside class="sidebar">
      <div class="brand">
        <div class="brand-logo">DCP</div>
        <div class="brand-text">
          <div class="brand-title">统一调度框架</div>
          <div class="brand-sub">etcd · namespace · 通用消费调度</div>
        </div>
      </div>
      <nav class="menu">
        <div
          v-for="m in menus" :key="m.key"
          class="menu-item" :class="{ active: current === m.key }"
          @click="current = m.key"
        >
          <span class="menu-icon">{{ m.icon }}</span>{{ m.label }}
        </div>
      </nav>
      <div class="sidebar-foot">
        <div class="foot-line">设计稿 · mock 数据演示</div>
        <div class="foot-line">docs/统一调度框架设计.md</div>
      </div>
    </aside>
    <main class="main">
      <component :is="{
        overview: Overview, namespaces: Namespaces,
        tasks: Tasks, scaling: Scaling, monitor: Monitor, ops: Ops,
      }[current]" />
    </main>
  </div>
</template>

<style>
* { margin: 0; padding: 0; box-sizing: border-box; }
body { font-family: "PingFang SC", "Microsoft YaHei", sans-serif; background: #f1f5f9; }
.app { display: flex; min-height: 100vh; }
.sidebar {
  width: 220px; background: #0f172a; color: #cbd5e1; flex-shrink: 0;
  display: flex; flex-direction: column; position: sticky; top: 0; height: 100vh;
}
.brand { display: flex; align-items: center; gap: 10px; padding: 18px 16px; border-bottom: 1px solid #1e293b; }
.brand-logo {
  width: 38px; height: 38px; border-radius: 9px; flex-shrink: 0;
  background: linear-gradient(135deg, #2563eb, #06b6d4);
  color: #fff; font-weight: 700; font-size: 13px;
  display: flex; align-items: center; justify-content: center;
}
.brand-title { font-size: 14px; font-weight: 600; color: #f1f5f9; }
.brand-sub { font-size: 11px; color: #64748b; margin-top: 2px; }
.menu { flex: 1; padding: 12px 8px; }
.menu-item {
  display: flex; align-items: center; gap: 10px; padding: 11px 14px;
  border-radius: 8px; cursor: pointer; font-size: 13.5px; margin-bottom: 4px;
  transition: all .15s; user-select: none;
}
.menu-item:hover { background: #1e293b; color: #e2e8f0; }
.menu-item.active { background: #2563eb; color: #fff; font-weight: 600; }
.menu-icon { font-size: 15px; width: 18px; text-align: center; }
.sidebar-foot { padding: 14px 16px; border-top: 1px solid #1e293b; }
.foot-line { font-size: 11px; color: #475569; line-height: 1.7; }
.main { flex: 1; padding: 20px 24px; min-width: 0; }
</style>
