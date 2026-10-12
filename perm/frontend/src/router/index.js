import { createRouter, createWebHashHistory } from 'vue-router'

const routes = [
  { path: '/', redirect: '/policies' },
  { path: '/policies', name: 'policies', component: () => import('../views/PolicyList.vue'), meta: { title: '策略管理' } },
  { path: '/policies/new', name: 'editor-new', component: () => import('../views/PolicyEditor.vue'), meta: { title: '新建策略' } },
  { path: '/policies/:id/edit', name: 'editor', component: () => import('../views/PolicyEditor.vue'), meta: { title: '策略编辑器' } },
  { path: '/playground', name: 'playground', component: () => import('../views/Playground.vue'), meta: { title: '判定演示台' } },
  { path: '/flow', name: 'flow', component: () => import('../views/FlowView.vue'), meta: { title: '全链路可视化' } }
]

export default createRouter({
  history: createWebHashHistory(),
  routes
})
