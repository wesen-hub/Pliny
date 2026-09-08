import { createRouter, createWebHistory } from 'vue-router'

const routes = [
  { path: '/login', component: () => import('../views/Login.vue') },
  {
    path: '/',
    component: () => import('../layout/Layout.vue'),
    redirect: '/dashboard',
    children: [
      { path: 'dashboard', name: 'dashboard', component: () => import('../views/Dashboard.vue'), meta: { title: '工作台' } },
      { path: 'datasource', name: 'datasource', component: () => import('../views/datasource/List.vue'), meta: { title: '数据源管理' } },
      { path: 'job', name: 'job', component: () => import('../views/job/List.vue'), meta: { title: '任务管理' } },
      { path: 'job/create', name: 'job-create', component: () => import('../views/job/Wizard.vue'), meta: { title: '新建任务', active: 'job' } },
      { path: 'job/:id/edit', name: 'job-edit', component: () => import('../views/job/Wizard.vue'), meta: { title: '编辑任务', active: 'job' } },
      { path: 'dict', name: 'dict', component: () => import('../views/dict/List.vue'), meta: { title: '字典管理' } },
      { path: 'monitor/execution', name: 'execution', component: () => import('../views/monitor/Execution.vue'), meta: { title: '执行历史' } },
      { path: 'monitor/stats', name: 'stats', component: () => import('../views/monitor/Stats.vue'), meta: { title: '统计报表' } },
      // ---------- 数据同步(建模驱动 + 链式缓冲) ----------
      { path: 'sync/overview', name: 'sync-overview', component: () => import('../views/sync/Overview.vue'), meta: { title: '同步总览' } },
      { path: 'model', name: 'model', component: () => import('../views/model/List.vue'), meta: { title: '模型管理' } },
      { path: 'model/:id', name: 'model-detail', component: () => import('../views/model/Detail.vue'), meta: { title: '模型详情', active: 'model' } },
      { path: 'sync/task', name: 'sync-task', component: () => import('../views/sync/TaskList.vue'), meta: { title: '同步任务' } },
      { path: 'sync/task/create', name: 'sync-task-create', component: () => import('../views/sync/TaskWizard.vue'), meta: { title: '新建同步任务', active: 'sync-task' } },
      { path: 'sync/task/:id', name: 'sync-task-detail', component: () => import('../views/sync/TaskDetail.vue'), meta: { title: '任务详情', active: 'sync-task' } },
      { path: 'sync/dlt', name: 'sync-dlt', component: () => import('../views/sync/DltList.vue'), meta: { title: '死信监控' } },
      // ---------- 文件同步(mc mirror 原生) ----------
      { path: 'agent', name: 'agent-list', component: () => import('../views/agent/List.vue'), meta: { title: '采集 Agent' } },
      { path: 'filesync/task', name: 'filesync-task', component: () => import('../views/filesync/TaskList.vue'), meta: { title: '文件同步' } },
      { path: 'filesync/task/create', name: 'filesync-task-create', component: () => import('../views/filesync/TaskWizard.vue'), meta: { title: '新建文件任务', active: 'filesync-task' } },
      { path: 'filesync/task/:id/edit', name: 'filesync-task-edit', component: () => import('../views/filesync/TaskWizard.vue'), meta: { title: '编辑文件任务', active: 'filesync-task' } },
      { path: 'filesync/task/:id', name: 'filesync-task-detail', component: () => import('../views/filesync/TaskDetail.vue'), meta: { title: '文件任务详情', active: 'filesync-task' } },
      // ---------- Kafka 接入(应用/凭证模型: 领 key 即任务, op 消息内指定) ----------
      { path: 'ingest/app', name: 'ingest-app', component: () => import('../views/ingest/AppList.vue'), meta: { title: '事件接入' } },
      { path: 'ingest/app/:appKey', name: 'ingest-app-detail', component: () => import('../views/ingest/AppDetail.vue'), meta: { title: '接入应用监控', active: 'ingest-app' } },
      // ---------- 数据资产(目录/血缘/详情) ----------
      { path: 'asset/catalog', name: 'asset-catalog', component: () => import('../views/asset/Catalog.vue'), meta: { title: '资产目录', active: 'asset-catalog' } },
      { path: 'asset/detail/:name', name: 'asset-detail2', component: () => import('../views/asset/AssetDetail.vue'), meta: { title: '资产详情', active: 'asset-catalog' } },
      { path: 'asset/lineage', name: 'asset-lineage', component: () => import('../views/asset/Lineage.vue'), meta: { title: '数据血缘', active: 'asset-lineage' } },
      { path: 'asset/:id', name: 'asset-detail', component: () => import('../views/asset/Detail.vue'), meta: { title: '资产详情', active: 'asset-catalog' } },
      // ---------- 标签管理 ----------
      { path: 'tag', name: 'tag', component: () => import('../views/tag/List.vue'), meta: { title: '标签管理' } },
      // ---------- 打标引擎(页面设计稿, 数据 mock, 未接后端) ----------
      { path: 'tagengine/task', name: 'tagengine-task', component: () => import('../views/tagengine/TaskList.vue'), meta: { title: '打标引擎 V1', active: 'tagengine' } },
      // ---------- 打标引擎 V2(注册表+规则库+任务, 页面设计稿 mock; 规则库与任务分离, 单元/流两级资产) ----------
      { path: 'tagengine2/tag', name: 'tagengine2-tag', component: () => import('../views/tagengine2/Registry.vue'), meta: { title: '标签注册表(V2)', active: '/tagengine2/tag' } },
      { path: 'tagengine2/rule', name: 'tagengine2-rule-lib', component: () => import('../views/tagengine2/RuleLibrary.vue'), meta: { title: '规则库(V2)', active: '/tagengine2/rule' } },
      { path: 'tagengine2/rule/edit', name: 'tagengine2-rule-edit', component: () => import('../views/tagengine2/RuleEdit.vue'), meta: { title: '规则编辑(V2)', active: '/tagengine2/rule' } },
      { path: 'tagengine2/rule/flow', name: 'tagengine2-rule-flow', component: () => import('../views/tagengine2/RuleFlowEdit.vue'), meta: { title: '规则流编辑(V2)', active: '/tagengine2/rule' } },
      { path: 'tagengine2/task', name: 'tagengine2-task-list', component: () => import('../views/tagengine2/TaskList.vue'), meta: { title: '打标任务(V2)', active: '/tagengine2/task' } },
      { path: 'tagengine2/task/create', name: 'tagengine2-create', component: () => import('../views/tagengine2/TaskWizard.vue'), meta: { title: '新建打标任务(V2)', active: '/tagengine2/task' } },
      { path: 'tagengine2/subs', name: 'tagengine2-subs', component: () => import('../views/tagengine2/Subscriptions.vue'), meta: { title: '标签订阅(V2)', active: '/tagengine2/subs' } },
      { path: 'tagengine2/task/detail', name: 'tagengine2-task-detail', component: () => import('../views/tagengine2/TaskDetail.vue'), meta: { title: '任务详情(V2)', active: '/tagengine2/task' } },
      { path: 'tagengine/rule', name: 'tagengine-rule', component: () => import('../views/tagengine/RuleList.vue'), meta: { title: '规则打标', active: 'tagengine-rule' } },
      { path: 'tagengine/rule/create', name: 'tagengine-rule-create', component: () => import('../views/tagengine/RuleWizard.vue'), meta: { title: '新建规则打标', active: 'tagengine-rule' } },
      { path: 'tagengine/task/create', name: 'tagengine-create', component: () => import('../views/tagengine/TaskWizard.vue'), meta: { title: '新建打标任务', active: 'tagengine' } },
      { path: 'tagengine/task/:id', name: 'tagengine-detail', component: () => import('../views/tagengine/TaskDetail.vue'), meta: { title: '打标任务详情', active: 'tagengine' } },
      // ---------- 订阅与分发 ----------
      { path: 'subscribe', name: 'subscribe', component: () => import('../views/subscribe/List.vue'), meta: { title: '订阅中心' } },
      { path: 'subscribe/create', name: 'subscribe-create', component: () => import('../views/subscribe/Wizard.vue'), meta: { title: '新建订阅', active: 'subscribe' } },
      { path: 'subscribe/:id', name: 'subscribe-detail', component: () => import('../views/subscribe/Detail.vue'), meta: { title: '订阅详情', active: 'subscribe' } },
      // ---------- 权限管理 ----------
      { path: 'system/role-perm', name: 'role-perm', component: () => import('../views/system/RolePerm.vue'), meta: { title: '角色权限', admin: true } },
      { path: 'system/permission', name: 'permission', component: () => import('../views/system/Permission.vue'), meta: { title: '数据权限', admin: true } },
      { path: 'datasub', name: 'datasub', component: () => import('../views/datasub/DatasubCenter.vue'), meta: { title: '数据订阅中心', active: '/datasub' } },
      { path: 'system/user', name: 'user', component: () => import('../views/system/User.vue'), meta: { title: '用户管理', admin: true } },
      { path: 'system/audit', name: 'audit', component: () => import('../views/system/Audit.vue'), meta: { title: '审计日志', admin: true } }
    ]
  }
]

const router = createRouter({
  history: createWebHistory(),
  routes
})

router.beforeEach((to, from, next) => {
  const token = localStorage.getItem('dcp_token')
  if (to.path !== '/login' && !token) {
    next('/login')
    return
  }
  if (to.path === '/login' && token) {
    next('/')
    return
  }
  next()
})

export default router
