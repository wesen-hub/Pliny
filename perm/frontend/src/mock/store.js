import { reactive } from 'vue'
import { policies as originPolicies } from './policies'

// 共享 mock 状态：列表页与编辑页读写同一份（内存级，刷新后恢复初始数据）
export const store = reactive({
  policies: JSON.parse(JSON.stringify(originPolicies))
})

export const resetStore = () => {
  store.policies = JSON.parse(JSON.stringify(originPolicies))
}

// 空白策略模板（新建模式）
export const blankPolicy = () => ({
  policyId: 'pol_draft_' + Date.now().toString(36).slice(-6),
  policyName: '未命名策略',
  version: 1,
  status: 'draft',
  effect: 'GRANT',
  priority: 100,
  subject: { type: 'role', ids: [], attributes: [] },
  resource: {
    resourceType: 'TABLE',
    resourceId: '',
    columns: [],
    excludeColumns: [],
    visibility: 'CONDITIONAL'
  },
  actions: [],
  constraints: { rowExpr: '', maskRules: [], env: [] },
  obligations: [],
  effectivePeriod: { from: new Date().toISOString().slice(0, 10) + 'T00:00:00+08:00', to: null },
  meta: { owner: '', description: '', updatedAt: new Date().toLocaleString('zh-CN', { hour12: false }).replace(/\//g, '-') }
})

// 保存：存在同 policyId 则更新，否则追加
export const savePolicy = (p) => {
  const idx = store.policies.findIndex(x => x.policyId === p.policyId)
  if (idx >= 0) store.policies[idx] = JSON.parse(JSON.stringify(p))
  else store.policies.unshift(JSON.parse(JSON.stringify(p)))
}
