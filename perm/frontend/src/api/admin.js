// 策略管理面 API（perm-admin 8082）
import { http } from './http'

export const policyApi = {
  list: () => http.get('/policies'),
  get: (id) => http.get(`/policies/${id}`),
  create: (dsl, operator) => http.post('/policies', dsl, { 'X-Operator': operator || 'admin' }),
  update: (id, dsl, operator) => http.put(`/policies/${id}`, dsl, { 'X-Operator': operator || 'admin' }),
  remove: (id) => http.del(`/policies/${id}`),

  /** 状态流转（R-M1 状态机动作）：submit / reject / approve / deprecate / retire */
  transition: (id, action, { operator, force } = {}) => {
    if (action === 'approve') {
      return http.post(`/policies/${id}/approve?force=${force ? 'true' : 'false'}`, undefined, { 'X-Operator': operator || 'admin' })
    }
    return http.post(`/policies/${id}/${action}`)
  },

  /** 冲突检测预览（不落库，R-M7/R-M8） */
  validate: (id) => http.post(`/policies/${id}/validate`),

  publications: () => http.get('/publications')
}

/** 后端 R-M1 生命周期状态元数据（DRAFT → IN_REVIEW → ACTIVE → DEPRECATED → RETIRED） */
export const backendStatusMeta = {
  DRAFT: { label: '草稿', type: 'info' },
  IN_REVIEW: { label: '审批中', type: 'warning' },
  ACTIVE: { label: '已发布', type: 'success' },
  DEPRECATED: { label: '已下线过渡', type: 'danger' },
  RETIRED: { label: '已退役', type: 'info' }
}

/** 各状态可执行的流转动作（label / 目标状态 / 样式） */
export const backendTransitions = {
  DRAFT: [
    { action: 'submit', to: 'IN_REVIEW', label: '提交审批', type: 'primary' },
    { action: 'retire', to: 'RETIRED', label: '直接退役', type: 'danger' }
  ],
  IN_REVIEW: [
    { action: 'approve', to: 'ACTIVE', label: '审批通过并发布', type: 'success' },
    { action: 'reject', to: 'DRAFT', label: '驳回', type: 'danger' }
  ],
  ACTIVE: [
    { action: 'deprecate', to: 'DEPRECATED', label: '下线过渡', type: 'danger' }
  ],
  DEPRECATED: [
    { action: 'retire', to: 'RETIRED', label: '退役', type: 'danger' }
  ],
  RETIRED: []
}
