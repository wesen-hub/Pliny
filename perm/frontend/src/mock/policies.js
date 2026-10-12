// ============ 预置策略清单（mock） ============
// 状态机：draft 草稿 → pending 审批中 → approved 已审批 → published 已发布 → superseded 已失效
export const policies = [
  {
    policyId: 'pol_sales_region_001',
    policyName: '区域经理-本区域订单可见',
    version: 3,
    status: 'published',
    effect: 'GRANT',
    priority: 100,
    subject: { type: 'role', ids: ['regional_manager'], attributes: [{ ref: '${subject.level}', op: 'EQ', value: 'manager' }] },
    resource: {
      resourceType: 'TABLE',
      resourceId: 'sales_db.sales_order',
      columns: ['order_id', 'amount', 'region', 'status', 'customer_phone'],
      excludeColumns: ['customer_id_card'],
      visibility: 'CONDITIONAL'
    },
    actions: ['read.snapshot', 'read.changes'],
    constraints: {
      rowExpr: "region = ${subject.region} AND status IN ('paid','pending')",
      maskRules: [{ column: 'customer_phone', rule: 'PARTIAL', expression: "CONCAT(LEFT(`customer_phone`,3),'****',RIGHT(`customer_phone`,4))" }],
      env: [{ ref: '${env.ipRange}', op: 'EQ', value: 'office' }]
    },
    obligations: [{ type: 'AUDIT', level: 'detail', sync: true }],
    effectivePeriod: { from: '2026-09-01T00:00:00+08:00', to: null },
    meta: { owner: '数据安全组', description: '区域经理查看本区域订单', updatedAt: '2026-09-15 10:24' }
  },
  {
    policyId: 'pol_deny_user_007',
    policyName: '封禁用户 user_007',
    version: 5,
    status: 'published',
    effect: 'DENY',
    priority: 999,
    subject: { type: 'user', ids: ['user_007'], attributes: [] },
    resource: { resourceType: 'TABLE', resourceId: 'sales_db.*', columns: [], excludeColumns: [], visibility: 'VISIBLE' },
    actions: ['read.*', 'export.*'],
    constraints: {},
    obligations: [],
    effectivePeriod: { from: '2026-08-01T00:00:00+08:00', to: null },
    meta: { owner: '安全响应组', description: '封禁用户：违规导出数据', updatedAt: '2026-09-10 14:02' }
  },
  {
    policyId: 'pol_ec_export_001',
    policyName: '数据分析师-订单导出（限流）',
    version: 2,
    status: 'published',
    effect: 'GRANT',
    priority: 50,
    subject: { type: 'role', ids: ['data_analyst'], attributes: [] },
    resource: {
      resourceType: 'TABLE',
      resourceId: 'sales_db.sales_order',
      columns: ['order_id', 'amount', 'region', 'status', 'customer_phone'],
      excludeColumns: ['customer_id_card'],
      visibility: 'CONDITIONAL'
    },
    actions: ['export.csv'],
    constraints: { rowExpr: "domain = 'EC'", maskRules: [], env: [] },
    obligations: [{ type: 'AUDIT', level: 'summary', sync: true }, { type: 'RATE_LIMIT', maxRows: 10000 }],
    effectivePeriod: { from: '2026-09-01T00:00:00+08:00', to: null },
    meta: { owner: '数据平台组', description: '分析师导出订单，10 万行上限', updatedAt: '2026-09-12 09:41' }
  },
  {
    policyId: 'pol_tag_claim_risk',
    policyName: '风控分析师-黑名单标签圈选',
    version: 1,
    status: 'approved',
    effect: 'GRANT',
    priority: 50,
    subject: { type: 'role', ids: ['risk_analyst'], attributes: [] },
    resource: { resourceType: 'TAG', resourceId: '标签/业务域/风控/黑名单', columns: [], excludeColumns: [], visibility: 'VISIBLE', tagTarget: null },
    actions: ['tag.claim'],
    constraints: { rowExpr: '', maskRules: [], env: [] },
    obligations: [{ type: 'AUDIT', level: 'detail', sync: true }],
    effectivePeriod: { from: '2026-09-20T00:00:00+08:00', to: null },
    meta: { owner: '风控组', description: '圈选黑名单客户（需 tagLevel T2+）', updatedAt: '2026-09-20 16:30' }
  },
  {
    policyId: 'pol_intern_readonly',
    policyName: '实习生-订单只读三列',
    version: 1,
    status: 'pending',
    effect: 'GRANT',
    priority: 10,
    subject: { type: 'role', ids: ['intern'], attributes: [] },
    resource: {
      resourceType: 'TABLE',
      resourceId: 'sales_db.sales_order',
      columns: ['order_id', 'region', 'status'],
      excludeColumns: ['amount', 'customer_phone'],
      visibility: 'CONDITIONAL'
    },
    actions: ['read.snapshot'],
    constraints: { rowExpr: "status = 'paid'", maskRules: [], env: [] },
    obligations: [{ type: 'AUDIT', level: 'basic', sync: false }],
    effectivePeriod: { from: '2026-09-25T00:00:00+08:00', to: '2026-12-31T00:00:00+08:00' },
    meta: { owner: '数据安全组', description: '实习生只读，待审批', updatedAt: '2026-09-21 11:08' }
  }
]

export const statusMeta = {
  draft: { label: '草稿', type: 'info' },
  pending: { label: '审批中', type: 'warning' },
  approved: { label: '已审批', type: 'primary' },
  published: { label: '已发布', type: 'success' },
  superseded: { label: '已失效', type: 'danger' }
}

// 状态流转（策略生命周期状态机，C24）
export const transitions = {
  draft: [{ to: 'pending', label: '提交审批', type: 'warning' }],
  pending: [
    { to: 'approved', label: '审批通过', type: 'primary' },
    { to: 'draft', label: '驳回', type: 'danger' }
  ],
  approved: [{ to: 'published', label: '发布上线', type: 'success' }],
  published: [{ to: 'superseded', label: '下线失效', type: 'danger' }],
  superseded: []
}

export const users = [
  { id: 'u_001', name: '张三', roles: ['regional_manager'], level: 'manager', groups: ['team_east'], domain: 'EC', tagLevel: 'T1', desc: '区域经理·华东' },
  { id: 'u_002', name: '李四', roles: ['intern'], level: 'intern', groups: ['team_east'], domain: 'EC', tagLevel: 'T0', desc: '实习生' },
  { id: 'user_007', name: 'user_007', roles: ['employee'], level: 'staff', groups: ['team_west'], domain: 'EC', tagLevel: 'T0', desc: '封禁用户' },
  { id: 'u_004', name: '王五', roles: ['data_analyst'], level: 'senior', groups: ['team_data'], domain: 'EC', tagLevel: 'T0', desc: '数据分析师' },
  { id: 'u_005', name: '赵六', roles: ['risk_analyst'], level: 'senior', groups: ['team_risk'], domain: 'EC', tagLevel: 'T2', desc: '风控分析师（tagLevel T2）' },
  { id: 'u_006', name: '钱七', roles: ['risk_analyst'], level: 'junior', groups: ['team_risk'], domain: 'EC', tagLevel: 'T0', desc: '风控新人（tagLevel T0）' }
]
