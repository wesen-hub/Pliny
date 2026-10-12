// ============ 判定演示剧本（静态 mock，预写好完整结果） ============
// 每个剧本：request（请求）+ result（判定结果）+ 演示重点

export const scenarios = [
  {
    id: 's1',
    name: '张三 · 区域经理 · 命中 GRANT + 行级过滤 + 掩码',
    badge: '典型命中',
    badgeType: 'success',
    highlights: ['GRANT 命中', '行谓词下推', '列掩码', 'Plan 缓存未命中（冷链路）'],
    request: {
      userId: 'u_001', userName: '张三',
      resourceId: 'sales_db.sales_order',
      resourceType: 'TABLE',
      action: 'read.snapshot',
      engine: 'mysql', dialect: 'mysql',
      env: { ipRange: 'office', time: '2026-09-22 10:00' }
    },
    decision: {
      type: 'PERMIT',
      label: 'PERMIT',
      color: 'success',
      reason: '命中 pol_sales_region_001（GRANT）',
      matched: ['pol_sales_region_001'],
      planKey: 'hash(planVersion, resourceId, action, rolesFp, stableAttrFp)',
      planHit: false,
      steps: [
        { step: '⑮ 查 Plan 缓存', note: '未命中（冷链路）' },
        { step: '⑯ 策略匹配', note: 'L3 索引过滤 → 命中 1 条 GRANT' },
        { step: '⑱ 裁决', note: '无 DENY 命中 → PERMIT' },
        { step: '⑲ 短路检查', note: '表级 CONDITIONAL / 动作 read.snapshot / 无标签门槛 → 通过' },
        { step: '⑳ 约束合并', note: "rowExpr: region='华东' AND status IN('paid','pending')；掩码 PARTIAL(customer_phone)" },
        { step: '㉑ 义务合并', note: 'AUDIT(detail, sync)' }
      ],
      artifact: {
        artifactVersion: '1.0',
        dialect: 'mysql',
        decision: 'PERMIT',
        pushdown: {
          predicate: "(`region` = ? AND `status` IN ('paid','pending'))",
          placeholders: [{ index: 0, ref: 'subject.region', type: 'varchar' }],
          columnProjection: {
            visible: ['order_id', 'amount', 'region', 'status', 'customer_phone'],
            masks: [{ column: 'customer_phone', rule: 'PARTIAL' }]
          }
        },
        obligations: [{ type: 'AUDIT', level: 'detail', sync: true }]
      },
      originalSql: 'SELECT * FROM sales_order WHERE created_at >= \'2026-09-01\'',
      rewrittenSql: "SELECT `order_id`,`amount`,`region`,`status`,CONCAT(LEFT(`customer_phone`,3),'****',RIGHT(`customer_phone`,4)) AS `customer_phone` FROM sales_order WHERE created_at >= '2026-09-01' AND (`region` = '华东' AND `status` IN ('paid','pending'))",
      rows: [
        { order_id: 'SO-1001', amount: 2580.00, region: '华东', status: 'paid', customer_phone: '138****1024' },
        { order_id: 'SO-1002', amount: 990.50, region: '华东', status: 'pending', customer_phone: '139****8821' },
        { order_id: 'SO-1003', amount: 4520.00, region: '华东', status: 'paid', customer_phone: '137****3302' }
      ]
    }
  },
  {
    id: 's2',
    name: '李四 · 实习生 · 策略未发布 → 无匹配 DENY',
    badge: '未发布不生效',
    badgeType: 'danger',
    highlights: ['无匹配 → DENY', '未发布策略不参与判定', 'fail-closed 默认拒绝'],
    request: {
      userId: 'u_002', userName: '李四',
      resourceId: 'sales_db.sales_order',
      resourceType: 'TABLE',
      action: 'read.snapshot',
      engine: 'mysql', dialect: 'mysql',
      env: { ipRange: 'office', time: '2026-09-22 10:00' }
    },
    decision: {
      type: 'DENY',
      label: 'DENY',
      color: 'danger',
      reason: '无任何 active 策略命中（pol_intern_readonly 处于审批中，不参与判定）',
      matched: [],
      planKey: 'hash(planVersion, resourceId, action, rolesFp, stableAttrFp)',
      planHit: true,
      steps: [
        { step: '⑮ 查 Plan 缓存', note: '命中（DENY 决策被缓存）' },
        { step: '⑯ 策略匹配', note: 'L3 索引：active 策略 0 条命中' },
        { step: '⑰ 无匹配裁决', note: '默认拒绝 → DENY（fail-closed）' }
      ],
      artifact: { artifactVersion: '1.0', dialect: 'mysql', decision: 'DENY', denyReason: 'NO_POLICY_MATCHED', pushdown: null, obligations: [] },
      originalSql: 'SELECT * FROM sales_order',
      rewrittenSql: '-- 请求被拒绝，不执行（NO_POLICY_MATCHED）',
      rows: []
    }
  },
  {
    id: 's3',
    name: 'user_007 · 封禁用户 · DENY-overrides 压制 GRANT',
    badge: 'DENY 压制',
    badgeType: 'danger',
    highlights: ['DENY-overrides（R-D2）', 'user 型策略（分层缓存键 C21）', '即便命中 GRANT 也拒绝'],
    request: {
      userId: 'user_007', userName: 'user_007',
      resourceId: 'sales_db.sales_order',
      resourceType: 'TABLE',
      action: 'read.snapshot',
      engine: 'mysql', dialect: 'mysql',
      env: { ipRange: 'office', time: '2026-09-22 10:00' }
    },
    decision: {
      type: 'DENY',
      label: 'DENY',
      color: 'danger',
      reason: 'pol_deny_user_007（DENY, priority 999）命中，DENY-overrides 压制同名 GRANT',
      matched: ['pol_deny_user_007', 'pol_sales_region_001（被压制）'],
      planKey: 'hash(planVersion, resourceId, action, rolesFp, stableAttrFp, subjectIdsFp) ← 分层键 C21',
      planHit: false,
      steps: [
        { step: '⑮ 查 Plan 缓存', note: '键追加 subjectIdsFingerprint（域内存在 user 型策略，C21）' },
        { step: '⑯ 策略匹配', note: '命中 DENY 1 条 + GRANT 1 条' },
        { step: '⑱ 裁决', note: 'DENY-overrides → DENY' },
        { step: '⑲ 短路', note: 'DENY 即终态，不再合并约束' }
      ],
      artifact: { artifactVersion: '1.0', dialect: 'mysql', decision: 'DENY', denyReason: 'DENY_POLICY_MATCHED', pushdown: null, obligations: [] },
      originalSql: 'SELECT * FROM sales_order',
      rewrittenSql: '-- 请求被拒绝，不执行（DENY_POLICY_MATCHED）',
      rows: []
    }
  },
  {
    id: 's4',
    name: '王五 · 数据分析师 · 导出命中 + RATE_LIMIT 义务',
    badge: '义务联动',
    badgeType: 'warning',
    highlights: ['export.csv 动作', 'RATE_LIMIT 义务', '行数超限截断'],
    request: {
      userId: 'u_004', userName: '王五',
      resourceId: 'sales_db.sales_order',
      resourceType: 'TABLE',
      action: 'export.csv',
      engine: 'mysql', dialect: 'mysql',
      env: { ipRange: 'office', time: '2026-09-22 10:00' }
    },
    decision: {
      type: 'PERMIT',
      label: 'PERMIT',
      color: 'success',
      reason: '命中 pol_ec_export_001（GRANT, export.csv）',
      matched: ['pol_ec_export_001'],
      planKey: 'hash(planVersion, resourceId, action, rolesFp, stableAttrFp)',
      planHit: false,
      steps: [
        { step: '⑯ 策略匹配', note: '命中 1 条 GRANT（action=export.csv）' },
        { step: '⑳ 约束合并', note: "rowExpr: domain='EC'" },
        { step: '㉑ 义务合并', note: 'AUDIT(summary) + RATE_LIMIT(maxRows=10000)' }
      ],
      artifact: {
        artifactVersion: '1.0',
        dialect: 'mysql',
        decision: 'PERMIT',
        pushdown: { predicate: "(`domain` = 'EC')", placeholders: [], columnProjection: { visible: ['order_id', 'amount', 'region', 'status', 'customer_phone'], masks: [] } },
        obligations: [{ type: 'AUDIT', level: 'summary', sync: true }, { type: 'RATE_LIMIT', maxRows: 10000 }]
      },
      originalSql: 'SELECT * FROM sales_order',
      rewrittenSql: "SELECT `order_id`,`amount`,`region`,`status`,`customer_phone` FROM sales_order WHERE (`domain` = 'EC') LIMIT 10000",
      rows: [
        { order_id: 'SO-2001', amount: 1299.00, region: '华北', status: 'paid', customer_phone: '13577778888' },
        { order_id: 'SO-2002', amount: 88.00, region: '华南', status: 'paid', customer_phone: '13366669999' },
        { note: '…导出共 9,842 行（LIMIT 10000 由 RATE_LIMIT 义务生成，未触顶）' }
      ]
    }
  },
  {
    id: 's5',
    name: '赵六 · 风控分析师 · 标签圈选（T2 达标）',
    badge: '标签门槛',
    badgeType: 'primary',
    highlights: ['tag.claim 动作', 'tagLevel 门槛 T2+（R-T2）', 'TAG 型资源（C22）'],
    request: {
      userId: 'u_005', userName: '赵六',
      resourceId: '标签/业务域/风控/黑名单',
      resourceType: 'TAG',
      action: 'tag.claim',
      engine: 'tag-service', dialect: 'tag',
      env: { ipRange: 'office', time: '2026-09-22 10:00' }
    },
    decision: {
      type: 'PERMIT',
      label: 'PERMIT',
      color: 'success',
      reason: '命中 pol_tag_claim_risk（GRANT, tag.claim）；tagLevel=T2 ≥ 门槛 T2',
      matched: ['pol_tag_claim_risk'],
      planKey: 'hash(planVersion, resourceId, action, rolesFp, stableAttrFp)',
      planHit: false,
      steps: [
        { step: '⑯ 策略匹配', note: 'TAG 型资源精确匹配（R-T10）' },
        { step: '⑲ 短路检查-标签门槛', note: 'getUserTagLevels → T2 ≥ tag.claim 门槛 T2 → 通过' },
        { step: '㉑ 义务合并', note: 'AUDIT(detail, sync)' }
      ],
      artifact: {
        artifactVersion: '1.0',
        dialect: 'tag',
        decision: 'PERMIT',
        pushdown: null,
        tagScope: { tagPath: '标签/业务域/风控/黑名单', level: 'T2', canDrillDown: false, expandMode: 'B', targetKey: 'customer_id', factTable: 'row_tag_fact' },
        obligations: [{ type: 'AUDIT', level: 'detail', sync: true }]
      },
      originalSql: 'SELECT * FROM sales_order o JOIN row_tag_fact f ON o.id = f.row_id WHERE f.tag_path = \'标签/业务域/风控/黑名单\'',
      rewrittenSql: "SELECT * FROM sales_order o JOIN row_tag_fact f ON o.id = f.row_id WHERE f.tag_path = '标签/业务域/风控/黑名单'",
      rows: [
        { customer_id: 'C-501', customer_name: '陈某', tag_path: '标签/业务域/风控/黑名单' },
        { customer_id: 'C-502', customer_name: '吴某', tag_path: '标签/业务域/风控/黑名单' },
        { customer_id: 'C-503', customer_name: '郑某', tag_path: '标签/业务域/风控/黑名单' }
      ]
    }
  },
  {
    id: 's6',
    name: '钱七 · 风控新人 · 标签门槛不足 → DENY',
    badge: '门槛短路',
    badgeType: 'danger',
    highlights: ['tagLevel T0 < T2', '门槛短路拒绝（R-D11 第③步）', '策略命中但权限等级不足'],
    request: {
      userId: 'u_006', userName: '钱七',
      resourceId: '标签/业务域/风控/黑名单',
      resourceType: 'TAG',
      action: 'tag.claim',
      engine: 'tag-service', dialect: 'tag',
      env: { ipRange: 'office', time: '2026-09-22 10:00' }
    },
    decision: {
      type: 'DENY',
      label: 'DENY',
      color: 'danger',
      reason: '策略命中但 tagLevel=T0 < tag.claim 门槛 T2 → 门槛短路拒绝',
      matched: ['pol_tag_claim_risk（命中但门槛不足）'],
      planKey: 'hash(planVersion, resourceId, action, rolesFp, stableAttrFp)',
      planHit: false,
      steps: [
        { step: '⑯ 策略匹配', note: '命中 pol_tag_claim_risk' },
        { step: '⑲ 短路检查-标签门槛', note: 'getUserTagLevels → T0 < T2 → 拒绝' }
      ],
      artifact: { artifactVersion: '1.0', dialect: 'tag', decision: 'DENY', denyReason: 'TAG_LEVEL_INSUFFICIENT', pushdown: null, obligations: [] },
      originalSql: 'SELECT * FROM sales_order o JOIN row_tag_fact f ON o.id = f.row_id',
      rewrittenSql: '-- 请求被拒绝，不执行（TAG_LEVEL_INSUFFICIENT）',
      rows: []
    }
  },
  {
    id: 's7',
    name: '张三 · PDP 不可达 · fail-closed 降级',
    badge: '降级演练',
    badgeType: 'warning',
    highlights: ['PDP 熔断（状态机 CLOSED→OPEN）', 'fail-closed：PERMIT 缓存降级为 DENY', '审计留存'],
    request: {
      userId: 'u_001', userName: '张三',
      resourceId: 'sales_db.sales_order',
      resourceType: 'TABLE',
      action: 'read.snapshot',
      engine: 'mysql', dialect: 'mysql',
      env: { ipRange: 'office', time: '2026-09-22 10:00' }
    },
    decision: {
      type: 'DENY',
      label: 'DENY（降级）',
      color: 'danger',
      reason: 'PDP 不可达 → PEP 熔断器 OPEN → fail-closed 降级策略：宁可错杀不可漏放',
      matched: [],
      planKey: '—（PDP 不可达）',
      planHit: false,
      steps: [
        { step: '㉒ 调 PDP', note: '超时 3 次 → 熔断器 CLOSED→OPEN（状态机）' },
        { step: '降级判定', note: '缓存内 PERMIT 决策降级为 DENY（fail-closed）' },
        { step: '审计', note: '降级事件写审计（FAIL_CLOSED_FALLBACK）' }
      ],
      artifact: { artifactVersion: '1.0', dialect: 'mysql', decision: 'DENY', denyReason: 'FAIL_CLOSED_FALLBACK', pushdown: null, obligations: [{ type: 'AUDIT', level: 'deny', sync: true }] },
      originalSql: 'SELECT * FROM sales_order',
      rewrittenSql: '-- 请求被拒绝，不执行（FAIL_CLOSED_FALLBACK）',
      rows: []
    }
  }
]

// 数据表 schema（mock，用于结果集展示）
export const tableSchema = {
  'sales_db.sales_order': [
    { col: 'order_id', type: 'varchar(20)' },
    { col: 'customer_id', type: 'varchar(20)' },
    { col: 'amount', type: 'decimal(10,2)' },
    { col: 'region', type: 'varchar(20)' },
    { col: 'status', type: 'varchar(10)' },
    { col: 'customer_phone', type: 'varchar(20)' },
    { col: 'customer_id_card', type: 'varchar(18)' },
    { col: 'domain', type: 'varchar(10)' }
  ]
}
