/**
 * 主体链路完整演示：PDP 判定 → 渲染 → 绑定 → 注入 → 投影
 * 运行：node demo.js
 *
 * 覆盖场景：
 *   S1 行级过滤 + 掩码 + 列裁剪（区域经理）
 *   S2 DENY-overrides（黑名单用户）
 *   S3 fail-closed 无策略命中
 *   S4 标签模式 A 列表下推（营销分析）
 *   S5 标签门槛不足 → DENY（R-T2）
 *   S6 表级 HIDDEN 短路（R-D6）
 *   S7 绑定失败 → DENY（fail-closed，R-F1）
 */
import {
  AuthorizationContext, AuthorizationRequest, Policy,
  Effect, Visibility, Decision,
  eq, inList, range, and, subjectRef, literal,
} from './core/model.js';
import { decide } from './core/decision.js';
import { renderPredicate } from './core/renderer.js';
import { bind, BindError } from './core/binder.js';
import {
  injectWhere, renderBoundPredicate, projectColumns,
  renderTagPredicate, tagBindValues,
} from './core/injector.js';

// ============ 资源元数据 ============
const SALES_ORDER_SCHEMA = ['id', 'order_no', 'customer_id', 'region', 'amount', 'phone', 'status'];

// ============ 策略集（生效版本快照） ============
const policies = [
  // 区域经理：本区域行级过滤 + amount 掩码 + phone 列裁剪 + sync 审计
  new Policy({
    policyId: 'pol_region_001',
    policyName: '区域经理行权限',
    effect: Effect.GRANT,
    subject: { type: 'role', ids: ['regional_manager'] },
    resourceId: 'sdm.sales_order',
    actions: ['select'],
    constraints: {
      visible: Visibility.CONDITIONAL,
      rowExpr: and(
        eq('region', subjectRef('subject.region')),
        inList('status', subjectRef('subject.allowedStatus')),
      ),
      columns: ['id', 'order_no', 'customer_id', 'region', 'amount', 'status'],
      masks: [{ column: 'amount', type: 'HASH' }],
    },
    obligations: [{ type: 'AUDIT', mode: 'sync' }],
  }),
  // 黑名单：张三显式 DENY（DENY-overrides 演示）
  new Policy({
    policyId: 'pol_deny_zhangsan_002',
    policyName: '张三黑名单',
    effect: Effect.DENY,
    subject: { type: 'user', ids: ['zhangsan'] },
    resourceId: 'sdm.sales_order',
    actions: ['select'],
    priority: 999,
    obligations: [{ type: 'AUDIT', mode: 'sync' }],
  }),
  // 营销分析：标签模式 A 列表下推（高价值客户圈选结果）
  new Policy({
    policyId: 'pol_marketing_tag_003',
    policyName: '高价值客户标签权限',
    effect: Effect.GRANT,
    subject: { type: 'role', ids: ['marketing_analyst'] },
    resourceId: 'sdm.sales_order',
    actions: ['select'],
    constraints: {
      visible: Visibility.CONDITIONAL,
      tagScope: {
        tagPath: '业务域/客户价值/高价值',
        expandMode: 'list', // 模式 A：实体列表下推（跨库定稿 R-T5）
        snapshotVersion: 'ts-20260928-001',
        targetKey: 'customer_id',
        requiredLevel: 2, // tag.claim 需 T2+（R-T2）
        entities: [101, 102, 103, 104, 105], // 演示值：真实场景由标签服务 getEntities 返回
      },
      columns: ['id', 'order_no', 'customer_id', 'amount', 'status'],
    },
    obligations: [{ type: 'AUDIT', mode: 'sync' }],
  }),
  // 实习生：表级不可见
  new Policy({
    policyId: 'pol_intern_hidden_004',
    policyName: '实习生表级不可见',
    effect: Effect.GRANT,
    subject: { type: 'role', ids: ['intern'] },
    resourceId: 'sdm.sales_order',
    actions: ['select'],
    constraints: { visible: Visibility.HIDDEN },
  }),
];

// ============ PEP 端到端封装（模拟适配器调管线） ============
/**
 * 端到端：判定 → 渲染 → 绑定 → 注入 → 投影
 * @returns {{decision, sql, bindOrder, tags, denyReason, obligations}}
 */
function executeQuery(sql, request) {
  const artifact = decide(request, policies);

  if (artifact.decision === Decision.DENY) {
    return {
      decision: 'DENY',
      sql: null,
      bindOrder: [],
      tags: null,
      denyReason: artifact.denyReason,
      obligations: artifact.obligations,
    };
  }

  // —— 判定产物 → 方言渲染（模板无值，C1） ——
  let permSql = null;
  let permBindOrder = [];
  let tagValues = [];

  if (artifact.rowPredicate) {
    const { template, placeholders } = renderPredicate(artifact.rowPredicate);
    permBindOrder = bind(placeholders, request.context); // 绑定失败抛 BindError → 上层降 DENY
    permSql = renderBoundPredicate(template, permBindOrder);
  }

  // —— 标签模式 A 列表下推 ——
  const tagPred = renderTagPredicate(artifact.tagScope);
  tagValues = tagBindValues(artifact.tagScope);
  if (tagPred) permSql = permSql ? `(${permSql}) AND (${tagPred})` : tagPred;

  // —— 注入 + 投影 ——
  let rewritten = injectWhere(sql, permSql);
  rewritten = projectColumns(rewritten, artifact, SALES_ORDER_SCHEMA);

  // —— bindOrder 顺序契约：原业务 ? 在前，权限 ? 在后 ——
  const bizParamCount = (sql.match(/\?/g) || []).length;
  const bindOrder = [
    ...Array.from({ length: bizParamCount }, (_, i) => ({ name: `业务参数${i + 1}`, value: `<业务值${i + 1}>` })),
    ...permBindOrder.map((b) => ({ name: '权限', value: b.value })),
    ...tagValues.map((v, i) => ({ name: `标签实体${i + 1}`, value: v })),
  ];

  return {
    decision: 'PERMIT',
    sql: rewritten,
    bindOrder,
    tags: artifact.tagScope,
    denyReason: null,
    obligations: artifact.obligations,
    matched: artifact.meta.matchedGrants,
  };
}

// ============ 演示场景 ============
const divider = '─'.repeat(88);

function runScene(no, title, request, sql) {
  console.log(`\n${divider}`);
  console.log(`S${no}  ${title}`);
  console.log(`${divider}`);
  console.log(`请求: userId=${request.context.userId} roles=[${request.context.roles}] `
    + `action=${request.action} resource=${request.resourceId}`);
  console.log(`原始 SQL: ${sql}`);
  try {
    const r = executeQuery(sql, request);
    console.log(`判定: ${r.decision}${r.denyReason ? `（${r.denyReason}）` : ''}`);
    if (r.decision === 'PERMIT') {
      console.log(`命中策略: [${r.matched.join(', ')}]`);
      console.log(`改写 SQL: ${r.sql}`);
      console.log(`绑定顺序: ${JSON.stringify(r.bindOrder)}`);
      if (r.tags) {
        console.log(`标签范围: tagPath=${r.tags.tagPath} expandMode=${r.tags.expandMode} `
          + `snapshotVersion=${r.tags.snapshotVersion} targetKey=${r.tags.targetKey}`);
      }
    }
    console.log(`义务: ${JSON.stringify(r.obligations)}`);
  } catch (e) {
    if (e instanceof BindError) {
      console.log(`判定: DENY（${e.message}）`);
    } else {
      console.log(`异常: ${e.name}: ${e.message}`);
    }
  }
}

// S1 区域经理：行级过滤 + 掩码 + 列裁剪
runScene(1, '区域经理（行谓词 AND + 掩码 + 列裁剪）', new AuthorizationRequest({
  context: new AuthorizationContext({
    userId: 'lisi', roles: ['regional_manager'],
    attributes: { region: '华东', allowedStatus: ['paid', 'shipped'] },
  }),
  resourceId: 'sdm.sales_order', action: 'select', dialect: 'mysql',
}), 'SELECT * FROM sales_order WHERE order_date >= ?');

// S2 黑名单：DENY-overrides
runScene(2, '黑名单张三（DENY-overrides）', new AuthorizationRequest({
  context: new AuthorizationContext({
    userId: 'zhangsan', roles: ['regional_manager'],
    attributes: { region: '华东', allowedStatus: ['paid', 'shipped'] },
  }),
  resourceId: 'sdm.sales_order', action: 'select', dialect: 'mysql',
}), 'SELECT * FROM sales_order WHERE order_date >= ?');

// S3 无策略命中：fail-closed
runScene(3, '无角色用户（fail-closed）', new AuthorizationRequest({
  context: new AuthorizationContext({ userId: 'zhaoliu', roles: [] }),
  resourceId: 'sdm.sales_order', action: 'select', dialect: 'mysql',
}), 'SELECT * FROM sales_order');

// S4 标签模式 A：列表下推
runScene(4, '营销分析（标签模式 A 列表下推）', new AuthorizationRequest({
  context: new AuthorizationContext({
    userId: 'qianqi', roles: ['marketing_analyst'],
    attributes: {},
    tagLevels: { '业务域/客户价值/高价值': 2 },
  }),
  resourceId: 'sdm.sales_order', action: 'select', dialect: 'mysql',
}), 'SELECT * FROM sales_order WHERE amount > ?');

// S5 标签门槛不足：R-T2 短路
runScene(5, '营销分析标签门槛不足（T0 < T2，R-T2 短路）', new AuthorizationRequest({
  context: new AuthorizationContext({
    userId: 'zhouba', roles: ['marketing_analyst'],
    attributes: {},
    tagLevels: { '业务域/客户价值/高价值': 0 },
  }),
  resourceId: 'sdm.sales_order', action: 'select', dialect: 'mysql',
}), 'SELECT * FROM sales_order WHERE amount > ?');

// S6 表级 HIDDEN：R-D6 短路
runScene(6, '实习生（表级 HIDDEN 短路）', new AuthorizationRequest({
  context: new AuthorizationContext({ userId: 'sunba', roles: ['intern'] }),
  resourceId: 'sdm.sales_order', action: 'select', dialect: 'mysql',
}), 'SELECT * FROM sales_order');

// S7 绑定失败：region 属性缺失 → fail-closed
runScene(7, '区域经理缺 region 属性（绑定失败 fail-closed）', new AuthorizationRequest({
  context: new AuthorizationContext({
    userId: 'wujiu', roles: ['regional_manager'],
    attributes: {}, // 无 region / allowedStatus
  }),
  resourceId: 'sdm.sales_order', action: 'select', dialect: 'mysql',
}), 'SELECT * FROM sales_order WHERE order_date >= ?');

// 多策略合并演示：两个 GRANT 同时命中（列交集）
console.log(`\n${divider}`);
console.log('S8  多策略合并（列交集 + 掩码最严 + 义务最强）');
console.log(`${divider}`);
const multiPolicies = [
  ...policies,
  new Policy({
    policyId: 'pol_finance_005',
    policyName: '财务补充列授权（冲突掩码：PARTIAL）',
    effect: Effect.GRANT,
    subject: { type: 'role', ids: ['regional_manager'] },
    resourceId: 'sdm.sales_order',
    actions: ['select'],
    constraints: {
      visible: Visibility.CONDITIONAL,
      rowExpr: range('amount', { min: literal(0), max: literal(50000) }),
      columns: ['id', 'order_no', 'customer_id', 'amount', 'status'],
      excludeColumns: ['status'], // 与 pol_region_001 交集后 status 被剔除
      masks: [{ column: 'amount', type: 'PARTIAL' }], // HASH vs PARTIAL → 取最严 HASH（R-D9）
    },
    obligations: [{ type: 'AUDIT', mode: 'async' }], // sync vs async → 取最强 sync（R-D16）
  }),
];
const req8 = new AuthorizationRequest({
  context: new AuthorizationContext({
    userId: 'lisi', roles: ['regional_manager'],
    attributes: { region: '华东', allowedStatus: ['paid', 'shipped'] },
  }),
  resourceId: 'sdm.sales_order', action: 'select', dialect: 'mysql',
});
const art8 = decide(req8, multiPolicies);
console.log(`请求: userId=lisi roles=[regional_manager]（两条 GRANT 同时命中）`);
console.log(`判定: ${art8.decision}`);
console.log(`合并行谓词: ${JSON.stringify(art8.rowPredicate)}`);
console.log(`合并可见列: [${art8.visibleColumns}]（列交集 − excludeColumns，status 被剔除）`);
console.log(`合并掩码: ${JSON.stringify(art8.masks)}（HASH 胜 PARTIAL，R-D9）`);
console.log(`合并义务: ${JSON.stringify(art8.obligations)}（sync 胜 async，R-D16）`);

console.log(`\n${divider}`);
console.log('主体链路演示结束');
