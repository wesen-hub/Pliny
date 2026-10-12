/**
 * PDP 判定链（主体链路核心）
 * 执行顺序（与《权限校验全链路》一致）：
 *   ① 策略匹配（subject 域 × resourceId × action × 有效期）
 *   ② Effect 裁决：DENY 命中 → 立即 DENY（R-D2 DENY-overrides）
 *   ③ 表级短路：HIDDEN → DENY（R-D6）；ALL → PERMIT 无行级
 *   ④ GRANT 约束合并（R-D7/R-D8/R-D9/R-D16，见 conflict.js）
 *   ⑤ 标签门槛短路（R-T2 / R-D11 第④步）：tagLevel 不足 → DENY
 *   ⑥ 无任何 GRANT 命中 → DENY（fail-closed，R-F1）
 *   ⑦ 列级短路：合并后可见列为空 → DENY（R-D10）
 */
import {
  Effect, Visibility, Decision, Artifact,
} from './model.js';
import {
  mergeRowPredicates, mergeColumns, mergeMasks, mergeObligations,
} from './conflict.js';

/** subject 匹配：type 与 ids 相交（role 按 roles 列表、user 按 userId、group 按 attributes.groups） */
function subjectMatches(policy, context) {
  const { type, ids } = policy.subject;
  if (!ids || ids.length === 0) return false;
  switch (type) {
    case 'role':
      return context.roles.some((r) => ids.includes(r));
    case 'user':
      return ids.includes(context.userId);
    case 'group': {
      const groups = context.attributes.groups || [];
      return groups.some((g) => ids.includes(g));
    }
    default:
      return false;
  }
}

/** 资源匹配：三段式 resourceId 精确匹配（v1，R-T10 同语义） */
function resourceMatches(policy, resourceId) {
  return policy.resourceId === resourceId;
}

/** 动作匹配 */
function actionMatches(policy, action) {
  return policy.actions.length === 0 || policy.actions.includes(action);
}

/** 有效期匹配（可选字段） */
function periodMatches(policy, now = Date.now()) {
  if (!policy.effectivePeriod) return true;
  const { from, to } = policy.effectivePeriod;
  if (from && now < new Date(from).getTime()) return false;
  if (to && now > new Date(to).getTime()) return false;
  return true;
}

/** 匹配结果：{ grants: Policy[], denies: Policy[] } */
function matchPolicies(request, policies) {
  const grants = [];
  const denies = [];
  for (const p of policies) {
    if (!subjectMatches(p, request.context)) continue;
    if (!resourceMatches(p, request.resourceId)) continue;
    if (!actionMatches(p, request.action)) continue;
    if (!periodMatches(p)) continue;
    if (p.effect === Effect.DENY) denies.push(p);
    else grants.push(p);
  }
  return { grants, denies };
}

/**
 * 标签门槛判定（R-T2 / R-D11 第④步）
 * @returns {boolean} 是否通过
 */
function checkTagGate(constraint, context) {
  const ts = constraint.tagScope;
  if (!ts || !ts.tagPath || !ts.requiredLevel) return true;
  const userLevel = context.tagLevels[ts.tagPath] ?? 0; // 默认 T0
  return userLevel >= ts.requiredLevel;
}

/**
 * 判定入口
 * @param {AuthorizationRequest} request
 * @param {Policy[]} policies 当前生效版本的策略快照（planVersion 绑定）
 * @returns {Artifact}
 */
export function decide(request, policies) {
  const { context } = request;
  const { grants, denies } = matchPolicies(request, policies);

  // ② Effect 裁决：DENY-overrides（R-D2）——任何 DENY 命中立即拒绝
  if (denies.length > 0) {
    const p = denies[0];
    return new Artifact({
      decision: Decision.DENY,
      denyReason: `DENY 策略命中: ${p.policyId}(${p.policyName})`,
      obligations: mergeObligations(denies.map((d) => d.obligations)),
      meta: { matchedDenies: denies.map((d) => d.policyId) },
    });
  }

  // ⑥ fail-closed：无 GRANT 命中 → DENY（R-F1）
  if (grants.length === 0) {
    return new Artifact({
      decision: Decision.DENY,
      denyReason: '无任何 GRANT 策略命中（fail-closed）',
    });
  }

  // ③ 表级短路（R-D6）
  const visibility = grants.map((g) => g.constraints.visible ?? Visibility.CONDITIONAL);
  if (visibility.includes(Visibility.HIDDEN)) {
    return new Artifact({
      decision: Decision.DENY,
      denyReason: '表级可见性 HIDDEN（R-D6 短路）',
      meta: { matchedGrants: grants.map((g) => g.policyId) },
    });
  }
  const allVisible = visibility.every((v) => v === Visibility.ALL);

  // ⑤ 标签门槛短路（R-T2）：任何一条带 tagScope 的约束门槛不足 → DENY
  for (const g of grants) {
    if (!checkTagGate(g.constraints, context)) {
      return new Artifact({
        decision: Decision.DENY,
        denyReason: `标签门槛不足（tagLevel 需 T${g.constraints.tagScope.requiredLevel}+）`,
        meta: { matchedGrants: grants.map((x) => x.policyId) },
      });
    }
  }

  // ④ 约束合并
  const rowPredicate = allVisible
    ? null
    : mergeRowPredicates(grants.map((g) => g.constraints.rowExpr ?? null));
  const visibleColumns = mergeColumns(
    grants.map((g) => {
      const c = g.constraints;
      if (c.columns || (c.excludeColumns && c.excludeColumns.length > 0)) {
        return { columns: c.columns ?? null, excludeColumns: c.excludeColumns ?? [] };
      }
      return null;
    }),
  );
  const masks = mergeMasks(grants.map((g) => g.constraints.masks ?? []));
  const obligations = mergeObligations(grants.map((g) => g.obligations ?? []));
  const tagScope = mergeTagScope(grants.map((g) => g.constraints.tagScope ?? null));

  // ⑦ 列级短路：可见列为空 → DENY（R-D10）
  if (visibleColumns !== null && visibleColumns.length === 0) {
    return new Artifact({
      decision: Decision.DENY,
      denyReason: '合并后可见列为空（R-D10 列级短路）',
      meta: { matchedGrants: grants.map((g) => g.policyId) },
    });
  }

  return new Artifact({
    decision: Decision.PERMIT,
    rowPredicate,
    visibleColumns,
    masks,
    tagScope,
    obligations,
    meta: {
      matchedGrants: grants.map((g) => g.policyId),
      mergedFrom: grants.length,
    },
  });
}

/**
 * 合并 tagScope：多条 GRANT 各自带标签约束时
 * 取门槛最高者（最严）；实体列表模式（list）需拼接实体集合（示例实现取首个 list）。
 * 校验式（verify）不注入谓词，只保留授权声明。
 */
function mergeTagScope(scopes) {
  const list = scopes.filter((s) => s !== null);
  if (list.length === 0) return null;
  // 门槛取最高（最严）
  const requiredLevel = Math.max(...list.map((s) => s.requiredLevel ?? 0));
  // v1：仅支持单标签约束合并；多标签 AND 语义（多个 TAG_REF）由渲染层叠加
  const base = { ...list[0], requiredLevel };
  if (base.expandMode === 'list' && base.entities) {
    base.entities = [...base.entities];
  }
  return base;
}
