/**
 * 核心模型：引擎无关语义模型（设计文档 §3）
 * 本模块只含数据结构与常量定义，不掺任何方言与执行逻辑。
 * 语义模型与引擎无关——同一份模型可被任意方言 Renderer 消费。
 */

// ============ 常量枚举 ============

/** 策略效果 */
export const Effect = {
  GRANT: 'GRANT',
  DENY: 'DENY',
};

/** 表级可见性（R-D6 短路判定输入） */
export const Visibility = {
  ALL: 'ALL',               // 全表可见，无行级过滤
  CONDITIONAL: 'CONDITIONAL', // 行级过滤
  HIDDEN: 'HIDDEN',         // 表级不可见 → 直接 DENY
};

/** 判定结果 */
export const Decision = {
  PERMIT: 'PERMIT',
  DENY: 'DENY',
};

/** 掩码严格度（R-D9 最严掩码：FULL > NULL > HASH > PARTIAL > REPLACE） */
export const MASK_SEVERITY = {
  FULL: 5,     // 完全不投影
  NULL: 4,     // 置空
  HASH: 3,     // 哈希
  PARTIAL: 2,  // 部分遮蔽
  REPLACE: 1,  // 替换为固定值
};

/** 审计义务强度（R-D16：合并取更强） */
export const AUDIT_MODE_SEVERITY = {
  sync: 2,
  async: 1,
};

/** 标签展开模式（R-T7，跨库定稿枚举） */
export const TAG_EXPAND_MODE = {
  LIST: 'list',     // 模式 A：实体列表下推（< 1000）
  VERIFY: 'verify', // 校验式：大标签圈选授权声明（不注入标签谓词）
};

/** 行谓词引用来源（绑定阶段解析） */
export const REF_KINDS = {
  SUBJECT: 'subject',   // ${subject.xxx}
  RESOURCE: 'resource', // ${resource.xxx}
};

// ============ 主体与请求 ============

/** 授权上下文（PEP 侧 Provider 提取后上报） */
export class AuthorizationContext {
  constructor({ userId, roles = [], attributes = {}, tagLevels = {} } = {}) {
    this.userId = userId;
    this.roles = roles;
    this.attributes = attributes;
    this.tagLevels = tagLevels; // tagPath -> level（R-T2 门槛输入）
  }
}

/** 判定请求（PEP → PDP） */
export class AuthorizationRequest {
  /**
   * @param {AuthorizationContext} context
   * @param {string} resourceId 三段式：ns.type.name（如 sdm.sales_order），v1 精确匹配
   * @param {string} action     如 select / insert / update / delete / export
   * @param {string} dialect    目标方言：mysql / tidb / trino / es / neo4j / spark / flink
   */
  constructor({ context, resourceId, action, dialect = 'mysql' } = {}) {
    this.context = context;
    this.resourceId = resourceId;
    this.action = action;
    this.dialect = dialect;
  }
}

// ============ 策略 ============

/**
 * 策略（管理面 PolicyVersion 的编译前语义模型）
 * subject.resourceId 三段式；resourceId 精确匹配（v1）
 */
export class Policy {
  constructor({
    policyId, policyName, effect,
    subject,          // { type: 'role'|'user'|'group', ids: [...] }
    resourceId,       // 三段式 resourceId
    actions = [],     // 授权动作集合
    constraints = {}, // { visible, rowExpr, columns, excludeColumns, masks, tagScope }
    obligations = [], // [{ type: 'AUDIT', mode: 'sync'|'async' }]
    priority = 100,   // 数值越大优先级越高（同级 Effect 对撞时 DENY 仍优先）
    effectivePeriod,  // { from: '2026-01-01', to: '2026-12-31' } 可选
  } = {}) {
    this.policyId = policyId;
    this.policyName = policyName;
    this.effect = effect;
    this.subject = subject;
    this.resourceId = resourceId;
    this.actions = actions;
    this.constraints = constraints;
    this.obligations = obligations;
    this.priority = priority;
    this.effectivePeriod = effectivePeriod;
  }
}

// ============ 判定产物 ============

/**
 * 判定产物（PDP 输出，与《产物缓存读写链路》结构对齐）
 * v1 实现核心字段：判定结论 + 合并约束 + 标签范围 + 义务 + 元信息
 * 签名/键空间版本化等安全字段在真实 PDP 服务层叠加，核心引擎只保证语义。
 */
export class Artifact {
  constructor({
    decision,          // Decision.PERMIT | Decision.DENY
    rowPredicate = null,   // 合并后行谓词 AST（AND 连接，R-D7）
    visibleColumns = null, // 可见列集合（交集 − exclude，R-D8）；null = 不裁剪
    masks = [],            // 合并后掩码列表（最严者胜，R-D9）
    tagScope = null,       // { tagPath, expandMode, snapshotVersion, targetKey, entities }
    obligations = [],      // 合并后义务（并集取最强，R-D16）
    denyReason = null,     // DENY 时的人读原因（审计追溯）
    planVersion = 1,       // 策略版本（缓存键空间版本化，R-K 键三段式）
    meta = {},
  } = {}) {
    this.decision = decision;
    this.rowPredicate = rowPredicate;
    this.visibleColumns = visibleColumns;
    this.masks = masks;
    this.tagScope = tagScope;
    this.obligations = obligations;
    this.denyReason = denyReason;
    this.planVersion = planVersion;
    this.meta = meta;
  }
}

// ============ 行谓词 AST 节点构造辅助 ============

/** 等值：col = ref（如 region = ${subject.region}） */
export const eq = (column, value) => ({ op: 'EQ', column, value });

/** 成员：col IN ref（ref 为列表，如 dept IN ${subject.depts}） */
export const inList = (column, value) => ({ op: 'IN', column, value });

/** 范围：min <= col <= max（min/max 可为 null 表示单边） */
export const range = (column, { min = null, max = null } = {}) => ({ op: 'RANGE', column, min, max });

/** 正则：col REGEXP pattern（字面量） */
export const regex = (column, pattern) => ({ op: 'REGEX', column, pattern });

/** 逻辑与（子节点列表） */
export const and = (...children) => ({ op: 'AND', children });

/** 逻辑或（子节点列表） */
export const or = (...children) => ({ op: 'OR', children });

/** 恒真（无行级约束） */
export const TRUE = { op: 'CONST', value: true };

/** subject 引用占位 */
export const subjectRef = (ref) => ({ type: 'ref', kind: REF_KINDS.SUBJECT, ref });

/** 字面量 */
export const literal = (value) => ({ type: 'literal', value });
