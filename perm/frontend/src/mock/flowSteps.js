// ============ 全链路 38 步（与设计文档 §20.2 一一对应） ============
// lane: mgmt 管理面 / pep PEP / cache 缓存链 / pdp PDP / ext 引擎审计
// type: normal 判定 / compile 编译 / cache 缓存 / deny 拒绝 / tag 标签 / exec 执行 / audit 审计

export const lanes = [
  { id: 'mgmt', label: '管理面', short: '定义·发布·失效', color: '#b45309' },
  { id: 'pep', label: 'PEP 拦截与执行', short: '拦截→绑定→改写→返回', color: '#2563eb' },
  { id: 'cache', label: '缓存链', short: 'L1/L2/Plan 三级', color: '#16a34a' },
  { id: 'pdp', label: 'PDP 判定与编译', short: '匹配→裁决→合并→翻译', color: '#7c3aed' },
  { id: 'ext', label: '引擎与审计', short: '执行·审计汇聚', color: '#ea580c' }
]

export const phases = [
  { id: 'P0', name: 'P0 策略定义与发布', lane: 'mgmt', desc: '管理面 · 编译线起点' },
  { id: 'P1', name: 'P1 PEP 拦截', lane: 'pep', desc: '每次查询必经入口' },
  { id: 'P2', name: 'P2 产物缓存链查找', lane: 'cache', desc: '三级缓存读取顺序' },
  { id: 'P3', name: 'P3 PDP 判定线', lane: 'pdp', desc: '缓存未命中时执行' },
  { id: 'P4', name: 'P4 编译线终点 · 两线交汇', lane: 'pdp', desc: 'Plan + 方言片段组装' },
  { id: 'P5', name: 'P5 PEP 执行线', lane: 'pep', desc: '值在这里才出现' },
  { id: 'P6', name: 'P6 后处理与输出', lane: 'pep', desc: 'residual / 限流 / 审计 / 返回' },
  { id: 'P7', name: 'P7 失效闭环', lane: 'mgmt', desc: '新版本发布 → 广播失效' }
]

export const steps = [
  { no: 1, icon: '①', phase: 'P0', lane: 'mgmt', title: '定义策略 JSON', type: 'normal', branch: false, desc: '9 部分：effect/subject/resource/actions/constraints/obligations/effectivePeriod/priority/meta', output: 'Policy JSON' },
  { no: 2, icon: '②', phase: 'P0', lane: 'mgmt', title: '校验', type: 'normal', branch: false, desc: '语法（解析 AST）/ 语义（变量命名空间与类型）/ 冲突检测', output: '校验报告' },
  { no: 3, icon: '③', phase: 'P0', lane: 'mgmt', title: '审批', type: 'normal', branch: false, desc: 'pending → approved（可 rejected），策略生命周期状态机', output: '状态流转' },
  { no: 4, icon: '④', phase: 'P0', lane: 'mgmt', title: '编译模板 AST', type: 'compile', branch: false, desc: 'Lexer → Parser → Analyzer → Optimizer', output: '引擎无关 AST', cache: '写编译缓存①（键 policyId:version）' },
  { no: 5, icon: '⑤', phase: 'P0', lane: 'mgmt', title: '版本固化', type: 'compile', branch: false, desc: '不可变快照入 perm_policy_version，版本单调递增', output: '版本快照' },
  { no: 6, icon: '⑥', phase: 'P0', lane: 'mgmt', title: '原子发布', type: 'compile', branch: false, desc: '预热 → 事件广播 → active；任一步失败全量回滚', output: 'active 状态', cache: '可选预热热门方言 → 编译缓存②' },
  { no: 7, icon: '⑦', phase: 'P1', lane: 'pep', title: '业务发起查询', type: 'normal', branch: false, desc: 'MyBatis Mapper / JDBC，业务无感知', output: '原始 SQL' },
  { no: 8, icon: '⑧', phase: 'P1', lane: 'pep', title: 'PEP 拦截', type: 'normal', branch: false, desc: 'StatementHandler.prepare（@Order 对插件无效，靠注册顺序）', output: '拦截点' },
  { no: 9, icon: '⑨', phase: 'P1', lane: 'pep', title: 'SPI 提取上下文', type: 'normal', branch: false, desc: 'SubjectAttributeExtractor / ResourceAttributeExtractor / EnvironmentExtractor；团队树、角色继承在此展开（扁平化原则）', output: 'AuthorizationRequest' },
  { no: 10, icon: '⑩', phase: 'P2', lane: 'cache', title: '计算六元缓存键', type: 'cache', branch: false, desc: 'hash(planVersion, dialect, resourceId, action, rolesFp, stableAttrFp)；user/group 型域追加 subjectIdsFp（C21）', output: 'cacheKey' },
  { no: 11, icon: '⑪', phase: 'P2', lane: 'cache', title: '查 L1 Caffeine', type: 'cache', branch: false, desc: 'PEP 进程内 + fullKey 二次校验（碰撞视为未命中，安全侧）', output: '命中 / 未命中', cache: '命中 → 跳㉕ 绿色快速通道' },
  { no: 12, icon: '⑫', phase: 'P2', lane: 'cache', title: '查 L2 Redis', type: 'cache', branch: false, desc: '跨进程共享，TTL 对齐策略版本', output: '命中 / 未命中', cache: '命中 → 跳㉕' },
  { no: 13, icon: '⑬', phase: 'P2', lane: 'cache', title: '调 PDP', type: 'normal', branch: false, desc: 'SingleFlight 合并并发，HMAC 签名请求', output: 'PDP 调用' },
  { no: 14, icon: '⑭', phase: 'P2', lane: 'cache', title: '降级分支', type: 'deny', branch: true, desc: 'PDP 不可达 → 熔断器 CLOSED→OPEN（状态机）→ 缓存 PERMIT 立即降级 DENY', output: '拒绝（fail-closed）' },
  { no: 15, icon: '⑮', phase: 'P3', lane: 'pdp', title: '查 Plan 缓存', type: 'cache', branch: false, desc: '五元键 = 产物键 − dialect（C19）', output: 'Plan / 未命中', cache: '读③；命中 → 跳㉒' },
  { no: 16, icon: '⑯', phase: 'P3', lane: 'pdp', title: '策略匹配', type: 'normal', branch: false, desc: 'subject∧resource∧action∧env∧period；读 L3 策略索引（resourceId+action 选择性最高先过滤）', output: 'matchedPolicies' },
  { no: 17, icon: '⑰', phase: 'P3', lane: 'pdp', title: '无匹配 → DENY', type: 'deny', branch: true, desc: '红支路：直接生成 DENY 产物（fail-closed）', output: 'DENY' },
  { no: 18, icon: '⑱', phase: 'P3', lane: 'pdp', title: 'Effect 裁决', type: 'normal', branch: false, desc: 'DENY-overrides；priority 不参与裁决（仅选 denyReason）', output: 'decision' },
  { no: 19, icon: '⑲', phase: 'P3', lane: 'pdp', title: '短路检查', type: 'tag', branch: false, desc: '表级 HIDDEN / 动作权限 / 标签门槛（调 getUserTagLevels）', output: '门槛结果' },
  { no: 20, icon: '⑳', phase: 'P3', lane: 'pdp', title: '约束合并', type: 'normal', branch: false, desc: '行谓词 AND、列交集 − exclude、掩码最严——复用编译缓存①的 AST 子树', output: 'mergedRowExpr（AST）', cache: '读①' },
  { no: 21, icon: '㉑', phase: 'P3', lane: 'pdp', title: '义务合并', type: 'normal', branch: false, desc: 'AUDIT 取最强、RATE_LIMIT 取 min', output: 'Plan ★', cache: '写 Plan 缓存③' },
  { no: 22, icon: '㉒', phase: 'P4', lane: 'pdp', title: '方言翻译', type: 'compile', branch: false, desc: '查编译缓存② → 未命中则 DialectRenderer（TAG_REF 调标签服务）', output: '方言片段', cache: '读/写编译缓存②' },
  { no: 23, icon: '㉓', phase: 'P4', lane: 'pdp', title: '两线交汇组装', type: 'compile', branch: false, desc: 'Plan（判定线）+ 方言片段（编译线）→ CachedArtifact 模板（placeholders 无值）', output: '产物模板' },
  { no: 24, icon: '㉔', phase: 'P4', lane: 'pdp', title: '签名回传', type: 'normal', branch: false, desc: 'HMAC 签名', output: '签名产物', cache: '写 L2 → 回填 L1' },
  { no: 25, icon: '㉕', phase: 'P5', lane: 'pep', title: '验签 + 版本校验', type: 'exec', branch: false, desc: '签名与 schema 版本校验（失败拒绝）', output: '校验通过' },
  { no: 26, icon: '㉖', phase: 'P5', lane: 'pep', title: 'DENY 检查', type: 'deny', branch: true, desc: 'decision=DENY → 抛权限异常（红支路）', output: '拒绝' },
  { no: 27, icon: '㉗', phase: 'P5', lane: 'pep', title: '解析原始 SQL', type: 'exec', branch: false, desc: 'BoundSql.getSql() 直作缓存键；JSqlParser 深拷贝', output: 'SQL AST' },
  { no: 28, icon: '㉘', phase: 'P5', lane: 'pep', title: 'v1 范围检查', type: 'exec', branch: false, desc: 'JOIN / 子查询 / 聚合 → UnsupportedSqlException（权限谓词豁免）', output: '通过' },
  { no: 29, icon: '㉙', phase: 'P5', lane: 'pep', title: '绑定值', type: 'exec', branch: false, desc: '按 placeholders.ref 取值（A 类 ? 绑定 / B 类安全字面量转义）；值在此处才出现', output: 'BoundArtifact' },
  { no: 30, icon: '㉚', phase: 'P5', lane: 'pep', title: '谓词注入与改写', type: 'exec', branch: false, desc: '谓词注入 WHERE 之后 + SELECT * 展开裁剪 + 掩码表达式替换', output: '改写后 SQL' },
  { no: 31, icon: '㉛', phase: 'P5', lane: 'pep', title: '安全检查', type: 'exec', branch: false, desc: '掩码列被 WHERE/GROUP BY/ORDER BY/JOIN ON 引用 → 拒绝；residual 过滤+LIMIT → 拒绝', output: '通过' },
  { no: 32, icon: '㉜', phase: 'P5', lane: 'ext', title: '引擎执行', type: 'exec', branch: false, desc: '权限谓词走索引，不拉回内存', output: '结果集' },
  { no: 33, icon: '㉝', phase: 'P6', lane: 'pep', title: 'residual 内存过滤', type: 'exec', branch: false, desc: 'residual AST 求值器内存过滤/掩码（上限 10000，超限拒绝）', output: '过滤后结果' },
  { no: 34, icon: '㉞', phase: 'P6', lane: 'pep', title: 'RATE_LIMIT 检查', type: 'exec', branch: false, desc: '行数超限告警/拒绝', output: '通过' },
  { no: 35, icon: '㉟', phase: 'P6', lane: 'ext', title: '审计入队', type: 'audit', branch: false, desc: 'sync=交付前入队确认（微秒级）→ 批量 Kafka → 汇聚', output: '审计事件' },
  { no: 36, icon: '㊱', phase: 'P6', lane: 'pep', title: '结果返回业务 ★', type: 'normal', branch: false, desc: '已过滤/脱敏的合法数据（或拒绝异常）', output: '业务结果' },
  { no: 37, icon: '㊲', phase: 'P7', lane: 'mgmt', title: '新版本发布', type: 'compile', branch: false, desc: '版本号 +1', output: '版本推进' },
  { no: 38, icon: '㊳', phase: 'P7', lane: 'mgmt', title: '失效广播', type: 'compile', branch: false, desc: 'L1/L2 + 编译缓存①②③ 全部失效；Renderer 更新只失效②和 L1/L2，Plan 不受影响', output: '缓存失效' }
]

// 两条演示路径
export const paths = {
  cold: { label: '冷链路（缓存全未命中）', desc: '38 步全流程：定义 → 拦截 → PDP 判定 → 编译 → 改写 → 返回', ids: steps.map(s => s.no) },
  fast: { label: '绿色快速通道（L1 命中）', desc: '⑦→⑪ 命中后直接跳㉕，跳过 PDP 与编译，P2→P5 直达', ids: [7, 8, 9, 10, 11, 25, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36] }
}

export const typeMeta = {
  normal: { label: '判定线', color: '#2563eb', bg: '#eff6ff' },
  compile: { label: '编译线', color: '#b45309', bg: '#fef3c7' },
  cache: { label: '缓存', color: '#15803d', bg: '#dcfce7' },
  deny: { label: '拒绝/降级', color: '#b91c1c', bg: '#fee2e2' },
  tag: { label: '标签服务', color: '#6d28d9', bg: '#ede9fe' },
  exec: { label: '执行线', color: '#0369a1', bg: '#e0f2fe' },
  audit: { label: '审计', color: '#c2410c', bg: '#ffedd5' }
}
