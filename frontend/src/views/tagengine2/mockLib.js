// 打标引擎 V2(mock 共享库, 未接后端)
// 规则资产统一为「规则」: 定义形态二选一 ——
//   simple: 基础规则(style=cond 单条 WHEN→THEN | style=bucket 分档规则[一条规则内含多档位排他])
//   flow  : 编排规则(规则流: 步骤引用规则库任意规则[基础或编排], 支持嵌套; 命中/未命中分支)
// 任务绑定规则资产(基础先行, 编排后跑)。模块级单例, 页面间共享, 刷新复位。
import { reactive } from 'vue'

/* ---------- 标签注册表(与 Registry.vue 同一套示例) ---------- */
export const tagMeta = {
  '订单/金额档': { bool: false, domain: ['大额', '中额', '小额'] },
  '订单/归属地区': { bool: false, domain: ['华北', '华东', '华南', '西南', '华中', '东北', '西北'] },
  '订单/净支付额': { bool: false, domain: [] },
  '客户分层/高价值': { bool: true, domain: [] },
  '区域/华东': { bool: true, domain: [] },
  '风控/黑名单': { bool: true, domain: [] },
  '行为/高频复购': { bool: true, domain: [] }
}
export const tagOptions = Object.keys(tagMeta)
export const metaBool = (t) => !!(tagMeta[t] || {}).bool
export const domainOf = (t) => (tagMeta[t] || {}).domain || []

/* ---------- 规则资产 ---------- */
let seq = 200
const nid = () => 'r' + ++seq

export const ruleState = reactive({
  rules: [
    // 基础-分档规则: 一条规则表达 金额档(档位排他 + 兜底), 无需先建多条单元
    {
      id: 'r1', name: '金额档', ver: 1, desc: '单笔金额分档: ≥1万 大额 / ≥1千 中额 / 兜底 小额',
      kind: 'simple', style: 'bucket', status: 1,
      bucket: { field: 'amount', tag: '订单/金额档', rows: [{ op: '>=', th: '10000', val: '大额' }, { op: '>=', th: '1000', val: '中额' }], elseVal: '小额' }
    },
    // 基础-单条规则
    {
      id: 'r2', name: '黑名单命中', ver: 1, desc: 'status=black → 打 风控/黑名单(布尔)',
      kind: 'simple', style: 'cond', status: 1,
      when: [{ logic: 'AND', conds: [{ field: 'status', op: '==', val: 'black' }] }],
      actions: [{ type: 'tag', tag: '风控/黑名单', val: '' }]
    },
    {
      id: 'r3', name: '华东归属', ver: 1, desc: 'region=华东 → 打 区域/华东(布尔)',
      kind: 'simple', style: 'cond', status: 1,
      when: [{ logic: 'AND', conds: [{ field: 'region', op: '==', val: '华东' }] }],
      actions: [{ type: 'tag', tag: '区域/华东', val: '' }]
    },
    // 编排规则(规则流): 引用 基础规则 / 其它编排规则, 决策链
    {
      id: 'f1', name: '风控优先决策链', ver: 1, desc: '黑名单命中即整链终止, 否则进金额档分档',
      kind: 'flow', status: 1,
      steps: [
        { ruleId: 'r2', hit: 'stop', hitJump: 0, miss: 'next', missJump: 0 },
        { ruleId: 'r1', hit: 'next', hitJump: 0, miss: 'end', missJump: 0 }
      ]
    },
    {
      id: 'f2', name: '高价值运营链(嵌套示例)', ver: 1, desc: '先看华东归属, 再整体跑「风控优先决策链」—— 规则流引用规则流',
      kind: 'flow', status: 1,
      steps: [
        { ruleId: 'r3', hit: 'next', hitJump: 0, miss: 'end', missJump: 0 },
        { ruleId: 'f1', hit: 'next', hitJump: 0, miss: 'next', missJump: 0 }
      ]
    }
  ]
})

export const ruleOf = (id) => ruleState.rules.find(x => x.id === id)
export const kindLabel = (r) => (r.kind === 'flow' ? '编排流' : (r.style === 'bucket' ? '分档' : '单条'))
export const kindTagType = (r) => (r.kind === 'flow' ? 'warning' : (r.style === 'bucket' ? 'danger' : 'primary'))

const condText = (when) => {
  if (!when || !when.length) return '恒真'
  return when.map(g => (g.conds || []).map(c => (c.field || '?') + ' ' + c.op + ' ' + (c.val || '')).join(' 且 ')).join('  或 ')
}
const actText = (a) => (a.type === 'stop' ? '停止' : '打' + a.tag + (metaBool(a.tag) || a.val === '' ? '' : '=' + a.val))
/** 一条规则的摘要(任意形态, 递归安全: 流只显示步骤数/节点名, 不外扩) */
export const sumRule = (r) => {
  if (!r) return '(已删)'
  if (r.kind === 'flow') {
    const names = (r.steps || []).map(s => {
      const t = ruleOf(s.ruleId)
      return (t ? t.name : '?' + s.ruleId) + (s.hit === 'stop' ? '⏹' : s.hit === 'jump' ? '⤵#' + s.hitJump : '')
    }).join(' → ')
    return '编排流(' + (r.steps || []).length + '步): ' + names
  }
  if (r.style === 'bucket') {
    const b = r.bucket
    const rows = (b.rows || []).map(x => b.field + ' ' + x.op + ' ' + x.th + '→' + x.val).join('; ')
    return '分档→' + b.tag + ': ' + rows + (b.elseVal ? '; 兜底→' + b.elseVal : '')
  }
  return condText(r.when) + ' → ' + (r.actions || []).map(actText).join('; ')
}
export const stepText = (st) => {
  const t = ruleOf(st.ruleId)
  const n = t ? (t.kind === 'flow' ? '⤷' : '') + t.name : '?' + st.ruleId
  const hit = st.hit === 'jump' ? '跳#' + st.hitJump : st.hit === 'stop' ? '终止' : '下一步'
  const miss = st.miss === 'jump' ? '跳#' + st.missJump : st.miss === 'end' ? '结束' : '下一步'
  return n + ' [命中→' + hit + ' / 未中→' + miss + ']'
}
/** 字段依赖(递归, 环安全 seen) */
export function depNamesOf(r, seen) {
  const s = new Set()
  const walk = (x, seen2) => {
    if (!x || seen2.has(x.id)) return
    seen2.add(x.id)
    if (x.kind === 'flow') (x.steps || []).forEach(st => walk(ruleOf(st.ruleId), seen2))
    else if (x.style === 'bucket') { if (x.bucket && x.bucket.field) s.add(x.bucket.field) }
    else (x.when || []).forEach(g => (g.conds || []).forEach(c => { if (c.field) s.add(c.field) }))
  }
  walk(r, seen || new Set())
  return [...s]
}
/** 是否被其它编排规则引用(删除守卫) */
export const usedByFlows = (id) => ruleState.rules.filter(x => x.kind === 'flow' && (x.steps || []).some(s => s.ruleId === id)).map(x => x.name)
/** 保存编排规则前的环检测: 新 steps 集合里若能从某步引用到达规则自身 → 成环 */
export function cycleCheck(flowRule) {
  const selfId = flowRule.id
  const seen = new Set()
  const canReach = (fromId) => {
    if (!fromId || seen.has(fromId)) return false
    seen.add(fromId)
    const x = ruleOf(fromId)
    if (!x) return false
    if (fromId === selfId) return true
    if (x.kind === 'flow') return (x.steps || []).some(st => canReach(st.ruleId))
    return false
  }
  return (flowRule.steps || []).some(st => canReach(st.ruleId))
}

export function upsertRule(rule) {
  const i = ruleState.rules.findIndex(x => x.id === rule.id)
  if (i >= 0) { rule.ver = (ruleState.rules[i].ver || 1) + 1; ruleState.rules[i] = rule }
  else { rule.id = nid(); rule.ver = 1; ruleState.rules.push(rule) }
  return rule
}
export function removeRule(id) {
  const usedBy = usedByFlows(id)
  if (usedBy.length) return { ok: false, usedBy }
  const i = ruleState.rules.findIndex(x => x.id === id)
  if (i >= 0) ruleState.rules.splice(i, 1)
  return { ok: true }
}
