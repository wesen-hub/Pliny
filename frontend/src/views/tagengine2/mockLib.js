// 打标引擎 V2(mock 共享库, 未接后端)
// 规则库 = 两块管理:
//   规则管理   kind='rule'  一条「完整规则」= 三种定义方式之一(三视图同源, 用户任选):
//              def.mode='visual' 可视化: WHEN 条件组(组内且/或) → THEN 动作(打标签[值|布尔]/停止)
//              def.mode='table'  决策表: 字段分档 → 档值(行=档位+兜底), 引擎编译为排他规则
//              def.mode='drl'    DRL: 直接写 Drools 源码(高级, 与可视化互斥)
//   规则流管理 kind='flow'  编排: 步骤引用「规则管理」的规则或其它规则流(嵌套), 命中/未命中分支
// 任务绑定: 规则(A, 先跑) → 规则流(B, 后跑)。模块级单例, 刷新复位。
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

/* ---------- 资产 ---------- */
let seq = 200
const nid = (k) => (k === 'flow' ? 'f' : 'r') + ++seq

export const ruleState = reactive({
  rules: [
    // ===== 规则管理 =====
    { // 决策表定义: 一条「金额档」= 档位表(排他), 无需拆多条
      id: 'r1', name: '金额档', ver: 1, desc: '单笔金额分档: ≥1万 大额 / ≥1千 中额 / 兜底 小额(决策表定义)',
      kind: 'rule', status: 1,
      def: { mode: 'table', table: { field: 'amount', tag: '订单/金额档', rows: [{ op: '>=', th: '10000', val: '大额' }, { op: '>=', th: '1000', val: '中额' }], elseVal: '小额' } }
    },
    { // 可视化定义
      id: 'r2', name: '黑名单命中', ver: 1, desc: 'status=black → 打 风控/黑名单(可视化定义)',
      kind: 'rule', status: 1,
      def: { mode: 'visual', visual: {
        when: [{ logic: 'AND', conds: [{ field: 'status', op: '==', val: 'black' }] }],
        actions: [{ type: 'tag', tag: '风控/黑名单', val: '' }]
      } }
    },
    {
      id: 'r3', name: '华东归属', ver: 1, desc: 'region=华东 → 打 区域/华东(可视化定义)',
      kind: 'rule', status: 1,
      def: { mode: 'visual', visual: {
        when: [{ logic: 'AND', conds: [{ field: 'region', op: '==', val: '华东' }] }],
        actions: [{ type: 'tag', tag: '区域/华东', val: '' }]
      } }
    },
    { // DRL 定义示例
      id: 'r4', name: '净支付超限(DRL 示例)', ver: 1, desc: 'DRL 手写: 净支付额>5万且非黑名单 → 打净支付额=超限(引用派生 net_amt, 需任务提供)',
      kind: 'rule', status: 1,
      def: { mode: 'drl', drl: 'rule "净支付超限"\n  when\n    $f : Fact(net_amt > 50000 && status != "black")\n  then\n    tag($f, "订单/净支付额", "超限");\nend' }
    },
    // ===== 规则流管理 =====
    {
      id: 'f1', name: '风控优先决策链', ver: 1, desc: '黑名单命中即整链终止, 否则进金额档(决策表规则)',
      kind: 'flow', status: 1,
      steps: [
        { refType: 'rule', refId: 'r2', hit: 'stop', hitJump: 0, miss: 'next', missJump: 0 },
        { refType: 'rule', refId: 'r1', hit: 'next', hitJump: 0, miss: 'end', missJump: 0 }
      ]
    },
    {
      id: 'f2', name: '高价值运营链(嵌套示例)', ver: 1, desc: '先看华东归属, 再整体跑「风控优先决策链」—— 规则流引用规则流',
      kind: 'flow', status: 1,
      steps: [
        { refType: 'rule', refId: 'r3', hit: 'next', hitJump: 0, miss: 'end', missJump: 0 },
        { refType: 'flow', refId: 'f1', hit: 'next', hitJump: 0, miss: 'next', missJump: 0 }
      ]
    }
  ]
})

/* ---------- 辅助 ---------- */
export const assetOf = (id) => ruleState.rules.find(x => x.id === id)
export const ruleOf = assetOf
export const defLabel = (r) => (r.kind === 'flow' ? '规则流' : ({ visual: '可视化', table: '决策表', drl: 'DRL' }[r.def.mode] || '?'))
export const defTagType = (r) => (r.kind === 'flow' ? 'warning' : r.def.mode === 'drl' ? 'danger' : r.def.mode === 'table' ? 'primary' : 'success')

const condText = (when) => {
  if (!when || !when.length) return '恒真'
  return when.map(g => (g.conds || []).map(c => (c.field || '?') + ' ' + c.op + ' ' + (c.val || '')).join(' 且 ')).join('  或 ')
}
const actText = (a) => (a.type === 'stop' ? '停止' : '打' + a.tag + (metaBool(a.tag) || a.val === '' ? '' : '=' + a.val))
/** 资产摘要(任意形态; 流不外扩递归, 只列节点名) */
export const sumRule = (r) => {
  if (!r) return '(已删除)'
  if (r.kind === 'flow') {
    const names = (r.steps || []).map(s => {
      const t = assetOf(s.refId)
      return (t ? (t.kind === 'flow' ? '⤷' : '') + t.name : '?' + s.refId) + (s.hit === 'stop' ? '⏹' : s.hit === 'jump' ? '⤵#' + s.hitJump : '')
    }).join(' → ')
    return '规则流(' + (r.steps || []).length + '步): ' + names
  }
  const d = r.def || {}
  if (d.mode === 'table') {
    const t = d.table || {}
    return '决策表→' + t.tag + ': ' + (t.rows || []).map(x => t.field + ' ' + x.op + ' ' + x.th + '→' + x.val).join('; ') + (t.elseVal ? '; 兜底→' + t.elseVal : '')
  }
  if (d.mode === 'drl') {
    const n = (d.drl || '').split('\n').filter(l => l.trim()).length
    return 'DRL(' + n + '行): ' + ((d.drl || '').split('\n')[0] || '')
  }
  const v = d.visual || {}
  return condText(v.when) + ' → ' + (v.actions || []).map(actText).join('; ')
}
export const stepText = (st) => {
  const t = assetOf(st.refId)
  const n = t ? (t.kind === 'flow' ? '⤷' : '') + t.name : '?' + st.refId
  const hit = st.hit === 'jump' ? '跳#' + st.hitJump : st.hit === 'stop' ? '终止' : '下一步'
  const miss = st.miss === 'jump' ? '跳#' + st.missJump : st.miss === 'end' ? '结束' : '下一步'
  return n + ' [命中→' + hit + ' / 未中→' + miss + ']'
}
/** 字段依赖(可视化=条件字段; 决策表=分档字段; DRL=手写, 无法自动收集 → 空并提示人工; 流=递归) */
export function depNamesOf(r, seen) {
  const s = new Set()
  const walk = (x, seen2) => {
    if (!x || seen2.has(x.id)) return
    seen2.add(x.id)
    if (x.kind === 'flow') (x.steps || []).forEach(st => walk(assetOf(st.refId), seen2))
    else {
      const d = x.def || {}
      if (d.mode === 'visual') (d.visual?.when || []).forEach(g => (g.conds || []).forEach(c => { if (c.field) s.add(c.field) }))
      else if (d.mode === 'table') { if (d.table?.field) s.add(d.table.field) }
      /* drl: 人工保证, 不收集 */
    }
  }
  walk(r, seen || new Set())
  return [...s]
}
export const usedByFlows = (id) => ruleState.rules.filter(x => x.kind === 'flow' && (x.steps || []).some(s => s.refId === id)).map(x => x.name)
/** 编排保存前环检测(仅流可被引用成环) */
export function cycleCheck(flowRule) {
  const selfId = flowRule.id
  const seen = new Set()
  const canReach = (id) => {
    if (!id || seen.has(id)) return false
    seen.add(id)
    if (id === selfId) return true
    const x = assetOf(id)
    if (!x) return false
    if (x.kind === 'flow') return (x.steps || []).some(st => canReach(st.refId))
    return false
  }
  return (flowRule.steps || []).some(st => canReach(st.refId))
}

export function upsertAsset(asset) {
  const i = ruleState.rules.findIndex(x => x.id === asset.id)
  if (i >= 0) { asset.ver = (ruleState.rules[i].ver || 1) + 1; ruleState.rules[i] = asset }
  else { asset.id = nid(asset.kind); asset.ver = 1; ruleState.rules.push(asset) }
  return asset
}
export function removeAsset(id) {
  const usedBy = usedByFlows(id)
  if (usedBy.length) return { ok: false, usedBy }
  const i = ruleState.rules.findIndex(x => x.id === id)
  if (i >= 0) ruleState.rules.splice(i, 1)
  return { ok: true }
}
