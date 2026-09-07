// 打标引擎 V2(mock 共享库, 未接后端)
// 标签注册表元数据 + 规则资产(规则单元 / 规则流) 两级。模块级单例: 页面间共享增删改, 刷新即复位。
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
// 单元(Unit): 原子 WHEN→THEN, 可被任务平铺引用或被流编排
//   activationGroup: 互斥组名(同组=同标签档位, 命中最高 salience 一条); '' = 不互斥
// 流(Flow): 决策链, step = { unitId, withGroup(连同互斥组), hit: next|stop|jump(+hitJump), miss: next|end|jump(+missJump) }
let seq = 100
const nid = () => 'u' + ++seq

export const ruleState = reactive({
  units: [
    {
      id: 'u1', name: '金额-大额', ver: 1, desc: '单笔金额 ≥ 1 万 → 金额档=大额',
      salience: 30, activationGroup: '订单/金额档', status: 1,
      when: [{ logic: 'AND', conds: [{ field: 'amount', op: '>=', val: '10000' }] }],
      actions: [{ type: 'tag', tag: '订单/金额档', val: '大额' }]
    },
    {
      id: 'u2', name: '金额-中额', ver: 1, desc: '单笔金额 ≥ 1 千 → 金额档=中额',
      salience: 20, activationGroup: '订单/金额档', status: 1,
      when: [{ logic: 'AND', conds: [{ field: 'amount', op: '>=', val: '1000' }] }],
      actions: [{ type: 'tag', tag: '订单/金额档', val: '中额' }]
    },
    {
      id: 'u3', name: '金额-小额(默认档)', ver: 1, desc: '金额档兜底: 恒真 → 小额',
      salience: 5, activationGroup: '订单/金额档', status: 1,
      when: [],
      actions: [{ type: 'tag', tag: '订单/金额档', val: '小额' }]
    },
    {
      id: 'u4', name: '黑名单命中', ver: 1, desc: 'status=black → 打风控/黑名单(布尔)',
      salience: 10, activationGroup: '', status: 1,
      when: [{ logic: 'AND', conds: [{ field: 'status', op: '==', val: 'black' }] }],
      actions: [{ type: 'tag', tag: '风控/黑名单', val: '' }]
    },
    {
      id: 'u5', name: '华东归属', ver: 1, desc: 'region=华东 → 打区域/华东(布尔)',
      salience: 10, activationGroup: '', status: 1,
      when: [{ logic: 'AND', conds: [{ field: 'region', op: '==', val: '华东' }] }],
      actions: [{ type: 'tag', tag: '区域/华东', val: '' }]
    }
  ],
  flows: [
    {
      id: 'f1', name: '风控优先决策链', ver: 1, desc: '先判黑名单(命中整链终止), 未命中再分金额档',
      status: 1,
      steps: [
        { unitId: 'u4', withGroup: false, hit: 'stop', hitJump: 0, miss: 'next', missJump: 0 },
        { unitId: 'u1', withGroup: true, hit: 'next', hitJump: 0, miss: 'end', missJump: 0 }
      ]
    }
  ]
})

export const unitOf = (id) => ruleState.units.find(u => u.id === id)
export const flowOf = (id) => ruleState.flows.find(f => f.id === id)
const actSum = (a) => (a.type === 'stop' ? '停止' : '打' + a.tag + (metaBool(a.tag) || a.val === '' ? '' : '=' + a.val))
export const whenSum = (u) => {
  if (!u.when || !u.when.length) return '恒真(默认档)'
  return u.when.map(g => (g.conds || []).map(c => (c.field || '?') + ' ' + c.op + ' ' + (c.val || '')).join(' 且 ')).join('  或 ')
}
export const thenSum = (u) => (u.actions || []).map(actSum).join('; ')
/** 规则资产声明的字段依赖: WHEN 条件引用的全部字段名(由任务装配的 数据集字段∪派生字段 满足) */
export const depsOf = (u) => {
  const s = new Set()
  ;(u.when || []).forEach(g => (g.conds || []).forEach(c => { if (c.field) s.add(c.field) }))
  return [...s]
}
/** 单元执行摘要 */
export const unitBrief = (u) => u.name + ' · v' + u.ver + ' — ' + whenSum(u) + ' → ' + thenSum(u)
/** 一个节点实际生效的单元集(连同互斥组时 = 共享 activationGroup 的全部同组单元) */
export const nodeUnits = (step) => {
  const u = unitOf(step.unitId)
  if (!u) return []
  if (step.withGroup && u.activationGroup) {
    return ruleState.units.filter(x => x.activationGroup === u.activationGroup && x.status !== 0)
  }
  return [u]
}
export const stepSum = (st, idx) => {
  const u = unitOf(st.unitId)
  const g = st.withGroup && u && u.activationGroup ? '(组:' + u.activationGroup + ')' : ''
  const hit = st.hit === 'jump' ? '跳#' + st.hitJump : st.hit === 'stop' ? '终止' : '下一步'
  const miss = st.miss === 'jump' ? '跳#' + st.missJump : st.miss === 'end' ? '结束' : '下一步'
  return (u ? u.name : '?' + st.unitId) + g + '  [命中→' + hit + ' / 未中→' + miss + ']'
}
export const flowBrief = (f) => (f.steps || []).map((s, i) => (i + 1) + '.' + stepSum(s, i)).join('  →  ')
/** 流内所有节点的字段依赖(供任务校验) */
export const flowDeps = (f) => {
  const s = new Set()
  ;(f.steps || []).forEach(st => nodeUnits(st).forEach(u => depsOf(u).forEach(d => s.add(d))))
  return [...s]
}

export function addUnit(u) { u.id = nid(); u.ver = 1; ruleState.units.push(u); return u }
export function upsertUnit(u) {
  const i = ruleState.units.findIndex(x => x.id === u.id)
  if (i >= 0) { u.ver = (ruleState.units[i].ver || 1) + 1; ruleState.units[i] = u } else { u.id = nid(); u.ver = 1; ruleState.units.push(u) }
  return u
}
export function removeUnit(id) {
  const usedBy = ruleState.flows.filter(f => (f.steps || []).some(s => s.unitId === id)).map(f => f.name)
  if (usedBy.length) return { ok: false, usedBy }
  const i = ruleState.units.findIndex(u => u.id === id)
  if (i >= 0) ruleState.units.splice(i, 1)
  return { ok: true }
}
export function upsertFlow(f) {
  const i = ruleState.flows.findIndex(x => x.id === f.id)
  if (i >= 0) { f.ver = (ruleState.flows[i].ver || 1) + 1; ruleState.flows[i] = f } else { f.id = 'f' + ++seq; f.ver = 1; ruleState.flows.push(f) }
  return f
}
export function removeFlow(id) {
  const i = ruleState.flows.findIndex(f => f.id === id)
  if (i >= 0) ruleState.flows.splice(i, 1)
}
