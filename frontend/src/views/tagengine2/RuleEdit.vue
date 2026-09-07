<template>
  <div class="page-card">
    <el-card shadow="never">
      <div class="pg-title">{{ isEdit ? '编辑规则' : '新建规则' }}</div>
      <el-text size="small" type="info">规则 = 一个<b>完整</b>判定定义(WHEN 命中 → THEN 打标), 三种定义方式任选(参考 Drools 规则编辑器): <b>可视化</b>条件组 / <b>决策表</b>档位 / <b>DRL</b> 源码。保存后进入「规则管理」, 可被规则流编排、被任务绑定。</el-text>

      <el-form label-width="86px" style="max-width:760px;margin-top:12px">
        <el-form-item label="规则名称">
          <el-input v-model="f.name" style="width:320px" placeholder="如: 金额档 / 黑名单命中" />
        </el-form-item>
        <el-form-item label="说明">
          <el-input v-model="f.desc" style="width:560px" placeholder="一句话说明(可选)" />
        </el-form-item>
        <el-form-item label="保存为">
          <el-radio-group v-model="mode">
            <el-radio-button value="visual">可视化定义</el-radio-button>
            <el-radio-button value="table">决策表定义</el-radio-button>
            <el-radio-button value="drl">DRL 定义(高级)</el-radio-button>
          </el-radio-group>
          <el-text size="small" type="info" style="margin-left:8px">{{ modeHint }}</el-text>
        </el-form-item>
      </el-form>

      <el-tabs v-model="tab">
        <!-- ═══ 可视化定义 ═══ -->
        <el-tab-pane label="可视化定义(WHEN→THEN)" name="visual">
          <div class="sec-title">WHEN 条件组(引用 数据集字段 / 派生字段)</div>
          <div v-for="(g, gi) in f.def.visual.when" :key="'g' + gi" class="grp">
            <div class="flex" style="gap:6px;align-items:center;margin-bottom:4px">
              <el-tag size="small" type="info" effect="plain">组 {{ gi + 1 }}</el-tag>
              <el-select v-model="g.logic" size="small" style="width:70px"><el-option value="AND" label="且" /><el-option value="OR" label="或" /></el-select>
              <el-text size="small" type="info">组间: 全部满足</el-text>
              <el-button size="small" text type="danger" @click="f.def.visual.when.splice(gi, 1)">删组</el-button>
            </div>
            <div v-for="(c, ci) in g.conds" :key="'c' + gi + ci" class="flex" style="gap:6px;margin:4px 0;flex-wrap:wrap;align-items:center">
              <el-select v-model="c.field" size="small" style="width:170px"><el-option v-for="x in fieldPool" :key="x" :value="x" :label="x" /></el-select>
              <el-select v-model="c.op" size="small" style="width:88px"><el-option v-for="o in opers" :key="o.v" :value="o.v" :label="o.l" /></el-select>
              <el-input v-model="c.val" size="small" style="width:130px" placeholder="值" />
              <el-button size="small" text type="danger" @click="g.conds.splice(ci, 1)">✕</el-button>
            </div>
            <el-button size="small" text type="primary" @click="g.conds.push({ field: '', op: '==', val: '' })">＋ 条件</el-button>
          </div>
          <el-button size="small" text type="success" @click="f.def.visual.when.push({ logic: 'AND', conds: [{ field: '', op: '==', val: '' }] })">＋ 条件组</el-button>

          <div class="sec-title">THEN 动作(命中后执行, 可多条)</div>
          <div v-for="(a, ai) in f.def.visual.actions" :key="'a' + ai" class="flex act-row" style="gap:6px;margin:4px 0;flex-wrap:wrap;align-items:center">
            <el-select v-model="a.type" size="small" style="width:110px">
              <el-option value="tag" label="打标签" />
              <el-option value="stop" label="停止(不再匹配)" />
            </el-select>
            <template v-if="a.type === 'tag'">
              <el-select v-model="a.tag" size="small" filterable style="width:200px" placeholder="标签(注册表)">
                <el-option v-for="t in tagOptions" :key="t" :value="t" :label="t" />
              </el-select>
              <template v-if="!metaBool(a.tag)">
                <el-text size="small" type="info">值</el-text>
                <el-input v-model="a.val" size="small" style="width:120px" placeholder="如 大额" />
              </template>
              <el-text v-else size="small" type="info">(布尔标签: 命中即打)</el-text>
              <el-text v-if="badActVal(a)" size="small" type="danger">⚠ 不在注册域: {{ domainOf(a.tag).join('/') }}</el-text>
            </template>
            <el-text v-else size="small" type="warning">置于规则流内 = 终止整链; 平铺 = 后续规则不再匹配</el-text>
            <el-button size="small" text type="danger" @click="f.def.visual.actions.splice(ai, 1)">✕</el-button>
          </div>
          <el-button size="small" text type="primary" @click="f.def.visual.actions.push({ type: 'tag', tag: '', val: '' })">＋ 动作</el-button>
        </el-tab-pane>

        <!-- ═══ 决策表定义 ═══ -->
        <el-tab-pane label="决策表定义(分档/映射)" name="table">
          <el-alert type="info" :closable="false" style="margin-bottom:10px"
            title="决策表 = 一张档位/映射表(同 Drools 决策表): 每一行一个档位, 命中即得对应值, 同行只中一档(引擎编译为排他规则); 未命中走兜底。金额档: 一行 ≥10000→大额, 一行 ≥1000→中额, 兜底 小额。" />
          <div class="flex" style="gap:8px;align-items:center;margin-bottom:8px;flex-wrap:wrap">
            <el-text size="small" type="info">分档字段(可引用派生字段)</el-text>
            <el-select v-model="f.def.table.field" size="small" style="width:160px"><el-option v-for="x in fieldPool" :key="x" :value="x" :label="x" /></el-select>
            <el-text size="small" type="info">标签</el-text>
            <el-select v-model="f.def.table.tag" size="small" style="width:190px"><el-option v-for="t in tagOptions" :key="t" :value="t" :label="t" /></el-select>
            <el-text size="small" type="info">(档值须在标签取值域内)</el-text>
          </div>
          <el-table :data="f.def.table.rows" size="small" border>
            <el-table-column label="档" type="index" width="46" />
            <el-table-column label="条件(阈值)">
              <template #default="{ row }">
                <div class="flex" style="gap:6px;align-items:center">
                  <el-select v-model="row.op" size="small" style="width:64px"><el-option value=">=" label="≥" /><el-option value=">" label=">" /><el-option value="<" label="<" /><el-option value="==" label="=" /><el-option value="<=" label="≤" /></el-select>
                  <el-input v-model="row.th" size="small" style="width:110px" placeholder="阈值" />
                </div>
              </template>
            </el-table-column>
            <el-table-column label="命中 → 档值">
              <template #default="{ row }">
                <div class="flex" style="gap:4px;align-items:center">
                  <el-input v-model="row.val" size="small" style="width:130px" placeholder="如 大额" />
                  <el-text v-if="badTableVal(row.val)" size="small" type="danger">域外</el-text>
                </div>
              </template>
            </el-table-column>
            <el-table-column width="56"><template #default="{ $index }"><el-button size="small" text type="danger" @click="f.def.table.rows.splice($index, 1)">✕</el-button></template></el-table-column>
          </el-table>
          <div class="flex" style="gap:10px;margin-top:8px;align-items:center;flex-wrap:wrap">
            <el-button size="small" text type="primary" @click="f.def.table.rows.push({ op: '>=', th: '', val: '' })">＋ 档位行</el-button>
            <el-text size="small" type="info">兜底档值(都不满足时; 留空=不打)</el-text>
            <el-input v-model="f.def.table.elseVal" size="small" style="width:120px" placeholder="如 小额" />
            <el-button size="small" plain @click="fillAmtDemo">填入金额档示例</el-button>
            <el-text v-if="f.def.table.elseVal && badTableVal(f.def.table.elseVal)" size="small" type="danger">兜底值域外</el-text>
          </div>
        </el-tab-pane>

        <!-- ═══ DRL 定义 ═══ -->
        <el-tab-pane label="DRL 定义(Drools 源码, 高级)" name="drl">
          <el-alert type="info" :closable="false" style="margin-bottom:8px"
            title="直接编写 Drools 规则语言; 引擎以 DRL 为准执行(手写与可视化互斥, 保存方式选 DRL 即执行此源码)。可先用「从可视化/决策表生成」打底再改。" />
          <div class="flex" style="gap:8px;margin-bottom:6px">
            <el-button size="small" plain @click="genDrlFromOthers">⇩ 从可视化/决策表生成源码打底</el-button>
            <el-text size="small" type="info">Fact 代表一行数据(含派生字段); 动作 tag($f, 标签, 值?)</el-text>
          </div>
          <el-input v-model="f.def.drl" type="textarea" :rows="12" spellcheck="false" class="drl-input"
            placeholder='rule "规则名"\n  when\n    $f : Fact(字段 条件 值)\n  then\n    tag($f, "标签", "值");\nend' />
        </el-tab-pane>
      </el-tabs>

      <div class="dep-bar" style="margin-top:12px">
        <el-text size="small"><b>字段依赖</b>: {{ depsNote }} —— 绑定时由任务的数据集字段/派生字段满足(DRL 依赖无法自动收集, 需人工保证)</el-text>
      </div>
      <div class="dep-bar" style="margin-top:6px;background:#fefaf3;border-color:#f5e3c3">
        <el-text size="small"><b>规则摘要</b>: {{ summary }}</el-text>
      </div>

      <div class="nav-bar">
        <el-button @click="$router.back()">取消</el-button>
        <el-button @click="doTest">▶ 试跑(样本模拟)</el-button>
        <el-button type="primary" :disabled="!okToSave" @click="save">保存规则</el-button>
        <el-text v-if="isEdit" size="small" type="info" style="margin-left:auto">保存将升版本 v{{ f.ver }} → v{{ f.ver + 1 }}</el-text>
      </div>
    </el-card>
  </div>
</template>

<script setup>
import { ref, computed } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { ElMessage } from 'element-plus'
import { tagOptions, metaBool, domainOf, upsertAsset, assetOf, sumRule, depNamesOf } from './mockLib.js'

const route = useRoute()
const router = useRouter()
const toast = (m) => ElMessage.info(m)
const isEdit = computed(() => !!route.query.id)
const src = isEdit.value ? assetOf(route.query.id) : null
const defDefault = (mode) => ({
  mode, table: { field: 'amount', tag: '订单/金额档', rows: [{ op: '>=', th: '', val: '' }], elseVal: '' },
  visual: { when: [{ logic: 'AND', conds: [{ field: '', op: '==', val: '' }] }], actions: [{ type: 'tag', tag: '', val: '' }] },
  drl: ''
})
const f = ref(src ? JSON.parse(JSON.stringify(src)) : { id: undefined, name: '', desc: '', kind: 'rule', status: 1, def: defDefault(route.query.mode || 'visual') })
const tab = ref(f.value.def.mode || 'visual')
const mode = computed({
  get: () => f.value.def.mode || 'visual',
  set: (v) => { f.value.def.mode = v }
})
const modeHint = computed(() => ({
  visual: '条件组可视化搭建, 直观可校验',
  table: '档位/映射表, 一条规则=整套分档, 适合金额档这类',
  drl: 'Drools 源码, 表达力最强, 适合复杂/高级规则'
}[mode.value]))

const fieldPool = ['amount', 'pay_amount', 'coupon_amount', 'quantity', 'region', 'status', 'order_date', 'customer_id', 'pay7d', 'net_amt', 'region_lv']
const opers = [
  { v: '==', l: '=' }, { v: '!=', l: '≠' }, { v: '>=', l: '≥' }, { v: '<=', l: '≤' },
  { v: '>', l: '>' }, { v: '<', l: '<' }, { v: 'in', l: '∈' }, { v: 'like', l: '含' }, { v: 'null', l: '为空' }
]
const badActVal = (a) => {
  const dom = domainOf(a.tag)
  return !!(dom.length && a.val && !dom.includes(a.val))
}
const badTableVal = (v) => {
  if (!v || !f.value.def.table.tag) return false
  const dom = domainOf(f.value.def.table.tag)
  return !!(dom.length && !dom.includes(v))
}
const depsNote = computed(() => {
  if (mode.value === 'drl') return 'DRL 手写: 自动收集不可用, 请人工核对引用的字段/派生字段名'
  const names = depNamesOf({ kind: 'rule', def: f.value.def, id: 'tmp' })
  return names.length ? names.join('、') : '(恒真, 无依赖)'
})
const summary = computed(() => sumRule({ kind: 'rule', def: f.value.def, id: 'tmp', name: f.value.name || '?' }))
const okToSave = computed(() => {
  if (!f.value.name.trim()) return false
  const d = f.value.def
  if (mode.value === 'drl') return !!(d.drl || '').trim()
  if (mode.value === 'table') {
    const t = d.table
    return !!(t.field && t.tag && (t.rows.some(r => r.th !== '' && r.val) || t.elseVal))
  }
  const groups = d.visual.when || []
  const hasCond = !groups.length || groups.some(g => (g.conds || []).some(c => c.field))
  const hasAct = (d.visual.actions || []).some(a => a.type === 'tag' ? !!a.tag : true)
  return hasCond && hasAct
})
function fillAmtDemo() {
  f.value.def.table = { field: 'amount', tag: '订单/金额档', rows: [{ op: '>=', th: '10000', val: '大额' }, { op: '>=', th: '1000', val: '中额' }], elseVal: '小额' }
}
function genDrlFromOthers() {
  if (mode.value === 'table') {
    const t = f.value.def.table
    const lines = ['// 由决策表生成(Drools): 同档排他 = activation-group + salience']
    ;(t.rows || []).forEach((r, i) => {
      lines.push('rule "' + (f.value.name || '档') + '-档' + (i + 1) + '"')
      lines.push('  salience ' + (100 - i * 10))
      lines.push('  activation-group "' + t.tag + '"')
      lines.push('  when')
      lines.push('    $f : Fact(' + t.field + ' ' + r.op + ' ' + r.th + ')')
      lines.push('  then')
      lines.push('    tag($f, "' + t.tag + '", "' + r.val + '");')
      lines.push('end')
    })
    if (t.elseVal) {
      lines.push('rule "' + (f.value.name || '档') + '-兜底"')
      lines.push('  salience 5')
      lines.push('  activation-group "' + t.tag + '"')
      lines.push('  when')
      lines.push('    $f : Fact()')
      lines.push('  then')
      lines.push('    tag($f, "' + t.tag + '", "' + t.elseVal + '");')
      lines.push('end')
    }
    f.value.def.drl = lines.join('\n')
  } else {
    const v = f.value.def.visual
    const conds = (v.when || []).map(g => (g.conds || []).map(c => (c.field || '?') + ' ' + c.op + ' "' + (c.val || '') + '"').join(' && ')).join(' || ')
    const thens = (v.actions || []).map(a => a.type === 'stop' ? 'halt()' : 'tag($f, "' + a.tag + '"' + (a.val !== undefined && a.val !== '' && !metaBool(a.tag) ? ', "' + a.val + '"' : '') + ')').join('; ')
    f.value.def.drl = 'rule "' + (f.value.name || '未命名') + '"\n  when\n    $f : Fact(' + (conds || 'true') + ')\n  then\n    ' + thens + '\nend'
  }
  toast('已生成 DRL 源码(可继续手改)')
}
function doTest() {
  if (!okToSave.value) { toast('先补全规则内容'); return }
  ElMessage.success('试跑(mock): 样本命中 3,842 行 · ' + summary.value)
}
function save() {
  const u = upsertAsset(JSON.parse(JSON.stringify(f.value)))
  toast('规则已保存: ' + u.name + ' v' + u.ver + (isEdit.value ? '(升版)' : '') + ' —— ' + ({ visual: '可视化', table: '决策表', drl: 'DRL' }[u.def.mode]) + '定义')
  router.back()
}
</script>

<style scoped>
.page-card { max-width: 1180px; }
.pg-title { font-size: 17px; font-weight: 700; margin-bottom: 4px; }
.sec-title { font-weight: 600; font-size: 14px; color: #2c3e50; margin: 12px 0 8px; }
.flex { display: flex; }
.grp { border: 1px dashed #c2d9c2; background: #f6fdf6; border-radius: 6px; padding: 8px 10px; margin: 6px 0; }
.act-row { background: #fef7f7; border-radius: 6px; padding: 8px 10px; }
.dep-bar { background: #f4f8ff; border: 1px solid #d6e4ff; border-radius: 6px; padding: 8px 10px; }
.drl-input :deep(textarea) { font-family: Menlo, Consolas, monospace; font-size: 12px; line-height: 1.7; background: #0d1b2a; color: #9fdcb0; }
.nav-bar { display: flex; gap: 10px; margin-top: 18px; align-items: center; }
</style>
