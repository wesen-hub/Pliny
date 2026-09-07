<template>
  <div class="page-card">
    <el-card shadow="never">
      <div class="pg-title">{{ isEdit ? '编辑规则单元' : '新建规则单元' }}</div>
      <el-text size="small" type="info">原子规则资产: WHEN 条件命中 → THEN 动作(打标签/停止)。可被打标任务平铺引用, 或被规则流编排; 引用字段即「字段依赖」, 由任务装配满足。</el-text>

      <el-form label-width="86px" style="max-width:720px;margin-top:14px">
        <el-form-item label="单元名称">
          <el-input v-model="f.name" style="width:320px" placeholder="如: 金额-大额" />
        </el-form-item>
        <el-form-item label="说明">
          <el-input v-model="f.desc" style="width:520px" placeholder="一句话说明(可选)" />
        </el-form-item>
        <el-form-item label="优先级">
          <el-input-number v-model="f.salience" :min="0" style="width:120px" />
          <el-text size="small" type="info" style="margin-left:10px">数值越高越先求值(同互斥组内只命中最高档)</el-text>
        </el-form-item>
        <el-form-item label="互斥组">
          <el-input v-model="f.activationGroup" style="width:240px" placeholder="留空=不互斥; 如 订单/金额档" />
          <el-text size="small" type="info" style="margin-left:10px">同组 = 同标签多档位, 命中一条即止; 组内必须有兜底档(恒真)才不漏</el-text>
        </el-form-item>
      </el-form>

      <div class="sec-title">WHEN 条件组(引用 数据集字段 / 派生字段)</div>
      <div v-for="(g, gi) in f.when" :key="'g' + gi" class="grp">
        <div class="flex" style="gap:6px;align-items:center;margin-bottom:4px">
          <el-tag size="small" type="info" effect="plain">组 {{ gi + 1 }}</el-tag>
          <el-select v-model="g.logic" size="small" style="width:70px"><el-option value="AND" label="且" /><el-option value="OR" label="或" /></el-select>
          <el-text size="small" type="info">组间: 全部满足</el-text>
          <el-button size="small" text type="danger" @click="f.when.splice(gi, 1)">删组</el-button>
        </div>
        <div v-for="(c, ci) in g.conds" :key="'c' + gi + ci" class="flex" style="gap:6px;margin:4px 0;flex-wrap:wrap;align-items:center">
          <el-select v-model="c.field" size="small" style="width:170px">
            <el-option v-for="x in fieldPool" :key="x" :value="x" :label="x" />
          </el-select>
          <el-select v-model="c.op" size="small" style="width:88px"><el-option v-for="o in opers" :key="o.v" :value="o.v" :label="o.l" /></el-select>
          <el-input v-model="c.val" size="small" style="width:130px" placeholder="值" />
          <el-button size="small" text type="danger" @click="g.conds.splice(ci, 1)">✕</el-button>
        </div>
        <el-button size="small" text type="primary" @click="g.conds.push({ field: '', op: '==', val: '' })">＋ 条件</el-button>
      </div>
      <el-button size="small" text type="success" @click="f.when.push({ logic: 'AND', conds: [{ field: '', op: '==', val: '' }] })">＋ 条件组</el-button>

      <div class="sec-title" style="margin-top:16px">THEN 动作(命中后执行, 可多条)</div>
      <div v-for="(a, ai) in f.actions" :key="'a' + ai" class="flex act-row" style="gap:6px;margin:4px 0;flex-wrap:wrap;align-items:center">
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
          <el-text v-if="badVal(a)" size="small" type="danger">⚠ 不在注册域: {{ domainOf(a.tag).join('/') }}</el-text>
        </template>
        <el-text v-else size="small" type="warning">置于流内时 = 终止整个流; 平铺执行时 = 本单元后不再跑后续平铺单元</el-text>
        <el-button size="small" text type="danger" @click="f.actions.splice(ai, 1)">✕</el-button>
      </div>
      <el-button size="small" text type="primary" @click="f.actions.push({ type: 'tag', tag: '', val: '' })">＋ 动作</el-button>

      <div class="dep-bar" style="margin-top:16px">
        <el-text size="small"><b>字段依赖(自动收集)</b>: 规则引用下面 {{ deps.length }} 个字段名 —— 绑定时由任务的数据集字段或派生字段满足:</el-text>
        <div style="margin-top:4px">
          <el-tag v-for="d in deps" :key="d" size="small" effect="plain" style="margin-right:6px">{{ d }}</el-tag>
          <el-text v-if="!deps.length" size="small" type="info">恒真单元, 无字段依赖(注意: 恒真需配互斥组兜底语义)</el-text>
        </div>
      </div>

      <div class="nav-bar">
        <el-button @click="$router.back()">取消</el-button>
        <el-button @click="doTest">▶ 试跑(样本模拟)</el-button>
        <el-button type="primary" :disabled="!f.name.trim()" @click="save">保存单元</el-button>
        <el-text v-if="isEdit" size="small" type="info" style="margin-left:auto">保存将升版本 v{{ f.ver }} → v{{ f.ver + 1 }}(引用处跟随最新版)</el-text>
      </div>
    </el-card>
  </div>
</template>

<script setup>
import { ref, computed } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { ElMessage } from 'element-plus'
import { tagOptions, metaBool, domainOf, upsertUnit, unitOf } from './mockLib.js'

const route = useRoute()
const router = useRouter()
const toast = (m) => ElMessage.info(m)
const isEdit = computed(() => !!route.query.id)
const src = isEdit.value ? unitOf(route.query.id) : null
const f = ref(src ? JSON.parse(JSON.stringify(src)) : {
  id: undefined, name: '', desc: '', salience: 10, activationGroup: '', status: 1,
  when: [{ logic: 'AND', conds: [{ field: '', op: '==', val: '' }] }],
  actions: [{ type: 'tag', tag: '', val: '' }]
})
const ver = computed(() => f.value.ver || 1)

const fieldPool = ['amount', 'pay_amount', 'coupon_amount', 'quantity', 'region', 'status', 'order_date', 'customer_id', 'pay7d', 'net_amt', 'region_lv']
const opers = [
  { v: '==', l: '=' }, { v: '!=', l: '≠' }, { v: '>=', l: '≥' }, { v: '<=', l: '≤' },
  { v: '>', l: '>' }, { v: '<', l: '<' }, { v: 'in', l: '∈' }, { v: 'like', l: '含' }, { v: 'null', l: '为空' }
]
const badVal = (a) => {
  const dom = domainOf(a.tag)
  return !!(dom.length && a.val && !dom.includes(a.val))
}
const deps = computed(() => {
  const s = new Set()
  f.value.when.forEach(g => (g.conds || []).forEach(c => { if (c.field) s.add(c.field) }))
  return [...s]
})

function doTest() {
  if (!f.value.name.trim()) { toast('先填单元名称'); return }
  ElMessage.success('试跑(mock): 样本命中 3,842 行 · 动作 ' + f.value.actions.length + ' 条 · 字段依赖 ' + deps.value.length + ' 个 全部可满足')
}
function save() {
  const u = upsertUnit(JSON.parse(JSON.stringify(f.value)))
  toast('单元已保存: ' + u.name + ' v' + u.ver + (isEdit.value ? '(升版)' : ''))
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
.nav-bar { display: flex; gap: 10px; margin-top: 18px; align-items: center; }
</style>
