<template>
  <div class="page-card">
    <el-card shadow="never">
      <div class="pg-title">{{ isEdit ? '编辑基础规则' : '新建基础规则' }}</div>
      <el-text size="small" type="info">一条规则 = 一个完整判定。两种写法, 存为同一条规则: 「单条」= 一个 WHEN→THEN 判定; 「分档」= 一张档位表(≥N→档值…+兜底), 引擎按档位排他求值 —— 金额档这类不需要拆成多条。</el-text>

      <el-form label-width="86px" style="max-width:760px;margin-top:12px">
        <el-form-item label="规则名称">
          <el-input v-model="f.name" style="width:320px" placeholder="如: 金额档 / 黑名单命中" />
        </el-form-item>
        <el-form-item label="说明">
          <el-input v-model="f.desc" style="width:560px" placeholder="一句话说明(可选)" />
        </el-form-item>
      </el-form>

      <el-tabs v-model="style" style="margin-top:4px">
        <!-- ═══ 写法一: 分档(档位表) ═══ -->
        <el-tab-pane label="分档规则(多档位, 排他)" name="bucket">
          <div class="flex" style="gap:8px;align-items:center;margin-bottom:8px;flex-wrap:wrap">
            <el-text size="small" type="info">按字段分档 → 打标签(每行 = 一个档位, 命中即得对应档值, 一行只中一档; 可引用派生字段)</el-text>
          </div>
          <div class="flex" style="gap:8px;align-items:center;margin-bottom:8px;flex-wrap:wrap">
            <el-text size="small" type="info">分档字段</el-text>
            <el-select v-model="f.bucket.field" size="small" style="width:160px"><el-option v-for="x in fieldPool" :key="x" :value="x" :label="x" /></el-select>
            <el-text size="small" type="info">标签</el-text>
            <el-select v-model="f.bucket.tag" size="small" style="width:190px"><el-option v-for="t in tagOptions" :key="t" :value="t" :label="t" /></el-select>
            <el-text size="small" type="info">(档值须在标签取值域内, 域外标红)</el-text>
          </div>
          <el-table :data="f.bucket.rows" size="small" border>
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
                  <el-text v-if="badBucketVal(row.val)" size="small" type="danger">域外</el-text>
                </div>
              </template>
            </el-table-column>
            <el-table-column width="56"><template #default="{ $index }"><el-button size="small" text type="danger" @click="f.bucket.rows.splice($index, 1)">✕</el-button></template></el-table-column>
          </el-table>
          <div class="flex" style="gap:10px;margin-top:8px;align-items:center;flex-wrap:wrap">
            <el-button size="small" text type="primary" @click="f.bucket.rows.push({ op: '>=', th: '', val: '' })">＋ 档位</el-button>
            <el-text size="small" type="info">兜底档值(都不满足时, 留空则不打)</el-text>
            <el-input v-model="f.bucket.elseVal" size="small" style="width:120px" placeholder="如 小额" />
            <el-button size="small" plain @click="fillAmtDemo">填入金额档示例</el-button>
            <el-text v-if="f.bucket.elseVal && badBucketVal(f.bucket.elseVal)" size="small" type="danger">兜底值域外</el-text>
          </div>
        </el-tab-pane>

        <!-- ═══ 写法二: 单条 WHEN→THEN ═══ -->
        <el-tab-pane label="单条规则(一个条件 → 动作)" name="cond">
          <div class="sec-title">WHEN 条件组(引用 数据集字段 / 派生字段)</div>
          <div v-for="(g, gi) in f.when" :key="'g' + gi" class="grp">
            <div class="flex" style="gap:6px;align-items:center;margin-bottom:4px">
              <el-tag size="small" type="info" effect="plain">组 {{ gi + 1 }}</el-tag>
              <el-select v-model="g.logic" size="small" style="width:70px"><el-option value="AND" label="且" /><el-option value="OR" label="或" /></el-select>
              <el-text size="small" type="info">组间: 全部满足</el-text>
              <el-button size="small" text type="danger" @click="f.when.splice(gi, 1)">删组</el-button>
            </div>
            <div v-for="(c, ci) in g.conds" :key="'c' + gi + ci" class="flex" style="gap:6px;margin:4px 0;flex-wrap:wrap;align-items:center">
              <el-select v-model="c.field" size="small" style="width:170px"><el-option v-for="x in fieldPool" :key="x" :value="x" :label="x" /></el-select>
              <el-select v-model="c.op" size="small" style="width:88px"><el-option v-for="o in opers" :key="o.v" :value="o.v" :label="o.l" /></el-select>
              <el-input v-model="c.val" size="small" style="width:130px" placeholder="值" />
              <el-button size="small" text type="danger" @click="g.conds.splice(ci, 1)">✕</el-button>
            </div>
            <el-button size="small" text type="primary" @click="g.conds.push({ field: '', op: '==', val: '' })">＋ 条件</el-button>
          </div>
          <el-button size="small" text type="success" @click="f.when.push({ logic: 'AND', conds: [{ field: '', op: '==', val: '' }] })">＋ 条件组</el-button>

          <div class="sec-title">THEN 动作(命中后执行, 可多条)</div>
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
              <el-text v-if="badActVal(a)" size="small" type="danger">⚠ 不在注册域: {{ domainOf(a.tag).join('/') }}</el-text>
            </template>
            <el-button size="small" text type="danger" @click="f.actions.splice(ai, 1)">✕</el-button>
          </div>
          <el-button size="small" text type="primary" @click="f.actions.push({ type: 'tag', tag: '', val: '' })">＋ 动作</el-button>
        </el-tab-pane>
      </el-tabs>

      <div class="dep-bar" style="margin-top:14px">
        <el-text size="small"><b>字段依赖(自动收集)</b>: {{ deps.length ? deps.join('、') : '(恒真, 无依赖)' }} —— 绑定时由任务的数据集字段或派生字段满足</el-text>
      </div>
      <div class="dep-bar" style="margin-top:6px;background:#fefaf3;border-color:#f5e3c3">
        <el-text size="small"><b>规则摘要</b>: {{ sumRule(f) }}</el-text>
      </div>

      <div class="nav-bar">
        <el-button @click="$router.back()">取消</el-button>
        <el-button @click="doTest">▶ 试跑(样本模拟)</el-button>
        <el-button type="primary" :disabled="!okToSave" @click="save">保存规则</el-button>
        <el-text v-if="isEdit" size="small" type="info" style="margin-left:auto">保存将升版本 v{{ f.ver }} → v{{ f.ver + 1 }}(引用/绑定处跟随最新版)</el-text>
      </div>
    </el-card>
  </div>
</template>

<script setup>
import { ref, computed } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { ElMessage } from 'element-plus'
import { tagOptions, metaBool, domainOf, upsertRule, ruleOf, sumRule, depNamesOf } from './mockLib.js'

const route = useRoute()
const router = useRouter()
const toast = (m) => ElMessage.info(m)
const isEdit = computed(() => !!route.query.id)
const src = isEdit.value ? ruleOf(route.query.id) : null
const f = ref(src ? JSON.parse(JSON.stringify(src)) : {
  id: undefined, name: '', desc: '', kind: 'simple', status: 1,
  style: route.query.style === 'bucket' ? 'bucket' : 'cond',
  when: [{ logic: 'AND', conds: [{ field: '', op: '==', val: '' }] }],
  actions: [{ type: 'tag', tag: '', val: '' }],
  bucket: { field: 'amount', tag: '订单/金额档', rows: [{ op: '>=', th: '', val: '' }], elseVal: '' }
})
const style = computed({
  get: () => f.value.style || 'cond',
  set: (v) => { f.value.style = v }
})

const fieldPool = ['amount', 'pay_amount', 'coupon_amount', 'quantity', 'region', 'status', 'order_date', 'customer_id', 'pay7d', 'net_amt', 'region_lv']
const opers = [
  { v: '==', l: '=' }, { v: '!=', l: '≠' }, { v: '>=', l: '≥' }, { v: '<=', l: '≤' },
  { v: '>', l: '>' }, { v: '<', l: '<' }, { v: 'in', l: '∈' }, { v: 'like', l: '含' }, { v: 'null', l: '为空' }
]
const badActVal = (a) => {
  const dom = domainOf(a.tag)
  return !!(dom.length && a.val && !dom.includes(a.val))
}
const badBucketVal = (v) => {
  if (!v || !f.value.bucket.tag) return false
  const dom = domainOf(f.value.bucket.tag)
  return !!(dom.length && !dom.includes(v))
}
const deps = computed(() => depNamesOf({ ...f.value, id: undefined }))
const okToSave = computed(() => {
  if (!f.value.name.trim()) return false
  if (f.value.style === 'bucket') {
    const b = f.value.bucket
    if (!b.field || !b.tag) return false
    return b.rows.some(r => r.th !== '' && r.val) || !!b.elseVal
  }
  const groups = f.value.when || []
  const hasCond = !groups.length || groups.some(g => (g.conds || []).some(c => c.field))
  const hasAct = (f.value.actions || []).some(a => a.type === 'tag' ? a.tag : true)
  return hasCond && hasAct
})
function fillAmtDemo() {
  f.value.bucket = { field: 'amount', tag: '订单/金额档', rows: [{ op: '>=', th: '10000', val: '大额' }, { op: '>=', th: '1000', val: '中额' }], elseVal: '小额' }
}
function doTest() {
  if (!okToSave.value) { toast('先补全规则内容'); return }
  ElMessage.success('试跑(mock): 样本命中 3,842 行 · ' + sumRule(f.value))
}
function save() {
  const u = upsertRule(JSON.parse(JSON.stringify(f.value)))
  toast('规则已保存: ' + u.name + ' v' + u.ver + (isEdit.value ? '(升版)' : '') + ' —— ' + (u.style === 'bucket' ? '分档' : '单条'))
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
