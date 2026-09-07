<template>
  <div class="page-card">
    <el-card shadow="never">
      <div class="pg-title">{{ isEdit ? '编辑规则流(规则流管理)' : '新建规则流(规则流管理)' }}</div>
      <el-text size="small" type="info">规则流 = 把「规则管理」中的规则(或别的规则流)按序编排成更复杂的规则。每步: 引用一条规则/规则流 → 命中/未命中各自分支(下一步 / 终止整链 / 跳转第 N 步)。金额档这类决策表规则直接一步引用; 引用规则流 = 嵌套(保存自动环检测)。</el-text>

      <el-form label-width="96px" style="max-width:720px;margin-top:12px">
        <el-form-item label="规则流名称">
          <el-input v-model="f.name" style="width:320px" placeholder="如: 风控优先决策链" />
        </el-form-item>
        <el-form-item label="说明">
          <el-input v-model="f.descr" style="width:560px" placeholder="一句话说明(可选)" />
        </el-form-item>
      </el-form>

      <div class="sec-title">编排步骤(自上而下; 引用规则/规则流, 被引用对象自带完整判定)</div>
      <el-table :data="f.steps" size="small" border style="margin-bottom:8px">
        <el-table-column label="步" type="index" width="44" />
        <el-table-column label="引用(规则/规则流)" min-width="250">
          <template #default="{ row, $index }">
            <div class="flex" style="gap:6px;align-items:center;flex-wrap:wrap">
              <el-select v-model="row.refId" size="small" style="width:230px" placeholder="从规则库选择" @change="onRef($index)">
                <el-option-group label="规则管理">
                  <el-option v-for="r in ruleOptions" :key="r.id" :value="r.id" :disabled="r.status === 0" :label="(r.status === 0 ? '[停] ' : '') + r.name + ' · v' + r.ver + ' (' + defLabel(r) + ')'" />
                </el-option-group>
                <el-option-group label="规则流管理">
                  <el-option v-for="r in flowOptions" :key="r.id" :value="r.id" :disabled="r.status === 0" :label="(r.status === 0 ? '[停] ' : '') + r.name + ' · v' + r.ver + ' (规则流)'" />
                </el-option-group>
              </el-select>
              <el-button size="small" text type="primary" @click="quickNew($index)">＋ 新建并引用</el-button>
            </div>
            <el-text v-if="assetOf(row.refId)" size="small" type="info">{{ sumRule(assetOf(row.refId)) }}</el-text>
          </template>
        </el-table-column>
        <el-table-column label="命中 →" width="150">
          <template #default="{ row }">
            <div class="flex" style="gap:4px;align-items:center">
              <el-select v-model="row.hit" size="small" style="width:90px">
                <el-option value="next" label="下一步" /><el-option value="stop" label="终止" /><el-option value="jump" label="跳转" />
              </el-select>
              <el-input-number v-if="row.hit === 'jump'" v-model="row.hitJump" size="small" style="width:54px" :min="1" :max="f.steps.length" />
            </div>
          </template>
        </el-table-column>
        <el-table-column label="未命中 →" width="150">
          <template #default="{ row }">
            <div class="flex" style="gap:4px;align-items:center">
              <el-select v-model="row.miss" size="small" style="width:90px">
                <el-option value="next" label="下一步" /><el-option value="end" label="结束" /><el-option value="jump" label="跳转" />
              </el-select>
              <el-input-number v-if="row.miss === 'jump'" v-model="row.missJump" size="small" style="width:54px" :min="1" :max="f.steps.length" />
            </div>
          </template>
        </el-table-column>
        <el-table-column label="依赖(递归收集)" min-width="130">
          <template #default="{ row }">
            <el-tag v-for="d in stepDeps(row)" :key="d" size="small" effect="plain" type="info" style="margin-right:4px">{{ d }}</el-tag>
            <el-text v-if="!stepDeps(row).length" size="small" type="info">{{ drlNote(row) }}</el-text>
          </template>
        </el-table-column>
        <el-table-column label="操作" width="120" fixed="right">
          <template #default="{ $index }">
            <el-button size="small" text :disabled="$index === 0" @click="mv($index, -1)">↑</el-button>
            <el-button size="small" text :disabled="$index === f.steps.length - 1" @click="mv($index, 1)">↓</el-button>
            <el-button size="small" text type="danger" @click="f.steps.splice($index, 1)">删</el-button>
          </template>
        </el-table-column>
      </el-table>
      <div class="flex" style="gap:10px;align-items:center;flex-wrap:wrap">
        <el-button size="small" text type="primary" @click="f.steps.push(newStep())">＋ 步骤</el-button>
        <el-button size="small" plain @click="fillDemo">填入示例(黑名单 → 金额档决策表)</el-button>
        <el-text size="small" type="warning">命中/未命中: 下一步 | 终止(整链) | 跳转第 N 步; 被引规则流「命中」= 其链内至少执行一条动作</el-text>
      </div>

      <div class="chain-preview">
        <el-text size="small"><b>编排结构预览</b></el-text>
        <div style="margin-top:4px;white-space:pre-wrap"><el-text size="small" type="info">{{ preview }}</el-text></div>
      </div>
      <div class="dep-bar">
        <el-text size="small"><b>总字段依赖(递归)</b>: {{ deps.length ? deps.join('、') : '—' }} —— 任务绑定时校验 数据集字段∪派生字段 满足(DRL 规则依赖人工保证)</el-text>
      </div>

      <div class="nav-bar">
        <el-button @click="$router.back()">取消</el-button>
        <el-button @click="doTest">▶ 干跑(样本模拟)</el-button>
        <el-button type="primary" :disabled="!okToSave" @click="save">保存规则流</el-button>
        <el-text v-if="isEdit" size="small" type="info" style="margin-left:auto">保存将升版本 v{{ f.ver }} → v{{ f.ver + 1 }}</el-text>
      </div>
    </el-card>

    <!-- ═══ 快速新建规则并引用 ═══ -->
    <el-dialog v-model="quickOpen" :title="'新建并引用 —— 步骤 ' + (quickIdx + 1)" width="620">
      <el-radio-group v-model="q.mode" size="small" style="margin-bottom:10px">
        <el-radio-button value="visual">可视化(单条件)</el-radio-button>
        <el-radio-button value="table">决策表(档位)</el-radio-button>
      </el-radio-group>
      <el-alert type="info" :closable="false" title="保存进「规则管理」后作为本步骤引用; 完整三视图编辑(含 DRL)在规则管理。" style="margin-bottom:10px" />
      <el-form label-width="88px">
        <el-form-item label="名称"><el-input v-model="q.name" style="width:280px" placeholder="如: 高频复购判定" /></el-form-item>
        <template v-if="q.mode === 'visual'">
          <el-form-item label="WHEN">
            <div class="flex" style="gap:6px;flex-wrap:wrap">
              <el-select v-model="q.field" style="width:150px"><el-option v-for="x in fieldPool" :key="x" :value="x" :label="x" /></el-select>
              <el-select v-model="q.op" style="width:84px"><el-option v-for="o in opers" :key="o.v" :value="o.v" :label="o.l" /></el-select>
              <el-input v-model="q.val" style="width:110px" placeholder="值" />
            </div>
          </el-form-item>
          <el-form-item label="THEN 打标签">
            <div class="flex" style="gap:6px;flex-wrap:wrap">
              <el-select v-model="q.tag" style="width:180px" filterable placeholder="标签(注册表)">
                <el-option v-for="t in tagOptions" :key="t" :value="t" :label="t" />
              </el-select>
              <template v-if="!metaBool(q.tag)">
                <el-input v-model="q.val2" style="width:110px" placeholder="标签值" />
              </template>
              <el-text v-else size="small" type="info">(布尔)</el-text>
            </div>
          </el-form-item>
        </template>
        <template v-else>
          <el-form-item label="决策表">
            <div class="flex" style="gap:6px;flex-wrap:wrap;align-items:center">
              <el-text size="small" type="info">字段</el-text>
              <el-select v-model="q.field" size="small" style="width:130px"><el-option v-for="x in fieldPool" :key="x" :value="x" :label="x" /></el-select>
              <el-text size="small" type="info">标签</el-text>
              <el-select v-model="q.tag" size="small" style="width:160px"><el-option v-for="t in tagOptions" :key="t" :value="t" :label="t" /></el-select>
            </div>
            <div v-for="(row, ri) in q.buckets" :key="ri" class="flex" style="gap:6px;margin-top:6px;flex-wrap:wrap;align-items:center">
              <el-select v-model="row.op" size="small" style="width:70px"><el-option value=">=" label="≥" /><el-option value="<" label="<" /></el-select>
              <el-input v-model="row.th" size="small" style="width:90px" placeholder="阈值" />
              <el-text size="small" type="info">→</el-text>
              <el-input v-model="row.val" size="small" style="width:100px" placeholder="档值" />
              <el-button size="small" text type="danger" @click="q.buckets.splice(ri, 1)">✕</el-button>
            </div>
            <el-button size="small" text type="primary" style="margin-top:6px" @click="q.buckets.push({ op: '>=', th: '', val: '' })">＋ 档位</el-button>
            <div class="flex" style="gap:6px;margin-top:6px;align-items:center">
              <el-text size="small" type="info">兜底</el-text>
              <el-input v-model="q.elseVal" size="small" style="width:100px" placeholder="如 小额" />
            </div>
          </el-form-item>
        </template>
      </el-form>
      <template #footer>
        <el-button @click="quickOpen = false">取消</el-button>
        <el-button type="primary" :disabled="!q.name.trim() || (q.mode === 'visual' && (!q.field || !q.tag))" @click="saveQuick">保存并作为本步骤引用</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { ElMessage } from 'element-plus'
import { rulesApi } from '../../api/tagengine.js'
import { defLabel, sumRule, depNamesOf } from './mockLib.js'
import { metaBool, tagOptions } from './mockLib.js'

const route = useRoute()
const router = useRouter()
const toast = (m, t) => (t ? ElMessage[t](m) : ElMessage.info(m))
const isEdit = computed(() => !!route.query.id)
const f = ref({ id: undefined, name: '', descr: '', kind: 'flow', status: 1, steps: [] })

/* 全量资产(候选 + 步骤引用解析), 来自后端 */
const allAssets = ref([])
const allMap = computed(() => new Map(allAssets.value.map(a => [Number(a.id), a])))
const assetOf = (id) => allMap.value.get(Number(id))
const ruleOptions = computed(() => allAssets.value.filter(r => r.kind === 'rule'))
const flowOptions = computed(() => allAssets.value.filter(r => r.kind === 'flow'))
onMounted(async () => {
  try {
    allAssets.value = await rulesApi.list()
    if (isEdit.value) {
      const row = await rulesApi.get(route.query.id)
      f.value = { id: row.id, name: row.name, descr: row.descr || '', kind: 'flow', status: row.status, ver: row.ver, steps: JSON.parse(JSON.stringify((row.def && row.def.steps) || [])) }
    }
  } catch (e) { /* 已提示 */ }
})

const newStep = () => ({ refId: '', refType: '', hit: 'next', hitJump: 1, miss: 'next', missJump: 1 })
function onRef(i) {
  const a = assetOf(f.value.steps[i].refId)
  f.value.steps[i].refType = a ? a.kind : ''
}
function mv(i, d) {
  const arr = f.value.steps
  const j = i + d
  ;[arr[i], arr[j]] = [arr[j], arr[i]]
}
const stepDeps = (st) => depNamesOf(assetOf(st.refId))
const drlNote = (st) => {
  const a = assetOf(st.refId)
  return a && a.kind === 'rule' && a.def && a.def.mode === 'drl' ? '(DRL 人工)' : ''
}
const deps = computed(() => {
  const s = new Set()
  ;(f.value.steps || []).forEach(st => {
    const a = assetOf(st.refId)
    if (a) depNamesOf(a).forEach(d => s.add(d))
  })
  return [...s]
})
const preview = computed(() => {
  const br = (v, j) => (v === 'jump' ? '跳#' + j : v === 'stop' ? '终止' : v === 'end' ? '结束' : '下一步')
  return (f.value.steps || []).map((s, i) => {
    const a = assetOf(s.refId)
    const n = a ? (a.kind === 'flow' ? '⤷' : '') + a.name : '?' + s.refId
    return (i + 1) + '. ' + n + ' [命中→' + br(s.hit, s.hitJump) + ' / 未中→' + br(s.miss, s.missJump) + ']'
  }).join('\n') || '(空链)'
})
const okToSave = computed(() => {
  if (!f.value.name.trim() || !f.value.steps.length) return false
  return f.value.steps.every(s => s.refId)
})
function fillDemo() {
  const black = ruleOptions.value.find(r => r.name.includes('黑名单')) || ruleOptions.value[0]
  const amt = ruleOptions.value.find(r => r.def && r.def.mode === 'table') || ruleOptions.value[ruleOptions.value.length - 1]
  if (!black && !amt) { toast('规则管理为空, 请先建规则', 'warning'); return }
  f.value.steps = [
    { refId: black ? black.id : '', refType: 'rule', hit: 'stop', hitJump: 0, miss: 'next', missJump: 0 },
    { refId: amt ? amt.id : '', refType: 'rule', hit: 'next', hitJump: 0, miss: 'end', missJump: 0 }
  ]
}
function doTest() {
  if (!okToSave.value) { toast('先选好引用与步骤'); return }
  toast('干跑需 Spark 环境(试跑接口待接): 保存后可在任务详情触发 run', 'warning')
}
function save() {
  const body = {
    name: f.value.name, descr: f.value.descr || '', status: f.value.status || 1,
    def: { steps: f.value.steps.map(s => ({ refId: s.refId, hit: s.hit || 'next', hitJump: s.hitJump || 0, miss: s.miss || 'next', missJump: s.missJump || 0 })) }
  }
  const done = (fv) => {
    toast('规则流已保存: ' + fv.name + ' v' + fv.ver + ' · ' + body.def.steps.length + ' 步' + (isEdit.value ? '(升版)' : '') + '(后端环检测通过)')
    router.back()
  }
  if (isEdit.value) rulesApi.update(route.query.id, body).then(done).catch(() => {})
  else rulesApi.create(body).then(done).catch(() => {})
}

/* 快速新建并引用(进规则管理) */
const fieldPool = ['amount', 'pay_amount', 'quantity', 'region', 'status', 'order_date', 'customer_id', 'pay7d', 'net_amt']
const opers = [{ v: '>=', l: '≥' }, { v: '<=', l: '≤' }, { v: '>', l: '>' }, { v: '<', l: '<' }, { v: '==', l: '=' }, { v: 'like', l: '含' }]
const quickOpen = ref(false)
const quickIdx = ref(0)
const q = ref({ mode: 'visual', name: '', field: 'amount', op: '>=', val: '', tag: '', val2: '', buckets: [{ op: '>=', th: '', val: '' }], elseVal: '' })
function quickNew(i) {
  quickIdx.value = i
  q.value = { mode: 'visual', name: '', field: 'amount', op: '>=', val: '', tag: '', val2: '', buckets: [{ op: '>=', th: '', val: '' }], elseVal: '' }
  quickOpen.value = true
}
function saveQuick() {
  const body = {
    name: q.value.name, descr: '规则流内快速新建', kind: 'rule', status: 1,
    def: {
      mode: q.value.mode,
      visual: q.value.mode === 'visual' ? { when: [{ logic: 'AND', conds: [{ field: q.value.field, op: q.value.op, val: q.value.val }] }], actions: [{ type: 'tag', tag: q.value.tag, val: metaBool(q.value.tag) ? '' : q.value.val2 }] } : { when: [], actions: [] },
      table: q.value.mode === 'table' ? { field: q.value.field, tag: q.value.tag, rows: q.value.buckets.filter(b => b.th !== '' && b.val), elseVal: q.value.elseVal } : { field: 'amount', tag: '', rows: [], elseVal: '' },
      drl: ''
    }
  }
  rulesApi.create(body).then((saved) => {
    const st = f.value.steps[quickIdx.value]
    st.refId = saved.id
    st.refType = 'rule'
    quickOpen.value = false
    allAssets.value = awaitReload()
    toast('已保存「' + saved.name + '」并作为步骤 ' + (quickIdx.value + 1) + ' 的引用')
  }).catch(() => {})
}
async function awaitReload() {
  try { return await rulesApi.list() } catch (e) { return allAssets.value }
}
</script>

<style scoped>
.page-card { max-width: 1280px; }
.pg-title { font-size: 17px; font-weight: 700; margin-bottom: 4px; }
.sec-title { font-weight: 600; font-size: 14px; color: #2c3e50; margin: 12px 0 8px; }
.flex { display: flex; }
.chain-preview { background: #0d1b2a; border-radius: 6px; padding: 10px 12px; margin-top: 14px; }
.chain-preview .el-text { color: #9fdcb0 !important; }
.dep-bar { background: #f4f8ff; border: 1px solid #d6e4ff; border-radius: 6px; padding: 8px 10px; margin-top: 10px; }
.nav-bar { display: flex; gap: 10px; margin-top: 16px; align-items: center; }
</style>
