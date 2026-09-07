<template>
  <div class="page-card">
    <el-card shadow="never">
      <div class="pg-title">{{ isEdit ? '编辑编排规则(规则流)' : '新建编排规则(规则流)' }}</div>
      <el-text size="small" type="info">规则流 = 把规则库里的<b>任意规则</b>(基础规则, 甚至别的规则流)按序编排成一条更复杂的规则。每步: 引用一条规则 → 命中/未命中各自分支(下一步 / 终止整链 / 跳转第 N 步)。金额档可直接引用「金额档」分档规则, 不需要先拆档位。嵌套引用保存时会做环检测。</el-text>

      <el-form label-width="96px" style="max-width:720px;margin-top:12px">
        <el-form-item label="规则名称">
          <el-input v-model="f.name" style="width:320px" placeholder="如: 风控优先决策链" />
        </el-form-item>
        <el-form-item label="说明">
          <el-input v-model="f.desc" style="width:560px" placeholder="一句话说明(可选)" />
        </el-form-item>
      </el-form>

      <div class="sec-title">编排步骤(自上而下; 每步引用一条规则; 被引规则自带完整判定[分档=整组排他])</div>
      <el-table :data="f.steps" size="small" border style="margin-bottom:8px">
        <el-table-column label="步" type="index" width="44" />
        <el-table-column label="引用规则" min-width="240">
          <template #default="{ row, $index }">
            <div class="flex" style="gap:6px;align-items:center;flex-wrap:wrap">
              <el-select v-model="row.ruleId" size="small" style="width:210px" placeholder="从规则库选择">
                <el-option v-for="r in libRules" :key="r.id" :value="r.id" :disabled="r.status === 0" :label="(r.status === 0 ? '[停] ' : '') + r.name + ' · v' + r.ver + ' (' + kindLabel(r) + ')'" />
              </el-select>
              <el-button size="small" text type="primary" @click="quickNew($index)">＋ 新建并引用</el-button>
            </div>
            <el-text v-if="ruleOf(row.ruleId)" size="small" type="info">{{ sumRule(ruleOf(row.ruleId)) }}</el-text>
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
            <el-text v-if="!stepDeps(row).length" size="small" type="info">—</el-text>
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
        <el-button size="small" plain @click="fillDemo">填入示例(黑名单 → 金额档)</el-button>
        <el-text size="small" type="warning">停止 = 终止整条规则流(后续步骤不再跑); 跳转目标 = 步骤序号(1 起); 被引用规则「命中」= 其动作至少执行一条(分档命中任一档)</el-text>
      </div>

      <div class="chain-preview">
        <el-text size="small"><b>流结构预览</b></el-text>
        <div style="margin-top:4px;white-space:pre-wrap"><el-text size="small" type="info">{{ preview }}</el-text></div>
      </div>
      <div class="dep-bar">
        <el-text size="small"><b>总字段依赖(递归)</b>: {{ deps.length ? deps.join('、') : '—' }} —— 任务绑定时校验 数据集字段∪派生字段 满足</el-text>
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
      <el-radio-group v-model="q.style" size="small" style="margin-bottom:10px">
        <el-radio-button value="cond">单条规则</el-radio-button>
        <el-radio-button value="bucket">分档规则</el-radio-button>
      </el-radio-group>
      <el-form label-width="88px">
        <el-form-item label="名称"><el-input v-model="q.name" style="width:280px" placeholder="如: 高频复购判定" /></el-form-item>
        <template v-if="q.style === 'cond'">
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
          <el-form-item label="分档">
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
        <el-button type="primary" :disabled="!q.name.trim() || (q.style === 'cond' && (!q.field || !q.tag))" @click="saveQuick">保存并作为本步骤引用</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup>
import { ref, computed } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { ElMessage } from 'element-plus'
import { ruleState, ruleOf, kindLabel, sumRule, stepText, upsertRule, upsertRule as upsertFlow, cycleCheck, depNamesOf } from './mockLib.js'

const route = useRoute()
const router = useRouter()
const toast = (m, t) => (t ? ElMessage[t](m) : ElMessage.info(m))
const isEdit = computed(() => !!route.query.id)
const src = isEdit.value ? ruleOf(route.query.id) : null
const f = ref(src ? JSON.parse(JSON.stringify(src)) : { id: undefined, name: '', desc: '', kind: 'flow', status: 1, steps: [] })

const libRules = computed(() => ruleState.rules)
const newStep = () => ({ ruleId: '', hit: 'next', hitJump: 1, miss: 'next', missJump: 1 })
function mv(i, d) {
  const arr = f.value.steps
  const j = i + d
  ;[arr[i], arr[j]] = [arr[j], arr[i]]
}
const stepDeps = (st) => depNamesOf(ruleOf(st.ruleId))
const deps = computed(() => depNamesOf(f.value))
const preview = computed(() => (f.value.steps || []).map((s, i) => (i + 1) + '. ' + stepText(s)).join('\n') || '(空链)')
const okToSave = computed(() => {
  if (!f.value.name.trim() || !f.value.steps.length) return false
  return f.value.steps.every(s => s.ruleId)
})
function fillDemo() {
  const black = ruleState.rules.find(r => r.id === 'r2')
  const amt = ruleState.rules.find(r => r.id === 'r1')
  f.value.steps = [
    { ruleId: black ? 'r2' : (amt ? 'r1' : ''), hit: 'stop', hitJump: 0, miss: 'next', missJump: 0 },
    { ruleId: amt ? 'r1' : '', hit: 'next', hitJump: 0, miss: 'end', missJump: 0 }
  ]
}
function doTest() {
  if (!okToSave.value) { toast('先选好规则与步骤'); return }
  ElMessage.success('干跑(mock): 样本 5,000 行 · 黑名单 63 行(整链终止) · 其余进金额档 4,937 行 · 无成环')
}
function save() {
  if (cycleCheck(f.value)) { toast('成环! 编排链不能直接/间接引用自身', 'error'); return }
  const fv = upsertFlow(JSON.parse(JSON.stringify(f.value)))
  toast('规则流已保存: ' + fv.name + ' v' + fv.ver + ' · ' + fv.steps.length + ' 步' + (isEdit.value ? '(升版)' : ''))
  router.back()
}

/* 快速新建并引用 */
const fieldPool = ['amount', 'pay_amount', 'quantity', 'region', 'status', 'order_date', 'customer_id', 'pay7d', 'net_amt']
const opers = [{ v: '>=', l: '≥' }, { v: '<=', l: '≤' }, { v: '>', l: '>' }, { v: '<', l: '<' }, { v: '==', l: '=' }, { v: 'like', l: '含' }]
const quickOpen = ref(false)
const quickIdx = ref(0)
const q = ref({ style: 'cond', name: '', field: 'amount', op: '>=', val: '', tag: '', val2: '', buckets: [{ op: '>=', th: '', val: '' }], elseVal: '' })
function quickNew(i) {
  quickIdx.value = i
  q.value = { style: 'cond', name: '', field: 'amount', op: '>=', val: '', tag: '', val2: '', buckets: [{ op: '>=', th: '', val: '' }], elseVal: '' }
  quickOpen.value = true
}
function saveQuick() {
  const rule = {
    name: q.value.name, desc: '流内快速新建', kind: 'simple', status: 1, style: q.value.style,
    when: q.value.style === 'cond' ? [{ logic: 'AND', conds: [{ field: q.value.field, op: q.value.op, val: q.value.val }] }] : [],
    actions: q.value.style === 'cond' ? [{ type: 'tag', tag: q.value.tag, val: metaBool(q.value.tag) ? '' : q.value.val2 }] : [],
    bucket: q.value.style === 'bucket' ? { field: q.value.field, tag: q.value.tag, rows: q.value.buckets.filter(b => b.th !== '' && b.val), elseVal: q.value.elseVal } : undefined
  }
  const saved = upsertRule(rule)
  f.value.steps[quickIdx.value].ruleId = saved.id
  quickOpen.value = false
  toast('已保存「' + saved.name + '」并作为步骤 ' + (quickIdx.value + 1) + ' 的引用')
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
