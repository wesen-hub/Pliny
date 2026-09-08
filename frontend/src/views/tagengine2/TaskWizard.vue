<template>
  <div class="page-card">
    <el-card shadow="never">
      <el-steps :active="step" align-center finish-status="success" style="margin-bottom:16px">
        <el-step title="数据集与事实" />
        <el-step title="绑定规则(规则/规则流)" />
        <el-step title="确认与发布" />
        <el-step title="调度与运行" />
      </el-steps>

      <!-- ═══ ① 执行形态 + 数据集与事实 ═══ -->
      <div v-if="step === 0" class="sec">
        <div class="sec-title">① 执行形态 + 数据集与事实(Fact)</div>
        <div class="flex" style="gap:10px;align-items:center;margin-bottom:12px;flex-wrap:wrap">
          <span style="font-weight:600">执行形态</span>
          <el-radio-group v-model="mode">
            <el-radio-button value="batch">批(离线/定时)</el-radio-button>
            <el-radio-button value="stream">流(近实时)</el-radio-button>
            <el-radio-button value="online">在线(瞬时置位)</el-radio-button>
          </el-radio-group>
          <el-tooltip content="批=Spark 读 Iceberg; 流=Flink 事件流(近实时双写); 在线=事件置位 Redis; 流/在线 P1 已开放" placement="top">
            <el-text size="small" type="warning">P0 仅批可用</el-text>
          </el-tooltip>
        </div>
        <div class="flex" style="gap:10px;flex-wrap:wrap;align-items:center;margin-bottom:10px">
          <el-select v-model="form.dataset" style="width:230px" @change="onDs">
            <el-option v-for="d in dsList" :key="d.tableName" :value="d.tableName" :label="d.tableName + ' · 分片' + (d.shards || '?') + ' (字段来自后端元数据)'" />
          </el-select>
          <el-text size="small" type="info">实体键</el-text>
          <el-select v-model="form.entityKey" style="width:150px">
            <el-option v-if="!pksOf.length" value="" label="(未探测到主键, 可手填)" />
            <el-option v-for="k in pksOf" :key="k" :value="k" :label="k" /></el-select>
          <el-text size="small" type="success">每行 = 一个 Fact, 规则逐行求值; 源 = 逻辑表的 Iceberg 湖表(湖落地外部推进, task-json 湖参数由平台配置)</el-text>
        </div>

        <div class="cfg-title">派生字段(事实预处理: 字段变换 / 聚合统计; 满足所绑规则字段依赖; 发布时后端校验)</div>
        <div v-for="(d, di) in derived" :key="'d' + di" class="metric-row">
          <el-input v-model="d.name" size="small" style="width:140px" placeholder="字段名, 如 pay7d" />
          <el-text size="small" type="info">=</el-text>
          <el-input v-model="d.expr" size="small" style="flex:1;min-width:260px" spellcheck="false" placeholder="表达式: concat(region,'-',city) / 或聚合 SUM(amount)(按实体近N天, 执行器实现)" />
          <el-button size="small" text type="danger" @click="derived.splice(di, 1)">✕</el-button>
        </div>
        <el-button size="small" type="primary" plain @click="derived.push({ name: '', expr: '' })">＋ 派生字段</el-button>
        <el-text size="small" type="info" style="margin-left:8px">例: pay7d = SUM(amount)(聚合, 执行器按实体 groupBy) · net = pay_amount - coupon_amount</el-text>
      </div>

      <!-- ═══ ② 绑定规则 ═══ -->
      <div v-if="step === 1" class="sec">
        <div class="sec-title">② 绑定规则(规则管理/规则流管理资产; 执行序 = 规则先跑 → 规则流按序后跑; 数据实时来自后端)</div>
        <div class="bind-sec">
          <div class="cfg-title">A. 规则(先执行)</div>
          <div v-if="!bindRule.length" class="empty-tip">尚未绑定规则 —— 从规则管理选择, 或直接新建一个</div>
          <div v-for="(r, ri) in bindRule" :key="'br' + ri" class="asset-row">
            <div class="flex" style="gap:8px;align-items:center;flex-wrap:wrap;flex:1">
              <el-tag size="small" :type="modeTag(r)" effect="plain">规则·{{ defLabel(r) }}</el-tag>
              <el-text size="small" style="font-weight:600">{{ r.name }}</el-text>
              <el-tag size="small" effect="plain">v{{ r.ver }}</el-tag>
              <el-text size="small" type="info">{{ brief(r) }}</el-text>
            </div>
            <div class="flex" style="gap:4px;align-items:center">
              <el-tag v-for="d in depsOf(r)" :key="d" size="small" :type="depOk(d) ? 'success' : 'danger'" effect="plain">{{ d }} {{ depOk(d) ? '✓' : '缺' }}</el-tag>
              <el-button size="small" text :disabled="ri === 0" @click="mvArr(bindRule, ri, -1)">↑</el-button>
              <el-button size="small" text :disabled="ri === bindRule.length - 1" @click="mvArr(bindRule, ri, 1)">↓</el-button>
              <el-button size="small" text type="danger" @click="bindRule.splice(ri, 1)">✕</el-button>
            </div>
          </div>
          <div class="flex" style="gap:8px;margin-top:6px;flex-wrap:wrap">
            <el-button size="small" type="primary" plain @click="openPicker('rule')">从规则管理选择</el-button>
            <el-button size="small" plain @click="quickOpen = true">＋ 新建规则(可视化/决策表)</el-button>
          </div>
        </div>

        <div class="bind-sec">
          <div class="cfg-title">B. 规则流(后执行, 按绑定顺序; 链内可引规则/规则流)</div>
          <div v-if="!bindFlow.length" class="empty-tip">尚未绑定规则流 —— 从规则流管理选择, 或新建一个</div>
          <div v-for="(r, ri) in bindFlow" :key="'bf' + ri" class="asset-row">
            <div class="flex" style="gap:8px;align-items:center;flex-wrap:wrap;flex:1">
              <el-tag size="small" type="warning" effect="plain">规则流</el-tag>
              <el-text size="small" style="font-weight:600">{{ r.name }}</el-text>
              <el-tag size="small" effect="plain">v{{ r.ver }}</el-tag>
              <el-text size="small" type="info">{{ brief(r) }}</el-text>
            </div>
            <div class="flex" style="gap:4px;align-items:center">
              <el-tag v-for="d in depsOf(r)" :key="d" size="small" :type="depOk(d) ? 'success' : 'danger'" effect="plain">{{ d }} {{ depOk(d) ? '✓' : '缺' }}</el-tag>
              <el-button size="small" text :disabled="ri === 0" @click="mvArr(bindFlow, ri, -1)">↑</el-button>
              <el-button size="small" text :disabled="ri === bindFlow.length - 1" @click="mvArr(bindFlow, ri, 1)">↓</el-button>
              <el-button size="small" text type="danger" @click="bindFlow.splice(ri, 1)">✕</el-button>
            </div>
          </div>
          <div class="flex" style="gap:8px;margin-top:6px;flex-wrap:wrap">
            <el-button size="small" type="primary" plain @click="openPicker('flow')">从规则流管理选择</el-button>
            <el-button size="small" plain @click="flowQuickOpen = true">＋ 新建规则流(示例决策链)</el-button>
          </div>
        </div>

        <div v-if="missingDeps.length" class="dep-warn">
          <el-text size="small" type="danger"><b>依赖不满足 {{ missingDeps.length }} 个字段</b>: {{ missingDeps.join('、') }} —— 回到①补派生字段或换数据集(发布时后端同样校验)</el-text>
        </div>
        <el-text v-else size="small" type="success" style="margin-top:6px;display:block">✓ 字段依赖全部可满足</el-text>
      </div>

      <!-- ═══ ③ 确认与发布 ═══ -->
      <div v-if="step === 2" class="sec">
        <div class="sec-title">③ 确认与发布</div>
        <el-form label-width="100px" style="max-width:620px">
          <el-form-item label="任务名称"><el-input v-model="taskName" style="width:320px" placeholder="任务名称(必填)" /></el-form-item>
          <el-form-item label="数据集"><el-text size="small">{{ form.dataset }} · 实体键 {{ form.entityKey }}</el-text></el-form-item>
          <el-form-item label="绑定资产">
            <el-text size="small">{{ bindRule.length }} 规则(先) + {{ bindFlow.length }} 规则流(后) —— 共 {{ bindRule.length + bindFlow.length }} 条</el-text>
          </el-form-item>
        </el-form>
        <div v-if="badChecks.length" class="check-bar">
          <el-text size="small" type="danger">校验 {{ badChecks.length }} 项:</el-text>
          <el-tag v-for="c in badChecks" :key="c" size="small" type="danger" effect="plain" style="margin-left:6px">{{ c }}</el-tag>
        </div>
        <el-button type="primary" :disabled="!!badChecks.length || saving" @click="createTask">{{ saving ? '保存中…' : '创建任务(草稿)' }}</el-button>
        <el-text v-if="createdId" size="small" type="success" style="margin-left:10px">已创建 #{{ createdId }} —— 下一步发布</el-text>
      </div>

      <!-- ═══ ④ 调度与运行 ═══ -->
      <div v-if="step === 3" class="sec">
        <div class="sec-title">④ 调度与运行</div>
        <el-form label-width="110px" style="max-width:640px">
          <el-form-item label="调度">
            <el-radio-group v-model="sched">
              <el-radio-button value="none">手动</el-radio-button>
              <el-radio-button value="cron">定时(cron)</el-radio-button>
            </el-radio-group>
            <el-input v-if="sched === 'cron'" v-model="cron" size="small" style="width:220px;margin-left:10px" placeholder="0 0 2 * * ?" />
            <el-text v-if="sched === 'cron'" size="small" type="info" style="margin-left:8px">后端 tick 自动触发(auto-run 开关)</el-text>
          </el-form-item>
          <el-form-item v-if="createdId && taskStatus === 'published'" label="运行">
            <el-button type="success" size="small" @click="runNow">⚡ 触发运行(演练/提交)</el-button>
            <el-text v-if="lastRun" size="small" type="info" style="margin-left:10px">run #{{ lastRun.runId }}: {{ lastRun.status }}</el-text>
          </el-form-item>
        </el-form>
        <div class="nav-bar" style="margin-top:14px">
          <el-button :disabled="step === 0" @click="step--">上一步</el-button>
          <el-button type="success" :disabled="!createdId || taskStatus === 'published'" @click="publishNow">
            {{ taskStatus === 'published' ? '✓ 已发布(快照固化)' : '发布(固化规则版本快照)' }}
          </el-button>
          <el-button v-if="taskStatus === 'published'" plain @click="$router.push('/tagengine2/task')">去任务列表</el-button>
          <div style="flex:1"></div>
          <el-text size="small" type="info">创建 → 发布 → 触发运行; 后端生成 task-json 落盘并演练/提交 Spark</el-text>
        </div>
      </div>

      <div class="nav-bar" v-if="step < 3">
        <el-button :disabled="step === 0" @click="step--">上一步</el-button>
        <el-button type="primary" @click="step++">下一步</el-button>
        <div style="flex:1"></div>
        <el-text size="small" type="info">第 {{ step + 1 }} / 4 步 · 数据已接后端 /api/tagengine/*</el-text>
      </div>
    </el-card>

    <!-- ═══ 从规则库选择 ═══ -->
    <el-dialog v-model="pickerOpen" :title="pickerKind === 'rule' ? '从规则管理选择(可多选)' : '从规则流管理选择(可多选)'" width="720">
      <el-table :data="candidates" size="small" border @selection-change="sel = $event">
        <el-table-column type="selection" width="42" />
        <el-table-column label="名称" min-width="170">
          <template #default="{ row }">
            <el-text size="small">{{ row.name }} <el-tag size="small" :type="modeTag(row)" effect="plain">{{ defLabel(row) }}</el-tag> <el-tag size="small" effect="plain">v{{ row.ver }}</el-tag></el-text>
          </template>
        </el-table-column>
        <el-table-column label="摘要" min-width="320"><template #default="{ row }"><el-text size="small" type="info">{{ brief(row) }}</el-text></template></el-table-column>
      </el-table>
      <template #footer>
        <el-button @click="pickerOpen = false">取消</el-button>
        <el-button type="primary" @click="addPicked">添加选中({{ sel.length }})</el-button>
      </template>
    </el-dialog>

    <!-- ═══ 快速新建规则 ═══ -->
    <el-dialog v-model="quickOpen" title="新建规则(可视化/决策表, 保存到规则管理并自动绑定)" width="600">
      <el-radio-group v-model="uq.mode" size="small" style="margin-bottom:10px">
        <el-radio-button value="visual">可视化(单条件)</el-radio-button>
        <el-radio-button value="table">决策表(档位)</el-radio-button>
      </el-radio-group>
      <el-form label-width="90px">
        <el-form-item label="名称"><el-input v-model="uq.name" style="width:300px" placeholder="如: 高价值客户判定" /></el-form-item>
        <template v-if="uq.mode === 'visual'">
          <el-form-item label="WHEN">
            <div class="flex" style="gap:6px;flex-wrap:wrap">
              <el-select v-model="uq.field" style="width:150px"><el-option v-for="f in condFields" :key="f" :value="f" :label="f" /></el-select>
              <el-select v-model="uq.op" style="width:90px"><el-option v-for="o in opers" :key="o.v" :value="o.v" :label="o.l" /></el-select>
              <el-input v-model="uq.val" style="width:110px" placeholder="值" />
            </div>
          </el-form-item>
          <el-form-item label="THEN 标签">
            <div class="flex" style="gap:6px;flex-wrap:wrap">
              <el-select v-model="uq.tag" style="width:180px" filterable placeholder="标签(注册表/路径)">
                <el-option v-for="t in tagOptions" :key="t" :value="t" :label="t" />
              </el-select>
              <template v-if="!metaBool(uq.tag)"><el-input v-model="uq.val2" style="width:110px" placeholder="值" /></template>
              <el-text v-else size="small" type="info">(布尔)</el-text>
            </div>
          </el-form-item>
        </template>
        <template v-else>
          <el-form-item label="决策表">
            <div class="flex" style="gap:6px;flex-wrap:wrap;align-items:center;margin-bottom:6px">
              <el-text size="small" type="info">字段</el-text>
              <el-select v-model="uq.field" size="small" style="width:140px"><el-option v-for="f in condFields" :key="f" :value="f" :label="f" /></el-select>
              <el-text size="small" type="info">标签</el-text>
              <el-select v-model="uq.tag" size="small" style="width:170px"><el-option v-for="t in tagOptions" :key="t" :value="t" :label="t" /></el-select>
            </div>
            <div v-for="(row, ri) in uq.buckets" :key="ri" class="flex" style="gap:6px;margin:4px 0;align-items:center">
              <el-select v-model="row.op" size="small" style="width:66px"><el-option value=">=" label="≥" /><el-option value="<" label="<" /></el-select>
              <el-input v-model="row.th" size="small" style="width:90px" placeholder="阈值" />
              <el-text size="small" type="info">→</el-text>
              <el-input v-model="row.val" size="small" style="width:100px" placeholder="档值" />
              <el-button size="small" text type="danger" @click="uq.buckets.splice(ri, 1)">✕</el-button>
            </div>
            <el-button size="small" text type="primary" @click="uq.buckets.push({ op: '>=', th: '', val: '' })">＋ 档位</el-button>
            <div class="flex" style="gap:6px;margin-top:6px;align-items:center">
              <el-text size="small" type="info">兜底</el-text><el-input v-model="uq.elseVal" size="small" style="width:100px" placeholder="如 小额" />
            </div>
          </el-form-item>
        </template>
      </el-form>
      <template #footer>
        <el-button @click="quickOpen = false">取消</el-button>
        <el-button type="primary" :disabled="!uq.name.trim()" @click="saveQuick">保存并绑定</el-button>
      </template>
    </el-dialog>

    <!-- ═══ 快速新建规则流 ═══ -->
    <el-dialog v-model="flowQuickOpen" title="新建规则流(示例决策链, 保存到规则流管理并自动绑定)" width="540">
      <el-form label-width="100px">
        <el-form-item label="规则流名称"><el-input v-model="fq.name" style="width:300px" placeholder="如: 风控+金额档链" /></el-form-item>
        <el-alert type="info" :closable="false" title="生成两步示例: ① 黑名单命中 → 终止整链; ② 金额档(决策表)分档; 若无同名规则则按库内可用规则回退。" />
      </el-form>
      <template #footer>
        <el-button @click="flowQuickOpen = false">取消</el-button>
        <el-button type="primary" :disabled="!fq.name.trim()" @click="saveQuickFlow">保存并绑定</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue'
import { ElMessage } from 'element-plus'
import { useRouter } from 'vue-router'
import { rulesApi, tasksApi, datasetsApi } from '../../api/tagengine.js'
import { tagOptions, metaBool, defLabel } from './mockLib.js'

const router = useRouter()
const toast = (m, t) => (t ? ElMessage[t](m) : ElMessage.info(m))
const step = ref(0)
const saving = ref(false)
const taskName = ref('')
const createdId = ref(null)
const taskStatus = ref('draft')
const lastRun = ref(null)

/* ---------- ① 形态/装配(数据集与字段来自后端元数据 /api/tagengine/datasets) ---------- */
const mode = ref('batch')
const dsList = ref([])          // [{tableName, tablePrefix, shards}]
const dsFields = ref({})        // dataset → 字段名数组
const pks = ref({})             // dataset → 主键候选
const form = ref({ dataset: '', entityKey: '' })
const pksOf = computed(() => (form.dataset && pks.value[form.dataset]) || [])
async function onDs() {
  form.entityKey = ''
  await loadDataset(form.dataset)
  const pk = pksOf.value
  const fs = dsFields.value[form.dataset] || []
  form.entityKey = pk.length ? pk[0] : (fs.includes('id') ? 'id' : (form.dataset + '_id' && fs.includes(form.dataset + '_id') ? form.dataset + '_id' : ''))
}
async function loadDataset(name) {
  if (!name || dsFields.value[name]) return
  try {
    const meta = await datasetsApi.fields(name)
    dsFields.value[name] = (meta.fields || []).map(f => f.name)
    pks.value[name] = (meta.pkCandidates && meta.pkCandidates.length) ? meta.pkCandidates : []
    if (!pks.value[name].length) {
      // 无主键信息: 优先常见 {表名}_id/id
      const fs = dsFields.value[name]
      ;['id', name + '_id'].forEach(c => { if (fs.includes(c) && !pks.value[name].length) pks.value[name] = [c] })
    }
    if (form.dataset === name && !form.entityKey && pksOf.value.length) form.entityKey = pksOf.value[0]
  } catch (e) { /* 已提示 */ }
}
onMounted(async () => {
  try {
    dsList.value = await datasetsApi.list()
    if (dsList.value.length && !form.dataset) {
      form.dataset = dsList.value[0].tableName
      await loadDataset(form.dataset)
      if (pksOf.value.length) form.entityKey = pksOf.value[0]
    }
  } catch (e) { /* 已提示 */ }
})
const derived = ref([])
const derivedNames = computed(() => derived.value.map(d => d.name).filter(Boolean))
const condFields = computed(() => [...new Set([...(dsFields.value[form.dataset] || []), ...derivedNames.value])])

/* ---------- ② 绑定(资产来自后端) ---------- */
const allAssets = ref([])
const mapOf = computed(() => new Map(allAssets.value.map(a => [Number(a.id), a])))
const bindRule = ref([])
const bindFlow = ref([])
onMounted(async () => {
  try { allAssets.value = await rulesApi.list() } catch (e) { /* 已提示 */ }
})
const mvArr = (arr, i, d) => { const j = i + d; [arr[i], arr[j]] = [arr[j], arr[i]] }

/* 展示助手(本地实现, 不依赖 mock 库内 store) */
const defLabelX = defLabel
const modeTag = (r) => (r.kind === 'flow' ? 'warning' : r.def && r.def.mode === 'drl' ? 'danger' : r.def && r.def.mode === 'table' ? 'primary' : 'success')
const brief = (r) => {
  if (!r) return '(已删)'
  if (r.kind === 'flow') {
    const names = (r.def && r.def.steps || []).map(s => { const t = mapOf.value.get(Number(s.refId)); return (t ? (t.kind === 'flow' ? '⤷' : '') + t.name : '?') }).join(' → ')
    return '编排(' + (r.def && r.def.steps || []).length + '步): ' + names
  }
  const d = r.def || {}
  if (d.mode === 'table') {
    const t = d.table || {}
    return '决策表→' + t.tag + ': ' + (t.rows || []).map(x => t.field + x.op + x.th + '→' + x.val).join('; ') + (t.elseVal ? '; 兜底→' + t.elseVal : '')
  }
  if (d.mode === 'drl') return 'DRL(' + ((d.drl || '').split('\n').length) + '行)'
  const v = d.visual || {}
  const w = (v.when || []).map(g => (g.conds || []).map(c => c.field + ' ' + c.op + ' ' + c.val).join(' 且 ')).join(' 或 ')
  return (w || '恒真') + ' → ' + (v.actions || []).map(a => a.type === 'stop' ? '停止' : '打' + a.tag).join('; ')
}
/** 本地递归依赖(flow 嵌套经全量资产表解析) */
function depCollect(r, seen) {
  const out = new Set()
  const walk = (x) => {
    if (!x || seen.has(x.id)) return
    seen.add(x.id)
    if (x.kind === 'flow') (x.def && x.def.steps || []).forEach(s => walk(mapOf.value.get(Number(s.refId))))
    else {
      const d = x.def || {}
      if (d.mode === 'table') { if (d.table && d.table.field) out.add(d.table.field) }
      else if (d.mode === 'visual') (d.visual && d.visual.when || []).forEach(g => (g.conds || []).forEach(c => { if (c.field) out.add(c.field) }))
    }
  }
  walk(r)
  return [...out]
}
const depsOf = (r) => depCollect(r, new Set())
const availFields = computed(() => [...new Set([...(dsFields.value[form.dataset] || []), ...derivedNames.value])])
const depOk = (d) => availFields.value.includes(d)
const missingDeps = computed(() => {
  const s = new Set()
  ;[...bindRule.value, ...bindFlow.value].forEach(r => depCollect(r, new Set()).forEach(d => { if (!depOk(d)) s.add(d) }))
  return [...s]
})

/* 选择器 */
const pickerOpen = ref(false)
const pickerKind = ref('rule')
const sel = ref([])
const candidates = computed(() => {
  const bound = pickerKind.value === 'rule' ? bindRule.value : bindFlow.value
  return allAssets.value.filter(r => (pickerKind.value === 'rule' ? r.kind === 'rule' : r.kind === 'flow') && r.status !== 0 && !bound.some(b => b.id === r.id))
})
const openPicker = (kind) => { pickerKind.value = kind; sel.value = []; pickerOpen.value = true }
function addPicked() {
  const arr = pickerKind.value === 'rule' ? bindRule.value : bindFlow.value
  sel.value.forEach(r => arr.push({ id: r.id, name: r.name, ver: r.ver, kind: r.kind, def: r.def }))
  toast('已绑定 ' + sel.value.length + ' 条' + (pickerKind.value === 'rule' ? '规则(A)' : '规则流(B)'))
  pickerOpen.value = false
}

/* 快速新建规则 */
const opers = [{ v: '==', l: '=' }, { v: '!=', l: '≠' }, { v: '>=', l: '≥' }, { v: '<=', l: '≤' }, { v: '>', l: '>' }, { v: '<', l: '<' }, { v: 'like', l: '含' }]
const quickOpen = ref(false)
const uq = ref({ mode: 'visual', name: '', field: 'amount', op: '>=', val: '', tag: '', val2: '', buckets: [{ op: '>=', th: '', val: '' }], elseVal: '' })
function saveQuick() {
  const body = {
    name: uq.value.name, descr: '任务内快速新建', kind: 'rule', status: 1,
    def: {
      mode: uq.value.mode,
      visual: uq.value.mode === 'visual' ? { when: [{ logic: 'AND', conds: [{ field: uq.value.field, op: uq.value.op, val: uq.value.val }] }], actions: [{ type: 'tag', tag: uq.value.tag, val: metaBool(uq.value.tag) ? '' : uq.value.val2 }] } : { when: [], actions: [] },
      table: uq.value.mode === 'table' ? { field: uq.value.field, tag: uq.value.tag, rows: uq.value.buckets.filter(b => b.th !== '' && b.val), elseVal: uq.value.elseVal } : { field: 'amount', tag: '', rows: [], elseVal: '' },
      drl: ''
    }
  }
  rulesApi.create(body).then((saved) => {
    bindRule.value.push({ id: saved.id, name: saved.name, ver: 1, kind: 'rule', def: body.def })
    quickOpen.value = false
    toast('规则已保存到规则管理并绑定(A): ' + saved.name)
    allAssets.value.push({ ...saved, def: body.def })
  }).catch(() => {})
}

/* 快速新建规则流 */
const flowQuickOpen = ref(false)
const fq = ref({ name: '' })
function saveQuickFlow() {
  const black = allAssets.value.find(r => r.kind === 'rule' && r.name.includes('黑名单')) || allAssets.value.find(r => r.kind === 'rule')
  const amt = allAssets.value.find(r => r.kind === 'rule' && r.def && r.def.mode === 'table') || allAssets.value.find(r => r.kind === 'rule')
  if (!black || !amt) { toast('规则管理为空, 请先建规则', 'warning'); return }
  const body = {
    name: fq.value.name, descr: '任务内快速新建', kind: 'flow', status: 1,
    def: { steps: [
      { refId: black.id, hit: 'stop', hitJump: 0, miss: 'next', missJump: 0 },
      { refId: amt.id, hit: 'next', hitJump: 0, miss: 'end', missJump: 0 }
    ] }
  }
  rulesApi.create(body).then((saved) => {
    bindFlow.value.push({ id: saved.id, name: saved.name, ver: 1, kind: 'flow', def: body.def })
    flowQuickOpen.value = false
    toast('规则流已保存并绑定(B): ' + saved.name)
    allAssets.value.push({ ...saved, def: body.def })
  }).catch(() => {})
}

/* ---------- ③④ 创建/发布/运行 ---------- */
const badChecks = computed(() => {
  const out = []
  if (!taskName.value.trim()) out.push('任务名称必填')
  if (!bindRule.value.length && !bindFlow.value.length) out.push('至少绑定一条规则或规则流')
  missingDeps.value.forEach(d => out.push('缺依赖字段: ' + d + '(请在①派生字段定义或换数据集)'))
  return [...new Set(out)]
})
const sched = ref('none')
const cron = ref('0 0 2 * * ?')

const bindingsPayload = computed(() => {
  const arr = []
  bindRule.value.forEach(r => arr.push({ kind: 'rule', id: r.id, ver: r.ver, name: r.name }))
  bindFlow.value.forEach(r => arr.push({ kind: 'flow', id: r.id, ver: r.ver, name: r.name }))
  return arr
})
async function createTask() {
  if (!taskName.value.trim() || !bindingsPayload.value.length) { toast('先填任务名并绑定资产', 'warning'); return }
  saving.value = true
  try {
    const r = await tasksApi.create({
      name: taskName.value, mode: mode.value, dataset: form.dataset, entityKey: form.entityKey,
      datasetFields: dsFields.value[form.dataset] || [], derived: derived.value.filter(d => d.name),
      bindings: bindingsPayload.value, schedule: sched.value, cronExpr: sched.value === 'cron' ? cron.value : ''
    })
    createdId.value = r.id
    toast('任务已创建 #' + r.id + '(草稿)')
    step.value = 3
  } catch (e) { /* 已提示 */ } finally { saving.value = false }
}
async function publishNow() {
  if (!createdId.value) return
  try {
    const r = await tasksApi.publish(createdId.value)
    taskStatus.value = r.status
    toast('已发布 v' + r.ver + ' —— 规则版本快照已固化(含定义)')
  } catch (e) { /* 已提示 */ }
}
async function runNow() {
  if (!createdId.value) return
  try {
    const r = await tasksApi.run(createdId.value)
    lastRun.value = r
    toast('run #' + r.runId + ' 已触发: ' + r.status + '(task-json 已生成' + (r.payloadPath ? ' @ ' + r.payloadPath : '') + ')')
  } catch (e) { /* 已提示 */ }
}
</script>

<style scoped>
.sec { min-height: 300px; }
.sec-title { font-weight: 600; font-size: 15px; margin-bottom: 12px; }
.flex { display: flex; }
.cfg-title { font-weight: 600; font-size: 13px; color: #2c3e50; margin: 10px 0 8px; }
.metric-row { display: flex; gap: 8px; align-items: center; margin: 6px 0; flex-wrap: wrap; background: #f8fafc; border-radius: 6px; padding: 6px 10px; }
.bind-sec { border: 1px solid #e4e7ed; border-radius: 10px; padding: 10px 14px; margin-bottom: 12px; }
.asset-row { display: flex; gap: 10px; align-items: center; justify-content: space-between; background: #f8fafc; border-radius: 6px; padding: 6px 10px; margin: 6px 0; flex-wrap: wrap; }
.empty-tip { border: 1px dashed #dcdfe6; color: #909399; border-radius: 6px; padding: 10px; font-size: 13px; }
.dep-warn { background: #fef0f0; border-radius: 6px; padding: 6px 10px; margin-top: 8px; }
.check-bar { background: #fef0f0; border-radius: 6px; padding: 6px 10px; margin: 8px 0; display: flex; align-items: center; flex-wrap: wrap; gap: 4px; }
.nav-bar { display: flex; gap: 10px; margin-top: 16px; align-items: center; }
</style>
