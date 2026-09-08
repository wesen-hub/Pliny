<template>
  <div class="page-card">
    <el-card shadow="never" v-loading="loading">
      <div class="flex" style="justify-content:space-between;align-items:center;margin-bottom:4px">
        <div>
          <div class="pg-title">打标任务(V2)</div>
          <el-text size="small" type="info">任务 = 执行形态(批) + 事实装配(逻辑表 + 派生字段) + 绑定的规则/规则流(执行序: 规则先跑 → 规则流后跑);已接后端。发布固化版本快照;「触发运行」生成 task-json 并演练/提交(见 run 记录)。</el-text>
        </div>
        <el-button type="primary" @click="$router.push('/tagengine2/task/create')">＋ 新建打标任务</el-button>
      </div>

      <el-table :data="tasks" size="small" border stripe style="margin-top:10px">
        <el-table-column label="任务名称" min-width="160"><template #default="{ row }"><el-text size="small" style="font-weight:600">{{ row.name }}</el-text></template></el-table-column>
        <el-table-column label="数据集/实体" min-width="170"><template #default="{ row }"><el-text size="small">{{ row.dataset }} · {{ row.entityKey }}</el-text></template></el-table-column>
        <el-table-column label="绑定(执行序)" min-width="260">
          <template #default="{ row }">
            <div style="display:flex;flex-wrap:wrap;gap:4px;align-items:center">
              <el-tag v-for="(b, i) in row.bindings" :key="i" size="small" :type="b.kind === 'flow' ? 'warning' : 'primary'" effect="plain">
                {{ b.kind === 'flow' ? '规则流' : '规则' }}:{{ b.name }}
              </el-tag>
              <el-text size="small" type="info">({{ (row.bindings || []).filter(b => b.kind !== 'flow').length }} 规则 → {{ (row.bindings || []).filter(b => b.kind === 'flow').length }} 流)</el-text>
            </div>
          </template>
        </el-table-column>
        <el-table-column label="版本" width="60"><template #default="{ row }"><el-tag size="small" effect="plain">v{{ row.ver }}</el-tag></template></el-table-column>
        <el-table-column label="增量游标" width="110"><template #default="{ row }">
            <el-tag v-if="row.cursor && row.cursor.maxSeq >= 0" size="small" type="success" effect="plain">seq≤{{ row.cursor.maxSeq }}</el-tag>
            <el-text v-else size="small" type="info">全量</el-text>
          </template></el-table-column>
        <el-table-column label="调度" width="90"><template #default="{ row }"><el-text size="small">{{ row.schedule === 'cron' ? row.cronExpr : (row.schedule || 'none') }}</el-text></template></el-table-column>
        <el-table-column label="状态" width="90">
          <template #default="{ row }">
            <el-tag size="small" :type="row.status === 'published' ? 'success' : 'info'" effect="plain">{{ row.status }}</el-tag>
          </template>
        </el-table-column>
        <el-table-column label="操作" width="300" fixed="right">
          <template #default="{ row }">
            <el-button size="small" text type="primary" @click="publish(row)">发布</el-button>
            <el-button size="small" text type="success" :disabled="row.status !== 'published'" @click="runTask(row)">触发运行</el-button>
            <el-button size="small" text type="warning" :disabled="row.status !== 'published'" @click="fullRun(row)">全量重算</el-button>
            <el-button size="small" text @click="openRuns(row)">Runs</el-button>
            <el-button size="small" text type="danger" @click="del(row)">删</el-button>
          </template>
        </el-table-column>
      </el-table>
      <el-text size="small" type="info" style="margin-top:8px;display:block">发布=固化规则版本快照(含定义,规则库升版不影响已发布任务);触发运行默认演练(simulate),配置 runner-mode=spark 后真实 spark-submit。</el-text>
    </el-card>

    <!-- ═══ runs 抽屉 ═══ -->
    <el-drawer v-model="runsDlg" :title="'运行记录 · ' + (curTask ? curTask.name : '')" size="560px">
      <el-table :data="runs" size="small" border>
        <el-table-column label="run" width="70"><template #default="{ row }">#{{ row.id }}</template></el-table-column>
        <el-table-column label="触发" width="80"><template #default="{ row }">{{ row.trigger_type }}</template></el-table-column>
        <el-table-column label="状态" width="100">
          <template #default="{ row }">
            <el-tag size="small" :type="row.status === 'succeeded' ? 'success' : row.status === 'failed' ? 'danger' : 'warning'" effect="plain">{{ row.status }}</el-tag>
          </template>
        </el-table-column>
        <el-table-column label="exit" width="60" align="center"><template #default="{ row }">{{ row.exit_code ?? '—' }}</template></el-table-column>
        <el-table-column label="时间" width="150"><template #default="{ row }"><el-text size="small" type="info">{{ (row.started_at || '').slice(5, 19) }}</el-text></template></el-table-column>
        <el-table-column label="操作" width="70">
          <template #default="{ row }"><el-button size="small" text type="primary" @click="openRun(row)">看 json</el-button></template>
        </el-table-column>
      </el-table>
      <el-dialog v-model="runJsonDlg" title="task-json / run 摘要" width="720" append-to-body>
        <el-text size="small" type="info" style="display:block;margin-bottom:6px">submit: {{ curRun.submit_cmd }}</el-text>
        <pre style="max-height:420px;overflow:auto;background:#0d1b2a;color:#9fdcb0;padding:10px;font-size:12px">{{ curRun.payloadText }}</pre>
      </el-dialog>
    </el-drawer>
  </div>
</template>

<script setup>
import { ref, onMounted } from 'vue'
import { useRouter } from 'vue-router'
import { ElMessage, ElMessageBox } from 'element-plus'
import { tasksApi, runsApi } from '../../api/tagengine.js'

const router = useRouter()
const toast = (m, t) => (t ? ElMessage[t](m) : ElMessage.info(m))
const loading = ref(false)
const tasks = ref([])
const runsDlg = ref(false)
const runs = ref([])
const curTask = ref(null)
const runJsonDlg = ref(false)
const curRun = ref({})

async function load() {
  loading.value = true
  try { tasks.value = await tasksApi.list() } catch (e) { /* 已提示 */ } finally { loading.value = false }
}
onMounted(load)

function publish(row) {
  tasksApi.publish(row.id).then((r) => {
    toast(`已发布 v${r.ver}(快照已固化)`)
    load()
  }).catch(() => {})
}
function runTask(row) {
  tasksApi.run(row.id).then((r) => {
    toast(`run #${r.runId} 已触发: ${r.status} —— ${r.submitCmd ? '命令已生成(见 Runs→看 json)' : ''}`)
  }).catch(() => {})
}
function fullRun(row) {
  ElMessageBox.confirm('全量重算: 忽略当前增量游标, 整集重算(跑完自动重新初始化游标)。确定?', '全量重算', { type: 'warning' })
    .then(() => tasksApi.run(row.id, true).then((r) => {
      toast(`全量 run #${r.runId} 已触发: ${r.status}`)
    }).catch(() => {}))
    .catch(() => {})
}
async function openRuns(row) {
  curTask.value = row
  try {
    runs.value = await runsApi.list(row.id)
    runsDlg.value = true
  } catch (e) { /* 已提示 */ }
}
async function openRun(row) {
  try {
    curRun.value = await runsApi.get(row.id)
    runJsonDlg.value = true
  } catch (e) { /* 已提示 */ }
}
function del(row) {
  ElMessageBox.confirm('删除任务「' + row.name + '」? 仅解除任务与规则绑定, 不影响规则库。', '删除任务', { type: 'warning' })
    .then(() => tasksApi.remove(row.id).then(() => { toast('已删除'); load() }).catch(() => {}))
    .catch(() => {})
}
</script>

<style scoped>
.page-card { max-width: 1400px; }
.pg-title { font-size: 17px; font-weight: 700; margin-bottom: 4px; }
.flex { display: flex; }
</style>
