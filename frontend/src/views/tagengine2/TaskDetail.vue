<template>
  <div class="page-card" v-loading="loading">
    <el-card shadow="never">
      <!-- ═══ 页头 ═══ -->
      <div class="head">
        <el-button size="small" text @click="$router.push('/tagengine2/task')">← 任务列表</el-button>
        <div class="title">
          <span>{{ task?.name || '任务详情' }}</span>
          <el-tag size="small" effect="plain" :type="modeTag">{{ modeLabel }}</el-tag>
          <el-tag v-if="task" size="small" :type="task.status === 'published' ? 'success' : 'info'" effect="plain">{{ task.status }} v{{ task.ver }}</el-tag>
          <el-tag v-if="task?.mode === 'stream'" size="small" effect="plain"
            :type="task.streamStatus === 'RUNNING' ? 'success' : task.streamStatus === 'CANCELED' ? 'info' : 'warning'">{{ task.streamStatus || '流未启动' }}</el-tag>
          <el-tag v-if="task?.cursor && task.cursor.maxSeq >= 0" size="small" type="success" effect="plain">增量 seq≤{{ task.cursor.maxSeq }}</el-tag>
          <el-tag v-else-if="task?.mode === 'batch'" size="small" type="info" effect="plain">全量</el-tag>
        </div>
        <div class="ops">
          <template v-if="task">
            <el-button v-if="task.mode !== 'stream'" size="small" :disabled="task.status !== 'published'" @click="publish">发布</el-button>
            <template v-if="task.mode === 'batch'">
              <el-button size="small" type="success" :disabled="task.status !== 'published'" @click="runTask">触发运行</el-button>
              <el-button size="small" type="warning" :disabled="task.status !== 'published'" @click="fullRun">全量重算</el-button>
              <el-button size="small" :disabled="task.status !== 'published'" @click="matrixRun">试跑</el-button>
            </template>
            <template v-else-if="task.mode === 'stream'">
              <el-button v-if="task.streamStatus !== 'RUNNING'" size="small" type="success" :disabled="task.status !== 'published'" :loading="busy" @click="startStream">启动流作业</el-button>
              <el-button v-else size="small" type="warning" :loading="busy" @click="stopStream">停止流作业</el-button>
            </template>
            <template v-else-if="task.mode === 'online'">
              <el-button size="small" type="primary" :disabled="task.status !== 'published'" @click="openEvent">发事件</el-button>
            </template>
            <el-button size="small" @click="reload">刷新</el-button>
          </template>
        </div>
      </div>

      <!-- ═══ 统计格 ═══ -->
      <div v-if="ov" class="stat-grid">
        <div class="stat"><div class="n">{{ totalEntities }}</div><div class="l">产出标签实体数(投影)</div></div>
        <div class="stat"><div class="n">{{ ov.tagStats.length }}</div><div class="l">产出标签种类(tag×值)</div></div>
        <div class="stat"><div class="n">{{ rs.total }}</div><div class="l">执行总次数</div></div>
        <div class="stat"><div class="n" :class="okRate >= 0.9 ? 'ok' : okRate > 0 ? 'warn' : ''">{{ (okRate * 100).toFixed(0) }}%</div><div class="l">成功率({{ rs.ok }} 成功 / {{ rs.bad }} 失败 / {{ rs.active }} 运行中)</div></div>
        <div class="stat"><div class="n small">{{ fmtTime(rs.last_at || rs.last_finished) }}</div><div class="l">最近一次执行</div></div>
        <div class="stat"><div class="n small">{{ fmtTime(task?.update_time) }}</div><div class="l">任务更新时间</div></div>
      </div>

      <el-row :gutter="16" style="margin-top:16px">
        <!-- ═══ 基础信息 ═══ -->
        <el-col :span="10">
          <el-card shadow="never" class="sub">
            <template #header><b>基础信息</b></template>
            <el-descriptions :column="1" size="small" border>
              <el-descriptions-item label="数据集">{{ task?.dataset || '—' }}</el-descriptions-item>
              <el-descriptions-item label="实体键">{{ task?.entityKey || '—' }}</el-descriptions-item>
              <el-descriptions-item label="调度">
                {{ task?.schedule === 'cron' ? 'cron ' + task?.cronExpr : task?.schedule === 'interval' ? 'interval ' + task?.intervalMin + ' 分钟' : '手动' }}
              </el-descriptions-item>
              <el-descriptions-item label="创建">{{ fmtTime(task?.create_time) }}</el-descriptions-item>
              <el-descriptions-item label="最近自动运行">{{ fmtTime(task?.last_auto_run_at) }}</el-descriptions-item>
              <el-descriptions-item v-if="task?.mode === 'stream'" label="流作业">
                {{ task.streamJobId ? task.streamJobId.slice(0, 16) + '…' : '—' }}
              </el-descriptions-item>
            </el-descriptions>
            <div class="sec-title">绑定执行计划(规则先跑 → 规则流后跑)</div>
            <div v-if="(task?.bindings || []).length" class="chips">
              <el-tag v-for="b in task.bindings" :key="b.id" size="small" class="clickable"
                :type="b.kind === 'flow' ? 'warning' : 'primary'" effect="plain"
                @click="openAsset(b)">
                {{ b.kind === 'flow' ? '规则流' : '规则' }} · {{ b.name }} <span class="dim">#{{ b.id }} v{{ b.ver }}</span>
              </el-tag>
            </div>
            <el-text v-else size="small" type="info">未绑定资产</el-text>
          </el-card>
        </el-col>

        <!-- ═══ 字段与派生 ═══ -->
        <el-col :span="14">
          <el-card shadow="never" class="sub">
            <template #header><b>字段域与派生</b></template>
            <div class="sec-title">数据集字段({{ (task?.datasetFields || []).length }})</div>
            <div class="chips">
              <el-tag v-for="f in task?.datasetFields || []" :key="f" size="small" effect="plain">{{ f }}</el-tag>
            </div>
            <div class="sec-title">派生字段({{ (task?.derived || []).length }})</div>
            <div v-if="(task?.derived || []).length" class="chips">
              <el-tag v-for="d in task.derived" :key="d.name" size="small" type="success" effect="plain">
                {{ d.name }} = {{ d.func || 'expr' }}({{ d.field || '' }})<span v-if="d.windowSeconds" class="dim"> · 窗口 {{ d.windowSeconds }}s</span>
              </el-tag>
            </div>
            <el-text v-else size="small" type="info">无派生字段</el-text>
          </el-card>
        </el-col>
      </el-row>

      <!-- ═══ 产出标签明细 ═══ -->
      <el-card shadow="never" class="sub" style="margin-top:16px">
        <template #header><b>产出标签明细</b>(投影 tag_fact_view · 当前值; 湖 tag_fact 为权威+time travel)</template>
        <el-table :data="ov?.tagStats || []" size="small" stripe>
          <el-table-column label="标签" min-width="220"><template #default="{ row }"><el-tag size="small" type="primary" effect="plain">{{ row.tag }}</el-tag></template></el-table-column>
          <el-table-column label="值" width="130"><template #default="{ row }">{{ row.value ?? '∅' }}</template></el-table-column>
          <el-table-column label="实体数" width="100" align="center"><template #default="{ row }"><b>{{ row.entities }}</b></template></el-table-column>
          <el-table-column label="最近变更" min-width="170"><template #default="{ row }"><el-text size="small" type="info">{{ fmtTime(row.last_at) }}</el-text></template></el-table-column>
        </el-table>
        <el-text v-if="!(ov?.tagStats || []).length" size="small" type="info">尚无产出(批任务需跑一轮; 流/在线任务命中后写入投影)</el-text>
      </el-card>

      <!-- ═══ 执行历史 ═══ -->
      <el-card shadow="never" class="sub" style="margin-top:16px">
        <template #header><b>执行历史</b>({{ runs.length }} 条)</template>
        <el-table :data="runs" size="small" stripe>
          <el-table-column label="run" width="70"><template #default="{ row }">#{{ row.id }}</template></el-table-column>
          <el-table-column label="触发" width="90"><template #default="{ row }">{{ row.trigger_type }}</template></el-table-column>
          <el-table-column label="状态" width="100">
            <template #default="{ row }"><el-tag size="small" :type="row.status === 'succeeded' ? 'success' : row.status === 'failed' ? 'danger' : 'warning'" effect="plain">{{ row.status }}</el-tag></template>
          </el-table-column>
          <el-table-column label="exit" width="60" align="center"><template #default="{ row }">{{ row.exit_code ?? '—' }}</template></el-table-column>
          <el-table-column label="游标 after" width="140">
            <template #default="{ row }">
              <el-text size="small" type="info">{{ row.cursor_after ? (Object.keys(row.cursor_after || {}).map(k => k + '=' + row.cursor_after[k]).join(' ')) : '—' }}</el-text>
            </template>
          </el-table-column>
          <el-table-column label="开始" width="150"><template #default="{ row }"><el-text size="small" type="info">{{ fmtTime(row.started_at) }}</el-text></template></el-table-column>
          <el-table-column label="耗时" width="90">
            <template #default="{ row }">{{ row.finished_at && row.started_at ? ((new Date(row.finished_at) - new Date(row.started_at)) / 1000).toFixed(1) + 's' : '—' }}</template>
          </el-table-column>
          <el-table-column label="操作" width="90">
            <template #default="{ row }"><el-button size="small" text type="primary" @click="openRun(row)">详情</el-button></template>
          </el-table-column>
        </el-table>
      </el-card>

      <!-- ═══ 最近变更流 ═══ -->
      <el-card v-if="(ov?.recentNotify || []).length" shadow="never" class="sub" style="margin-top:16px">
        <template #header><b>最近标签变更(本任务审计 · 10 条)</b></template>
        <el-table :data="ov.recentNotify" size="small">
          <el-table-column label="时间" width="180"><template #default="{ row }"><el-text size="small" type="info">{{ fmtTime(row.occur_ts) }}</el-text></template></el-table-column>
          <el-table-column prop="entity" label="实体" width="150" />
          <el-table-column label="标签" min-width="220"><template #default="{ row }"><el-tag size="small" type="primary" effect="plain">{{ row.tag_path }}</el-tag></template></el-table-column>
          <el-table-column label="值" width="120"><template #default="{ row }">{{ row.tag_value ?? '∅' }}</template></el-table-column>
        </el-table>
      </el-card>
    </el-card>

    <!-- ═══ run 详情抽屉 ═══ -->
    <el-drawer v-model="runDlg" :title="'run #' + (curRun.id ?? '')" size="680px">
      <el-text size="small" type="info" style="display:block;margin-bottom:6px">submit: {{ curRun.submit_cmd || '—' }}</el-text>
      <template v-if="curRun.matrix">
        <div class="sec-title">试跑命中矩阵(采样求值, 未写 tag_fact)</div>
        <el-table :data="curRun.matrix.assets || []" size="small" border>
          <el-table-column label="资产" min-width="200"><template #default="{ row }">{{ row.kind }}:{{ row.name }} (id{{ row.id }} v{{ row.ver }})</template></el-table-column>
          <el-table-column label="命中行" width="90" align="center"><template #default="{ row }">{{ row.hitRows }}</template></el-table-column>
          <el-table-column label="标签行" width="90" align="center"><template #default="{ row }">{{ row.tagRows }}</template></el-table-column>
        </el-table>
        <el-text size="small" type="info" style="display:block;margin:6px 0">总行数 {{ curRun.matrix.totalRows }} · 空实体 {{ curRun.matrix.nullEntityRows }}</el-text>
      </template>
      <template v-else-if="curRun.summary">
        <div class="sec-title">run 摘要</div>
        <pre class="mono">{{ JSON.stringify(curRun.summary, null, 2) }}</pre>
      </template>
      <div v-if="curRun.error_msg" class="sec-title">错误</div>
      <pre v-if="curRun.error_msg" class="mono err">{{ curRun.error_msg }}</pre>
      <div class="sec-title" style="margin-top:10px">task-json payload</div>
      <pre class="mono">{{ curRun.payloadText }}</pre>
    </el-drawer>

    <!-- 在线发事件 -->
    <el-dialog v-model="eventDlg" :title="'在线事件 · ' + (task?.name || '')" width="560" append-to-body>
      <el-text size="small" type="info" style="display:block;margin-bottom:8px">事件 JSON(含实体键 {{ task?.entityKey || '?' }} 与规则字段; 命中即置位 Redis)</el-text>
      <el-input v-model="eventJson" type="textarea" :rows="6" />
      <template #footer>
        <el-button size="small" @click="eventDlg = false">取消</el-button>
        <el-button size="small" type="primary" :loading="eventSending" @click="sendEvent">求值</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue'
import { useRoute } from 'vue-router'
import { ElMessage, ElMessageBox } from 'element-plus'
import { tasksApi, runsApi, streamApi, onlineApi } from '../../api/tagengine.js'

const route = useRoute()
const toast = (m, t) => (t ? ElMessage[t](m) : ElMessage.info(m))
const loading = ref(false)
const busy = ref(false)
const task = ref(null)
const ov = ref(null)
const runs = ref([])
const runDlg = ref(false)
const curRun = ref({})
const eventDlg = ref(false)
const eventJson = ref('')
const eventSending = ref(false)

const taskId = computed(() => Number(route.query.id || 0))
const modeLabel = computed(() => ({ batch: '批 Batch', stream: '流 Stream', online: '在线 Online' }[task.value?.mode] || '—'))
const modeTag = computed(() => ({ batch: 'primary', stream: 'warning', online: 'info' }[task.value?.mode] || 'info'))
const rs = computed(() => ov.value?.runStats || { total: 0, ok: 0, bad: 0, active: 0 })
const okRate = computed(() => (rs.value.total ? rs.value.ok / rs.value.total : 0))
const totalEntities = computed(() => (ov.value?.tagStats || []).reduce((a, x) => a + Number(x.entities || 0), 0))

function fmtTime(t) {
  if (!t) return '—'
  return String(t).slice(0, 19).replace('T', ' ')
}
async function load() {
  loading.value = true
  try {
    const [t, o] = await Promise.all([tasksApi.get(taskId.value), tasksApi.overview(taskId.value)])
    task.value = t
    ov.value = o
    const r = await runsApi.list(taskId.value)
    runs.value = Array.isArray(r) ? r : []
  } catch (e) { /* 已提示 */ } finally { loading.value = false }
}
function reload() { load() }

/* 操作 */
function publish() {
  tasksApi.publish(taskId.value).then((r) => { toast(`已发布 v${r.ver}(快照已固化)`); load() }).catch(() => {})
}
function runTask() {
  tasksApi.run(taskId.value).then((r) => { toast(`run #${r.runId} 已触发: ${r.status}`); load() }).catch(() => {})
}
function fullRun() {
  ElMessageBox.confirm('全量重算: 忽略增量游标整集重算(跑完重新初始化游标)。确定?', '全量重算', { type: 'warning' })
    .then(() => tasksApi.run(taskId.value, true).then((r) => { toast(`全量 run #${r.runId} 已触发`); load() }).catch(() => {}))
    .catch(() => {})
}
function matrixRun() {
  tasksApi.run(taskId.value, false, true).then((r) => { toast(`试跑 run #${r.runId} 已触发(命中矩阵, 不写 tag_fact)`); load() }).catch(() => {})
}
function startStream() {
  busy.value = true
  streamApi.start(taskId.value).then((r) => { toast(`流作业已启动 job=${r.jobId}`); load() }).catch(() => {}).finally(() => { busy.value = false })
}
function stopStream() {
  ElMessageBox.confirm('停止流作业? 将 cancel Flink 作业(任务保留可再启动)。', '停止流作业', { type: 'warning' })
    .then(() => streamApi.stop(taskId.value).then(() => { toast('已停止'); load() }).catch(() => {}))
    .catch(() => {})
}
function openEvent() {
  eventJson.value = JSON.stringify({ [task.value?.entityKey || 'id']: '', ttlSeconds: 600 }, null, 0)
    .replace('"":', '"E' + Date.now().toString().slice(-4) + '":')
  eventDlg.value = true
}
function sendEvent() {
  let body
  try { body = JSON.parse(eventJson.value) } catch (e) { toast('事件 JSON 格式错误', 'warning'); return }
  eventSending.value = true
  onlineApi.event(taskId.value, body).then((r) => {
    toast(`已求值: 命中 ${(r.tags || []).length} 个标签 → ${r.key}(TTL ${r.ttlSeconds}s)`)
    eventDlg.value = false
  }).catch(() => {}).finally(() => { eventSending.value = false })
}
function openAsset(b) {
  if (b.kind === 'flow') this.$router?.push(`/tagengine2/rule/flow?id=${b.id}`)
  else this.$router?.push(`/tagengine2/rule/edit?id=${b.id}`)
}
function openRun(row) {
  runsApi.get(row.id).then((r) => { curRun.value = r; runDlg.value = true }).catch(() => {})
}
onMounted(load)
</script>

<style scoped>
.page-card { max-width: 1460px; }
.head { display: flex; align-items: center; gap: 10px; flex-wrap: wrap; }
.head .title { font-size: 18px; font-weight: 700; display: flex; align-items: center; gap: 8px; flex-wrap: wrap; }
.head .ops { margin-left: auto; display: flex; gap: 4px; flex-wrap: wrap; }
.stat-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 12px; margin-top: 16px; }
.stat { background: var(--el-fill-color-lighter, #f6f7f9); border-radius: 8px; padding: 12px 14px; }
.stat .n { font-size: 24px; font-weight: 700; }
.stat .n.ok { color: var(--el-color-success); }
.stat .n.warn { color: var(--el-color-warning); }
.stat .n.small { font-size: 14px; font-weight: 600; }
.stat .l { color: var(--el-text-color-secondary); font-size: 12px; margin-top: 2px; }
.sub { border: none; }
.chips { display: flex; flex-wrap: wrap; gap: 6px; margin-bottom: 8px; }
.chips .clickable { cursor: pointer; }
.dim { color: #999; font-weight: 400; }
.sec-title { font-size: 13px; font-weight: 600; margin: 10px 0 6px; color: var(--el-text-color-regular); }
.mono { background: #0d1b2a; color: #9fdcb0; padding: 10px; font-size: 12px; border-radius: 6px; max-height: 300px; overflow: auto; white-space: pre-wrap; word-break: break-all; }
.mono.err { color: #f9a1a1; background: #2a0d0d; }
</style>
