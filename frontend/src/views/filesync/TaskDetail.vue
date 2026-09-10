<template>
  <div v-if="task" class="page-card">
    <div class="head">
      <div>
        <el-button size="small" @click="router.push('/filesync/task')">← 返回</el-button>
        <span class="title">{{ task.name }}</span>
        <el-tag size="small" :type="task.exec_host === 'agent' ? 'primary' : 'success'" effect="plain">{{ task.exec_host === 'agent' ? `Agent#${task.agent_id}` : '平台执行' }}</el-tag>
        <el-tag v-if="task.exec_host === 'agent' && !task.online" size="small" type="danger">Agent 离线</el-tag>
        <el-tag size="small" :type="statusType(task.run_status)" effect="plain">{{ statusName(task.run_status) }}</el-tag>
      </div>
      <div>
        <el-button size="small" @click="reload">刷新</el-button>
        <el-button size="small" @click="router.push(`/filesync/task/${id}/edit`)">编辑</el-button>
        <template v-if="isWatch">
          <el-button v-if="task.watching || task.run_status === 'WATCHING'" type="danger" size="small" @click="stopWatch">■ 停止实时同步</el-button>
          <el-button v-else type="success" size="small" @click="run">▶ 启动实时同步</el-button>
        </template>
        <el-button v-else type="success" size="small" :disabled="task.run_status === 'RUNNING'" @click="run">执行一轮</el-button>
        <el-button type="danger" size="small" plain @click="remove">删除任务</el-button>
      </div>
    </div>

    <el-descriptions :column="3" border size="small" style="margin-top:10px">
      <el-descriptions-item label="源数据源">
        {{ task.source_ds_name || `#${task.source_datasource_id}` }}
        <el-tag size="small" type="info" effect="plain">{{ task.source_ds_type || '?' }}</el-tag>
      </el-descriptions-item>
      <el-descriptions-item label="源路径">{{ task.source_path || '(整源)' }}</el-descriptions-item>
      <el-descriptions-item label="执行端">{{ task.exec_host === 'agent' ? `Agent#${task.agent_id}` : '平台(容器 mc)' }}</el-descriptions-item>
      <el-descriptions-item label="目标">
        {{ task.target_ds_name ? task.target_ds_name + ' · ' : '' }}s3://{{ task.target_bucket }}{{ task.target_prefix ? '/' + task.target_prefix : '' }}
      </el-descriptions-item>
      <el-descriptions-item label="调度">{{ schedName(task) }}</el-descriptions-item>
      <el-descriptions-item label="最近一轮">
        <template v-if="summ">
          {{ summ.transferred ?? 0 }} 文件 / {{ fmtBytes(summ.bytes) }}
          <el-text v-if="summ.failed" type="danger"> / 失败 {{ summ.failed }}</el-text>
          <el-text type="info" style="margin-left:6px;font-size:11px">{{ summ.durationMs ? (summ.durationMs / 1000).toFixed(1) + 's' : '' }}</el-text>
        </template>
        <span v-else>—</span>
      </el-descriptions-item>
    </el-descriptions>

    <el-card shadow="never" style="margin-top:12px">
      <template #header>
        <div class="sec-head"><b>同步策略</b></div>
      </template>
      <template v-if="stratList.length">
        <el-tag v-for="t in stratList" :key="t.text" :type="t.type" size="small" effect="plain" style="margin:0 6px 6px 0">{{ t.text }}</el-tag>
      </template>
      <el-text v-else size="small" type="info">默认策略:增量同步,只搬新增文件,不覆盖已有</el-text>
    </el-card>

    <el-card shadow="never" style="margin-top:12px">
      <template #header>
        <div class="sec-head">
          <b>传输历史(每轮 = 一次 mc mirror)</b>
          <el-text size="small" type="info">共 {{ runs.total }} 轮 · 目标桶即当前状态,轮次文件明细用于审计</el-text>
        </div>
      </template>
      <el-table :data="runs.items" size="small" border stripe v-loading="runsLoading">
        <el-table-column type="expand">
          <template #default="{ row }">
            <RunFiles :task-id="id" :run-id="row.id" />
          </template>
        </el-table-column>
        <el-table-column prop="id" label="run#" width="70" />
        <el-table-column label="时间" width="175">
          <template #default="{ row }">{{ row.started_at }}</template>
        </el-table-column>
        <el-table-column prop="trigger_type" label="触发" width="90" />
        <el-table-column prop="executor" label="执行端" min-width="110" />
        <el-table-column label="状态" width="100">
          <template #default="{ row }">
            <el-tag size="small" :type="statusType(row.status)" effect="plain">{{ statusName(row.status) }}</el-tag>
          </template>
        </el-table-column>
        <el-table-column label="transferred" width="100">
          <template #default="{ row }">{{ row.summary?.transferred ?? 0 }}</template>
        </el-table-column>
        <el-table-column label="bytes" width="95">
          <template #default="{ row }">{{ fmtBytes(row.summary?.bytes) }}</template>
        </el-table-column>
        <el-table-column label="失败" width="70">
          <template #default="{ row }">
            <el-text v-if="row.summary?.failed" type="danger">{{ row.summary.failed }}</el-text>
            <span v-else>0</span>
          </template>
        </el-table-column>
        <el-table-column label="耗时" width="85">
          <template #default="{ row }">{{ row.summary?.durationMs ? (row.summary.durationMs / 1000).toFixed(1) + 's' : '—' }}</template>
        </el-table-column>
        <el-table-column prop="error_msg" label="错误" min-width="140" show-overflow-tooltip />
      </el-table>
      <el-pagination style="margin-top:10px" background layout="total, prev, pager, next" :total="runs.total"
        :page-size="pageSize" :current-page="page" @current-change="loadRuns" />
    </el-card>

    <el-card shadow="never" style="margin-top:12px">
      <template #header>
        <div class="sec-head">
          <b>目标浏览(实时)</b>
          <el-text size="small" type="info">直接从目标桶 mc ls,当前状态以这里为准</el-text>
        </div>
      </template>
      <div style="margin-bottom:10px;display:flex;align-items:center;gap:4px;flex-wrap:wrap">
        <el-link type="primary" :underline="false" @click="go('')">s3://{{ task.target_bucket }}</el-link>
        <template v-for="(seg, i) in browsePath" :key="i">
          <span style="color:#c0c4cc">/</span>
          <el-link type="primary" :underline="false" @click="go(browsePath.slice(0, i + 1).join('/'))">{{ seg }}</el-link>
        </template>
        <el-button size="small" style="margin-left:auto" @click="loadBrowse">刷新</el-button>
      </div>
      <el-table :data="browseItems" size="small" border stripe v-loading="browseLoading">
        <el-table-column label="名称" min-width="280">
          <template #default="{ row }">
            <el-link v-if="row.folder" type="primary" @click="go(browsePath.concat(row.name).join('/'))">📁 {{ row.name }}/</el-link>
            <span v-else>📄 {{ row.name }}</span>
          </template>
        </el-table-column>
        <el-table-column label="类型" width="90">
          <template #default="{ row }">
            <el-tag size="small" :type="row.folder ? 'warning' : 'info'" effect="plain">{{ row.folder ? '目录' : '文件' }}</el-tag>
          </template>
        </el-table-column>
        <el-table-column label="大小" width="110">
          <template #default="{ row }">{{ row.folder ? '—' : fmtBytes(row.size) }}</template>
        </el-table-column>
        <el-table-column prop="lastModified" label="修改时间" width="190" />
      </el-table>
      <el-text v-if="!browseLoading && !browseItems.length" size="small" type="info">目标下暂无文件(或目录为空)</el-text>
    </el-card>

    <el-card shadow="never" style="margin-top:12px">
      <template #header>
        <div class="sec-head">
          <b>一致性对账</b>
          <el-text size="small" type="info">只报告不修改: 源/目标清单递归比对</el-text>
        </div>
      </template>
      <div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap;margin-bottom:8px">
        <el-button size="small" type="primary" :loading="reconcileLoading" @click="doReconcile">开始对账</el-button>
        <template v-if="reconcile">
          <el-tag size="small" effect="plain">源 {{ reconcile.sourceCount }}</el-tag>
          <el-tag size="small" effect="plain">目标 {{ reconcile.targetCount }}</el-tag>
          <el-tag size="small" :type="reconcile.missingCount ? 'warning' : 'success'" effect="plain">目标缺失 {{ reconcile.missingCount }}</el-tag>
          <el-tag size="small" :type="reconcile.extraCount ? 'warning' : 'success'" effect="plain">目标多余 {{ reconcile.extraCount }}</el-tag>
          <el-tag size="small" :type="reconcile.sizeDiffCount ? 'warning' : 'success'" effect="plain">大小不一致 {{ reconcile.sizeDiffCount }}</el-tag>
          <el-text size="small" type="info">{{ reconcile.elapsedMs }}ms</el-text>
          <el-tag v-if="reconcile.truncated" size="small" type="danger" effect="plain">清单已截断</el-tag>
        </template>
      </div>
      <el-alert v-if="reconcile && reconcile.note" :title="reconcile.note" type="warning" :closable="false" show-icon style="margin-bottom:8px" />
      <template v-if="reconcile">
        <el-text v-if="!reconcile.missingCount && !reconcile.extraCount && !reconcile.sizeDiffCount" size="small" type="success">源与目标一致 ✓(本任务无差异)</el-text>
        <el-tabs v-else v-model="reconcileTab">
          <el-tab-pane :label="`目标缺失 (${reconcile.missingCount})`" name="missing">
            <el-table :data="reconcile.missing" size="small" border stripe max-height="260">
              <el-table-column prop="relPath" label="相对路径" min-width="280" show-overflow-tooltip />
              <el-table-column label="源大小" width="110"><template #default="{ row }">{{ fmtBytes(row.sourceSize) }}</template></el-table-column>
            </el-table>
          </el-tab-pane>
          <el-tab-pane :label="`目标多余 (${reconcile.extraCount})`" name="extra">
            <el-table :data="reconcile.extra" size="small" border stripe max-height="260">
              <el-table-column prop="relPath" label="相对路径" min-width="280" show-overflow-tooltip />
              <el-table-column label="目标大小" width="110"><template #default="{ row }">{{ fmtBytes(row.targetSize) }}</template></el-table-column>
            </el-table>
          </el-tab-pane>
          <el-tab-pane :label="`大小不一致 (${reconcile.sizeDiffCount})`" name="size">
            <el-table :data="reconcile.sizeMismatch" size="small" border stripe max-height="260">
              <el-table-column prop="relPath" label="相对路径" min-width="260" show-overflow-tooltip />
              <el-table-column label="源大小" width="110"><template #default="{ row }">{{ fmtBytes(row.sourceSize) }}</template></el-table-column>
              <el-table-column label="目标大小" width="110"><template #default="{ row }">{{ fmtBytes(row.targetSize) }}</template></el-table-column>
            </el-table>
          </el-tab-pane>
        </el-tabs>
      </template>
      <template v-if="reconcileHistory.length">
        <el-divider style="margin:10px 0" />
        <div style="font-size:13px;font-weight:600;margin-bottom:6px">对账历史</div>
        <el-table :data="reconcileHistory" size="small" border stripe max-height="220">
          <el-table-column prop="id" label="#" width="60" />
          <el-table-column prop="trigger_type" label="触发" width="90" />
          <el-table-column prop="source_count" label="源" width="70" />
          <el-table-column prop="target_count" label="目标" width="70" />
          <el-table-column prop="missing_count" label="缺" width="60" />
          <el-table-column prop="extra_count" label="多" width="60" />
          <el-table-column prop="size_diff_count" label="大小差" width="80" />
          <el-table-column prop="elapsed_ms" label="耗时" width="80" />
          <el-table-column prop="create_time" label="时间" min-width="160" />
          <el-table-column label="操作" width="80">
            <template #default="{ row }"><el-link type="primary" @click="showReconcileHistory(row)">查看</el-link></template>
          </el-table-column>
        </el-table>
      </template>
    </el-card>
  </div>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue'
import { useRouter } from 'vue-router'
import { ElMessage, ElMessageBox } from 'element-plus'
import { defineComponent, watch } from 'vue'
import request from '../../api/request'

const router = useRouter()
const id = Number(router.currentRoute.value.params.id)
const task = ref(null)
const runs = ref({ items: [], total: 0 })
const page = ref(1)
const pageSize = 10
const runsLoading = ref(false)

const RunFiles = defineComponent({
  name: 'RunFiles',
  props: { taskId: Number, runId: Number },
  setup(props) {
    const items = ref([])
    const loading = ref(false)
    watch([() => props.taskId, () => props.runId], async () => {
      if (!props.runId) return
      loading.value = true
      try {
        const r = await request.get(`/filesync/tasks/${props.taskId}/runs/${props.runId}/files`, { params: { page: 1, pageSize: 300 } })
        items.value = r.items || []
      } catch (e) {
        items.value = []
      } finally {
        loading.value = false
      }
    }, { immediate: true })
    return { items, loading }
  },
  template: `
    <div v-loading="loading" style="padding:4px 20px">
      <el-table :data="items" size="small" border max-height="280">
        <el-table-column prop="rel_path" label="文件(相对路径)" min-width="280" />
        <el-table-column label="大小" width="100">
          <template #default="{ row }">{{ row.size }}B</template>
        </el-table-column>
        <el-table-column label="结果" width="90">
          <template #default="{ row }">
            <el-tag size="small" :type="row.status === 'success' ? 'success' : 'danger'" effect="plain">{{ row.status === 'success' ? '成功' : '失败' }}</el-tag>
          </template>
        </el-table-column>
        <el-table-column prop="error_msg" label="错误信息" min-width="220" show-overflow-tooltip />
        <el-table-column prop="event_at" label="时间" width="165" />
      </el-table>
      <el-text v-if="!loading && !items.length" size="small" type="info">该轮无文件级事件(0 变化增量 / 全跳过)</el-text>
    </div>`
})

function parseSummary(row) {
  if (!row.last_run_summary) return null
  if (typeof row.last_run_summary === 'string') { try { return JSON.parse(row.last_run_summary) } catch (e) { return null } }
  return row.last_run_summary
}
const isWatch = computed(() => Number(task.value?.watch) === 1)
const summ = computed(() => (task.value ? parseSummary(task.value) : null))
const stratList = computed(() => {
  const s = task.value?.strategy || {}
  const out = []
  if (s.overwrite) out.push({ text: '同名覆盖', type: 'warning' })
  if (s.remove) out.push({ text: '删除目标多余文件', type: 'danger' })
  if (s.exclude && s.exclude.length) out.push({ text: `排除 ${s.exclude.length} 条规则`, type: '' })
  if (s.timeFilter) {
    const m = String(s.timeFilter.value || '').match(/^(\d+)([hd])$/)
    out.push({ text: `仅同步最近${m ? m[1] + (m[2] === 'h' ? '小时' : '天') : s.timeFilter.value}修改`, type: '' })
  }
  return out
})

async function load() {
  try {
    const t = await request.get(`/filesync/tasks/${id}`)
    task.value = { ...t, strategy: typeof t.strategy === 'string' ? JSON.parse(t.strategy) : t.strategy }
  } catch (e) {
    ElMessage.error('加载失败: ' + (e?.message || e))
  }
}
async function loadRuns(p = 1) {
  page.value = p
  runsLoading.value = true
  try {
    const r = await request.get(`/filesync/tasks/${id}/runs`, { params: { page: p, pageSize } })
    runs.value = {
      total: r.total,
      items: (r.items || []).map(x => ({ ...x, summary: typeof x.summary === 'string' ? JSON.parse(x.summary) : x.summary }))
    }
  } catch (e) {
    ElMessage.error('历史加载失败: ' + (e?.message || e))
  } finally {
    runsLoading.value = false
  }
}
const browsePath = ref([])
const browseItems = ref([])
const browseLoading = ref(false)
async function loadBrowse() {
  browseLoading.value = true
  try {
    const r = await request.get(`/filesync/tasks/${id}/browse`, { params: { path: browsePath.value.join('/') } })
    browseItems.value = r.items || []
  } catch (e) {
    ElMessage.error('浏览失败: ' + (e?.message || e))
  } finally {
    browseLoading.value = false
  }
}
function go(path) {
  browsePath.value = path ? path.split('/').filter(Boolean) : []
  loadBrowse()
}
async function reload() { load(); loadRuns(page.value) }
async function run() {
  try {
    const r = await request.post(`/filesync/tasks/${id}/run`)
    ElMessage.success(r?.watching ? '实时同步已启动(启动即全量一轮, 之后持续监听)' : '已触发一轮执行')
    setTimeout(reload, 3000)
  } catch (e) {
    ElMessage.error('执行失败: ' + (e?.message || e))
  }
}
async function stopWatch() {
  try {
    await request.post(`/filesync/tasks/${id}/stop`)
    ElMessage.success('已停止实时同步')
    setTimeout(reload, 2500)
  } catch (e) {
    ElMessage.error('停止失败: ' + (e?.message || e))
  }
}
async function remove() {
  await ElMessageBox.confirm(`删除任务「${task.value.name}」?(传输历史一并删除;目标桶已同步文件不受影响)`, '提示', { type: 'warning' })
  try {
    await request.delete(`/filesync/tasks/${id}`)
    ElMessage.success('已删除')
    router.push('/filesync/task')
  } catch (e) {
    ElMessage.error('删除失败: ' + (e?.message || e))
  }
}

const reconcile = ref(null)
const reconcileLoading = ref(false)
const reconcileTab = ref('missing')
const reconcileHistory = ref([])
async function doReconcile() {
  reconcileLoading.value = true
  try {
    reconcile.value = await request.post(`/filesync/tasks/${id}/reconcile`)
    ElMessage.success('对账完成')
    loadReconcileHistory()
  } catch (e) {
    ElMessage.error('对账失败: ' + (e?.message || e))
  } finally {
    reconcileLoading.value = false
  }
}
async function loadReconcileHistory() {
  try {
    reconcileHistory.value = (await request.get(`/filesync/tasks/${id}/reconciles`, { params: { limit: 10 } })) || []
  } catch (e) { /* 历史为空不打扰 */ }
}
function showReconcileHistory(row) {
  let d = {}
  try { d = JSON.parse(row.detail_json || '{}') } catch (e) { d = {} }
  reconcile.value = {
    sourceCount: row.source_count, targetCount: row.target_count,
    missingCount: row.missing_count, extraCount: row.extra_count, sizeDiffCount: row.size_diff_count,
    elapsedMs: row.elapsed_ms, truncated: !!row.truncated,
    missing: d.missing || [], extra: d.extra || [], sizeMismatch: d.sizeMismatch || []
  }
}

const statusName = s => ({ IDLE: '未执行', RUNNING: '执行中', COMPLETED: '已完成', FAILED: '失败', WATCHING: '实时同步中' }[s] || s || '—')
const statusType = s => ({ RUNNING: 'warning', COMPLETED: 'success', FAILED: 'danger', WATCHING: 'danger' }[s] || 'info')
const schedName = t => t.schedule_mode === 'interval' ? `每 ${t.interval_min || 5}min 自动` : t.schedule_mode === 'cron' ? `${t.cron_expr} 自动` : '仅手动'
function fmtBytes(b) {
  const n = Number(b || 0)
  if (n >= 1048576) return (n / 1048576).toFixed(1) + 'MB'
  if (n >= 1024) return (n / 1024).toFixed(1) + 'KB'
  return n + 'B'
}

onMounted(() => { load(); loadRuns(1); loadBrowse(); loadReconcileHistory() })
</script>

<style scoped>
.head { display: flex; justify-content: space-between; align-items: center; }
.title { font-size: 16px; font-weight: 600; margin: 0 10px; }
.sec-head { display: flex; align-items: center; justify-content: space-between; }
</style>
