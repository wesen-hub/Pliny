<template>
  <div v-if="task" class="page-card">
    <div class="head">
      <div>
        <el-button size="small" @click="router.push('/filesync/task')">← 返回</el-button>
        <span class="title">{{ task.name }}</span>
        <el-tag size="small" :type="task.exec_host === 'agent' ? 'primary' : 'success'" effect="plain">{{ task.exec_host === 'agent' ? `Agent#${task.agent_id}` : '平台执行' }}</el-tag>
        <el-tag v-if="task.exec_host === 'agent' && !task.online" size="small" type="danger">Agent 离线</el-tag>
        <el-tag size="small" :type="modeType(task.sync_mode)" effect="plain">{{ modeName(task.sync_mode) }}</el-tag>
        <el-tag size="small" :type="statusType(task.run_status)" effect="plain">{{ statusName(task.run_status) }}</el-tag>
      </div>
      <div>
        <el-button size="small" @click="reload">刷新</el-button>
        <el-button size="small" @click="router.push(`/filesync/task/${id}/edit`)">编辑</el-button>
        <el-button type="success" size="small" :disabled="task.run_status === 'RUNNING'" @click="run">执行一轮</el-button>
        <el-button type="danger" size="small" plain @click="remove">删除任务</el-button>
      </div>
    </div>

    <el-descriptions :column="3" border size="small" style="margin-top:10px">
      <el-descriptions-item label="源数据源">
        {{ task.source_ds_name || `#${task.source_datasource_id}` }}
        <el-tag size="small" type="info" effect="plain">{{ task.source_ds_type || '?' }}</el-tag>
      </el-descriptions-item>
      <el-descriptions-item label="源路径">{{ task.source_path || '(整源)' }}</el-descriptions-item>
      <el-descriptions-item label="执行端">{{ task.exec_host === 'agent' ? `Agent#${task.agent_id}` : '平台(容器 rclone)' }}</el-descriptions-item>
      <el-descriptions-item label="目标">
        {{ task.target_ds_name ? task.target_ds_name + ' · ' : '' }}s3://{{ task.target_bucket }}{{ task.target_prefix ? '/' + task.target_prefix : '' }}
      </el-descriptions-item>
      <el-descriptions-item label="同步模式">
        {{ modeName(task.sync_mode) }}
        <el-tag v-if="task.sync_mode === 'full_once'" size="small" type="info" effect="plain">跑完即止</el-tag>
        <el-tag v-else-if="task.baseline_done" size="small" type="success" effect="plain">基线已建</el-tag>
        <el-tag v-else size="small" type="warning" effect="plain">待建基线</el-tag>
      </el-descriptions-item>
      <el-descriptions-item label="调度">{{ schedName(task) }}</el-descriptions-item>
      <el-descriptions-item label="最近一轮" :span="3">
        <template v-if="summ">
          <el-tag size="small" effect="plain" style="margin-right:6px">{{ opName(summ.op) }}</el-tag>
          传输 {{ summ.transferred ?? 0 }} 文件 / {{ fmtBytes(summ.bytes) }}
          <el-text v-if="summ.deleted" type="info" style="margin-left:6px">删除 {{ summ.deleted }}</el-text>
          <el-text v-if="summ.failed" type="danger" style="margin-left:6px">失败 {{ summ.failed }}</el-text>
          <el-text v-if="summ.checks != null" type="info" style="margin-left:6px;font-size:11px">比对 {{ summ.checks }} / 清单 {{ summ.listed ?? '—' }}</el-text>
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
      <el-text v-else size="small" type="info">默认策略:rclone 默认口径(比较=大小+修改时间,覆盖=不同即覆盖)</el-text>
    </el-card>

    <el-tabs v-model="activeTab" style="margin-top:12px" @tab-change="onTabChange">
      <el-tab-pane label="传输历史" name="history">
        <div class="sec-head">
          <b>传输历史(每轮 = 一次 rclone sync/copy)</b>
          <el-text size="small" type="info">共 {{ runs.total }} 轮 · 目标桶即当前状态,轮次文件明细用于审计</el-text>
        </div>
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
        <el-table-column prop="executor" label="执行端" min-width="100" />
        <el-table-column label="动作" width="80">
          <template #default="{ row }">
            <el-tag size="small" effect="plain">{{ opName(row.summary?.op) }}</el-tag>
          </template>
        </el-table-column>
        <el-table-column label="状态" width="100">
          <template #default="{ row }">
            <el-tag size="small" :type="statusType(row.status)" effect="plain">{{ statusName(row.status) }}</el-tag>
          </template>
        </el-table-column>
        <el-table-column label="transferred" width="100">
          <template #default="{ row }">{{ row.summary?.transferred ?? 0 }}</template>
        </el-table-column>
        <el-table-column label="deleted" width="80">
          <template #default="{ row }">
            <el-text v-if="row.summary?.deleted" type="info">{{ row.summary.deleted }}</el-text>
            <span v-else>0</span>
          </template>
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
        <el-table-column label="checks/listed" width="110">
          <template #default="{ row }">
            <el-text size="small" type="info">{{ row.summary?.checks ?? '—' }} / {{ row.summary?.listed ?? '—' }}</el-text>
          </template>
        </el-table-column>
        <el-table-column label="耗时" width="85">
          <template #default="{ row }">{{ row.summary?.durationMs ? (row.summary.durationMs / 1000).toFixed(1) + 's' : '—' }}</template>
        </el-table-column>
        <el-table-column prop="error_msg" label="错误" min-width="140" show-overflow-tooltip />
      </el-table>
      <el-pagination style="margin-top:10px" background layout="total, prev, pager, next" :total="runs.total"
        :page-size="pageSize" :current-page="page" @current-change="loadRuns" />
      </el-tab-pane>

      <el-tab-pane label="目标浏览" name="browse">
        <div class="sec-head">
          <b>目标浏览(实时)</b>
          <el-text size="small" type="info">直接从目标桶 rclone lsjson 读取,当前状态以这里为准</el-text>
        </div>
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
      </el-tab-pane>

      <el-tab-pane label="一致性对账" name="reconcile">
        <div class="sec-head">
          <b>一致性对账</b>
          <el-text size="small" type="info">只报告不修改: 源/目标清单递归比对(rclone lsjson)</el-text>
        </div>
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
      </el-tab-pane>
    </el-tabs>
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
const activeTab = ref('history')
const browseLoaded = ref(false)
const reconcileLoaded = ref(false)
function onTabChange(name) {
  if (name === 'browse' && !browseLoaded.value) loadBrowse()
  if (name === 'reconcile' && !reconcileLoaded.value) loadReconcileHistory()
}

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
            <el-tag size="small" :type="row.status === 'success' ? 'success' : row.status === 'deleted' ? 'info' : 'danger'" effect="plain">{{ row.status === 'success' ? '已传输' : row.status === 'deleted' ? '已删除' : '失败' }}</el-tag>
          </template>
        </el-table-column>
        <el-table-column prop="error_msg" label="错误信息" min-width="220" show-overflow-tooltip />
        <el-table-column prop="event_at" label="时间" width="165" />
      </el-table>
      <el-text v-if="!loading && !items.length" size="small" type="info">该轮无文件级事件(0 变化增量 / 全部未变化)</el-text>
    </div>`
})

function parseSummary(row) {
  if (!row.last_run_summary) return null
  if (typeof row.last_run_summary === 'string') { try { return JSON.parse(row.last_run_summary) } catch (e) { return null } }
  return row.last_run_summary
}
const summ = computed(() => (task.value ? parseSummary(task.value) : null))
const stratList = computed(() => {
  const s = task.value?.strategy || {}
  const out = []
  if (s.compare === 'checksum') out.push({ text: '校验=校验和', type: '' })
  else if (s.compare === 'size_only') out.push({ text: '校验=仅大小', type: '' })
  if (s.updateMode === 'update') out.push({ text: '覆盖=只搬更新', type: 'warning' })
  else if (s.updateMode === 'ignore_existing') out.push({ text: '覆盖=跳过已存在', type: 'warning' })
  if (s.deleteExtras === true) out.push({ text: '删除=删除目标多余', type: 'danger' })
  else if (s.deleteExtras === false) out.push({ text: '删除=保留目标多余', type: '' })
  if (s.includes?.length) out.push({ text: `包含×${s.includes.length}`, type: '' })
  if (s.excludes?.length) out.push({ text: `排除×${s.excludes.length}`, type: 'warning' })
  if (s.minAge) out.push({ text: `仅最近${s.minAge}`, type: '' })
  if (s.maxAge) out.push({ text: `截至${s.maxAge}`, type: '' })
  if (s.minSize) out.push({ text: `最小${s.minSize}`, type: '' })
  if (s.maxSize) out.push({ text: `最大${s.maxSize}`, type: '' })
  if (s.transfers || s.checkers) out.push({ text: `并发${s.transfers ?? 8}/${s.checkers ?? 8}`, type: 'info' })
  if (s.bwlimit) out.push({ text: `限速${s.bwlimit}`, type: 'info' })
  if (s.dryRun) out.push({ text: '试运行', type: 'info' })
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
  browseLoaded.value = true
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
    if (r?.online === false) ElMessage.warning('Agent 当前离线:执行指令已入队,Agent 重连后会自动执行')
    else ElMessage.success('已触发一轮执行(rclone 无常驻会话,跑完即止)')
    setTimeout(reload, 3000)
  } catch (e) {
    ElMessage.error('执行失败: ' + (e?.message || e))
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
  reconcileLoaded.value = true
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

const statusName = s => ({ IDLE: '未执行', RUNNING: '执行中', COMPLETED: '已完成', FAILED: '失败' }[s] || s || '—')
const statusType = s => ({ RUNNING: 'warning', COMPLETED: 'success', FAILED: 'danger' }[s] || 'info')
const modeName = m => ({ full_then_incr: '全量+定时增量', full_once: '一次性全量', incr_scheduled: '仅定时增量' }[m] || m || '全量+定时增量')
const modeType = m => ({ full_then_incr: 'success', full_once: 'warning', incr_scheduled: 'primary' }[m] || 'info')
const opName = o => ({ sync: '全量(sync)', copy: '增量(copy)' }[o] || (o ? o : 'rclone'))
const schedName = t => {
  if (t.sync_mode === 'full_once') return '无需调度(一次性全量,跑完即止)'
  if (t.schedule_mode === 'interval') return `每 ${t.interval_min || 5}min 自动`
  if (t.schedule_mode === 'cron') return `${t.cron_expr} 自动`
  return '仅手动'
}
function fmtBytes(b) {
  const n = Number(b || 0)
  if (n >= 1048576) return (n / 1048576).toFixed(1) + 'MB'
  if (n >= 1024) return (n / 1024).toFixed(1) + 'KB'
  return n + 'B'
}

onMounted(() => { load(); loadRuns(1) })
</script>

<style scoped>
.head { display: flex; justify-content: space-between; align-items: center; }
.title { font-size: 16px; font-weight: 600; margin: 0 10px; }
.sec-head { display: flex; align-items: center; justify-content: space-between; }
</style>
