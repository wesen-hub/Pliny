<template>
  <div class="page-card" v-loading="loading">
    <el-card shadow="never">
      <div class="toolbar">
        <div style="font-weight: 600; display: flex; align-items: center; gap: 10px">
          {{ task.logical_table || '任务详情' }}
          <el-tag size="small" :type="statusTag" effect="plain">{{ statusText }}</el-tag>
          <el-text type="info" style="font-weight: 400">一任务一表 · 数据获取统一走消费侧</el-text>
        </div>
        <div>
          <el-button @click="runNow">立即运行</el-button>
          <el-button @click="toggle" :type="task.enabled ? 'warning' : 'success'" plain>{{ task.enabled ? '暂停' : '启用' }}</el-button>
          <el-button @click="$router.push(`/lakesync/${task.id}/edit`)">编辑</el-button>
          <el-button @click="$router.push('/lakesync/list')">返回列表</el-button>
        </div>
      </div>

      <el-descriptions :column="4" border size="small">
        <el-descriptions-item label="目标湖表">{{ lakeTable }}</el-descriptions-item>
        <el-descriptions-item label="调度">{{ scheduleText }}</el-descriptions-item>
        <el-descriptions-item label="最近运行">{{ task.last_run_at || '—' }}</el-descriptions-item>
        <el-descriptions-item label="消费方">{{ consumerId }}</el-descriptions-item>
      </el-descriptions>
    </el-card>

    <!-- 消费指标(现场读, 不依赖 Prometheus) -->
    <el-card shadow="never" style="margin-top: 14px">
      <template #header>
        <div style="display: flex; align-items: center; justify-content: space-between">
          <span>消费指标 <el-text type="info">现场读,不依赖 Prometheus</el-text></span>
          <el-button size="small" :loading="metricsLoading" @click="loadMetrics">刷新指标</el-button>
        </div>
      </template>

      <el-alert v-if="!metrics" type="info" :closable="false" show-icon
        title="暂无指标:消费方/契约尚未绑定(接入消费侧后自动生成,命名约定 lake-{源表})" />

      <template v-else>
        <el-row :gutter="12">
          <el-col :span="4"><div class="metric"><span>位点(TSO)</span><b class="mono">{{ short(metrics.positionTso) }}</b></div></el-col>
          <el-col :span="4"><div class="metric"><span>水位(WATERMARK)</span><b class="mono">{{ short(metrics.watermarkTso) }}</b></div></el-col>
          <el-col :span="4"><div class="metric"><span>滞后</span><b :class="metrics.caughtUp ? 'ok' : 'warn'">{{ metrics.lagSeconds }}s</b></div></el-col>
          <el-col :span="4"><div class="metric"><span>追平</span><b :class="metrics.caughtUp ? 'ok' : 'warn'">{{ metrics.caughtUp ? '✅ true' : '⚠️ false' }}</b></div></el-col>
          <el-col :span="4"><div class="metric"><span>快照存量</span><b>{{ snapshotText }}</b></div></el-col>
          <el-col :span="4"><div class="metric"><span>告警</span><b :class="alertLevel === 'OK' ? 'ok' : 'warn'">{{ alertLevel }}</b></div></el-col>
        </el-row>

        <div class="sub-title">最近批次(执行器 B)</div>
        <el-table :data="metrics.recent || []" size="small" stripe>
          <el-table-column prop="jobType" label="类型" width="120" />
          <el-table-column label="区间" min-width="260">
            <template #default="{ row }"><span class="mono">{{ short(row.loTso) }} → {{ short(row.hiTso) }}</span></template>
          </el-table-column>
          <el-table-column prop="rows" label="行数" width="80" />
          <el-table-column label="耗时" width="100">
            <template #default="{ row }">{{ row.durationSeconds }}s</template>
          </el-table-column>
          <el-table-column prop="result" label="结果" width="90">
            <template #default="{ row }">
              <el-tag size="small" :type="row.result === 'ok' ? 'success' : (row.result === 'fail' ? 'danger' : 'info')" effect="plain">{{ row.result }}</el-tag>
            </template>
          </el-table-column>
          <el-table-column prop="note" label="备注" min-width="160" show-overflow-tooltip />
        </el-table>
      </template>
    </el-card>

    <!-- 运行历史 -->
    <el-card shadow="never" style="margin-top: 14px">
      <template #header>
        <div style="display: flex; justify-content: space-between; align-items: center">
          <span>运行历史</span>
          <el-button size="small" @click="loadRuns">刷新</el-button>
        </div>
      </template>
      <el-table :data="runs" size="small" stripe>
        <el-table-column prop="id" label="ID" width="70" />
        <el-table-column prop="trigger_type" label="触发" width="90" />
        <el-table-column prop="status" label="状态" width="110">
          <template #default="{ row }">
            <el-tag size="small" :type="runTag(row.status)" effect="plain">{{ row.status }}</el-tag>
          </template>
        </el-table-column>
        <el-table-column label="位点/游标" min-width="220">
          <template #default="{ row }">
            <span class="mono">{{ short(row.cursor_before) }} → {{ short(row.cursor_after) }}</span>
          </template>
        </el-table-column>
        <el-table-column prop="exit_code" label="退出码" width="80" />
        <el-table-column prop="started_at" label="开始" width="165" />
        <el-table-column prop="finished_at" label="结束" width="165" />
        <el-table-column prop="error_msg" label="错误" min-width="180" show-overflow-tooltip />
        <el-table-column label="提交命令" min-width="220">
          <template #default="{ row }"><span class="mono">{{ row.submit_cmd }}</span></template>
        </el-table-column>
      </el-table>
    </el-card>
  </div>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue'
import { useRoute } from 'vue-router'
import { ElMessage, ElMessageBox } from 'element-plus'
import { getLakeSyncTask, listLakeSyncRuns, runLakeSyncTask, updateLakeSyncTask, consumerMetrics } from '../../api'

const route = useRoute()
const taskId = route.params.id
const task = ref({})
const runs = ref([])
const metrics = ref(null)
const loading = ref(false)
const metricsLoading = ref(false)

const consumerId = computed(() => `lake-${task.value.logical_table || ''}`)
const lakeTable = computed(() => task.value.lake_table
  ? `${task.value.lake_catalog || 'demo'}.${task.value.lake_namespace || 'dcp'}.${task.value.lake_table}`
  : `demo.dcp.${task.value.logical_table || '—'}(默认)`)
const statusText = computed(() => !task.value.enabled ? '已暂停' : (task.value.status === 'running' ? '运行中' : (task.value.status || '手动')))
const statusTag = computed(() => !task.value.enabled ? 'info' : (task.value.status === 'running' ? 'success' : 'warning'))
const scheduleText = computed(() => task.value.schedule === 'cron' ? `cron ${task.value.cron_expr || ''}`
  : task.value.schedule === 'manual' ? '手动' : `每 ${task.value.interval_min || 15} 分钟`)
const snapshotText = computed(() => {
  const s = metrics.value?.snapshotJobs || {}
  const parts = Object.entries(s).map(([k, v]) => `${k} ${v}`)
  return parts.length ? parts.join(' / ') : '—'
})
const alertLevel = computed(() => metrics.value?.alertLevel || 'OK')

function short(v) { const s = String(v ?? '—'); return s.length > 9 ? '…' + s.slice(-9) : s }
function runTag(s) { return s === 'succeeded' ? 'success' : s === 'failed' ? 'danger' : s === 'running' ? 'warning' : 'info' }

async function load() {
  loading.value = true
  try {
    task.value = await getLakeSyncTask(taskId)
    await Promise.all([loadRuns(), loadMetrics()])
  } finally {
    loading.value = false
  }
}
async function loadRuns() {
  try { runs.value = (await listLakeSyncRuns(taskId)) || [] } catch (e) { runs.value = [] }
}
async function loadMetrics() {
  metricsLoading.value = true
  try {
    const m = await consumerMetrics(consumerId.value, 10)
    const s = (m?.streams || [])[0]
    metrics.value = m ? {
      positionTso: s?.positionTso, watermarkTso: s?.watermarkTso,
      lagSeconds: s?.lagSeconds ?? '—', caughtUp: !!(s?.caughtUp ?? m.caughtUp),
      recent: (m?.batch?.recent || []).map(r => ({ ...r, loTso: r.loTso, hiTso: r.hiTso })),
      snapshotJobs: m?.snapshotJobs, alertLevel: m?.alertLevel
    } : null
  } catch (e) {
    metrics.value = null   // 未绑定消费方时静默(接口 404/无数据)
  } finally {
    metricsLoading.value = false
  }
}
async function runNow() {
  await runLakeSyncTask(taskId)
  ElMessage.success('已触发运行')
  loadRuns()
}
async function toggle() {
  await updateLakeSyncTask(taskId, {
    schedule: task.value.schedule, intervalMin: task.value.interval_min,
    cronExpr: task.value.cron_expr, enabled: task.value.enabled ? 0 : 1,
    status: task.value.status || 'running'
  })
  ElMessage.success(task.value.enabled ? '已暂停' : '已启用')
  load()
}

onMounted(load)
</script>

<style scoped>
.mono { font-family: monospace; font-size: 12px; }
.metric { background: #fafafa; border: 1px solid #ebeef5; border-radius: 8px; padding: 10px; margin-bottom: 12px; }
.metric span { display: block; color: #909399; font-size: 12px; margin-bottom: 4px; }
.metric b { font-size: 13.5px; }
.ok { color: #67c23a; }
.warn { color: #e6a23c; }
.sub-title { font-weight: 600; font-size: 13px; margin: 14px 0 8px; }
</style>
