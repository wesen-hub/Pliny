<template>
  <div class="page-card">
    <el-card shadow="never">
      <div class="toolbar">
        <div style="font-weight: 600">
          湖表同步
          <el-text type="info" style="margin-left: 8px; font-weight: 400">
            任务配置式 · 一任务一表 · 数据获取统一走消费侧逻辑
          </el-text>
        </div>
        <el-button type="primary" @click="$router.push('/lakesync/create')">＋ 新建同步任务</el-button>
      </div>

      <div class="toolbar">
        <el-input v-model="keyword" placeholder="任务名 / 源表…" clearable style="width: 260px">
          <template #prefix><el-icon><Search /></el-icon></template>
        </el-input>
        <el-select v-model="statusFilter" placeholder="状态" clearable style="width: 140px">
          <el-option label="运行中" value="running" />
          <el-option label="已暂停" value="paused" />
          <el-option label="手动" value="manual" />
        </el-select>
        <el-button :loading="loading" @click="load">刷新</el-button>
      </div>

      <el-table :data="filtered" stripe v-loading="loading">
        <el-table-column label="任务(源表)" min-width="170">
          <template #default="{ row }">
            <el-link type="primary" @click="$router.push(`/lakesync/${row.id}`)">{{ row.logical_table }}</el-link>
          </template>
        </el-table-column>
        <el-table-column label="目标湖表" min-width="200">
          <template #default="{ row }"><span class="mono">{{ lakeTable(row) }}</span></template>
        </el-table-column>
        <el-table-column label="调度" width="150">
          <template #default="{ row }">{{ scheduleText(row) }}</template>
        </el-table-column>
        <el-table-column label="状态" width="110">
          <template #default="{ row }">
            <el-tag size="small" :type="statusTag(row)" effect="plain">{{ statusText(row) }}</el-tag>
          </template>
        </el-table-column>
        <el-table-column label="最近运行" width="165">
          <template #default="{ row }">{{ row.last_run_at || '—' }}</template>
        </el-table-column>
        <el-table-column label="位点滞后 / 追平" width="190">
          <template #default="{ row }">
            <span v-if="!lagOf(row)" class="muted" title="待消费侧绑定(平台自动生成消费方/契约后可见)">—</span>
            <span v-else>
              {{ lagOf(row).lagSeconds }}s
              <el-tag size="small" :type="lagOf(row).caughtUp ? 'success' : 'warning'" effect="plain">
                {{ lagOf(row).caughtUp ? '✅ 追平' : '⚠️ 滞后' }}
              </el-tag>
            </span>
          </template>
        </el-table-column>
        <el-table-column label="操作" width="230" fixed="right">
          <template #default="{ row }">
            <el-link type="primary" underline="never" @click="runNow(row)">运行</el-link>
            <el-link :type="row.enabled ? 'warning' : 'success'" underline="never" style="margin-left: 8px" @click="toggle(row)">
              {{ row.enabled ? '暂停' : '启用' }}
            </el-link>
            <el-link type="primary" underline="never" style="margin-left: 8px" @click="$router.push(`/lakesync/${row.id}/edit`)">编辑</el-link>
            <el-link type="danger" underline="never" style="margin-left: 8px" @click="remove(row)">删除</el-link>
          </template>
        </el-table-column>
      </el-table>

      <div class="hint">
        一任务一表:同一源表在“未停用任务”中唯一。位点(TSO)/区间/采集通道由消费侧自动绑定,页面只展示不配置。
      </div>
    </el-card>
  </div>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import { listLakeSyncTasks, deleteLakeSyncTask, updateLakeSyncTask, runLakeSyncTask, consumerMetricsQuiet } from '../../api'

const tasks = ref([])
const metrics = ref({})          // logical_table -> { lagSeconds, caughtUp }
const keyword = ref('')
const statusFilter = ref('')
const loading = ref(false)

const filtered = computed(() => tasks.value.filter(t => {
  const kw = keyword.value.trim()
  if (kw && !String(t.logical_table || '').includes(kw)) return false
  if (statusFilter.value === 'running' && !(t.enabled && t.status === 'running')) return false
  if (statusFilter.value === 'paused' && t.enabled) return false
  if (statusFilter.value === 'manual' && t.schedule !== 'manual') return false
  return true
}))

function lakeTable(row) {
  // TODO(接入消费侧): 目标湖表落库后直接读 row.lake_catalog/namespace/table
  return row.lake_table || `demo.dcp.${row.logical_table}`
}
function scheduleText(row) {
  if (row.schedule === 'cron') return `cron ${row.cron_expr || ''}`
  if (row.schedule === 'manual') return '手动'
  return `每 ${row.interval_min || 15} 分钟`
}
function statusText(row) {
  if (!row.enabled) return '已暂停'
  if (row.status === 'running') return '运行中'
  return row.status || '手动'
}
function statusTag(row) {
  if (!row.enabled) return 'info'
  return row.status === 'running' ? 'success' : 'warning'
}
function lagOf(row) { return metrics.value[row.logical_table] || null }

async function load() {
  loading.value = true
  try {
    tasks.value = (await listLakeSyncTasks()) || []
    // 指标:消费方命名约定 lake-{table}(待后端绑定后生效;失败静默为“—”)
    const names = tasks.value.map(t => `lake-${t.logical_table}`)
    const res = await Promise.allSettled(names.map(n => consumerMetricsQuiet(n, 1)))
    res.forEach((r, i) => {
      if (r.status !== 'fulfilled' || !r.value) return
      const s = (r.value.streams || [])[0]
      metrics.value[tasks.value[i].logical_table] = {
        lagSeconds: s?.lagSeconds ?? r.value.lagSeconds ?? '—',
        caughtUp: s?.caughtUp ?? r.value.caughtUp ?? false
      }
    })
  } finally {
    loading.value = false
  }
}

async function runNow(row) {
  await runLakeSyncTask(row.id)
  ElMessage.success('已触发运行')
  load()
}
async function toggle(row) {
  await updateLakeSyncTask(row.id, {
    schedule: row.schedule, intervalMin: row.interval_min, cronExpr: row.cron_expr,
    enabled: row.enabled ? 0 : 1, status: row.status || 'running'
  })
  ElMessage.success(row.enabled ? '已暂停' : '已启用')
  load()
}
async function remove(row) {
  await ElMessageBox.confirm(`删除任务「${row.logical_table}」?`, '确认', { type: 'warning' })
  await deleteLakeSyncTask(row.id)
  ElMessage.success('已删除')
  load()
}

onMounted(load)
</script>

<style scoped>
.mono { font-family: var(--el-font-family, monospace); font-size: 12.5px; }
.muted { color: #a8abb2; }
.hint { color: #909399; font-size: 12px; margin-top: 10px; }
</style>
