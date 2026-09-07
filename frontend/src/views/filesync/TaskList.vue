<template>
  <div class="page-card">
    <el-alert type="success" :closable="false" show-icon style="margin-bottom: 16px"
      title="文件同步(mc mirror 原生):任务 = 源文件数据源 → 目标 MinIO + mirror 同步策略 + 触发方式;执行 = mc mirror,增量内建,审计按轮(run/文件明细)" />

    <el-card shadow="never">
      <div class="toolbar">
        <div>
          <el-radio-group v-model="filter" size="small">
            <el-radio-button value="">全部</el-radio-button>
            <el-radio-button value="WATCHING">实时中</el-radio-button>
            <el-radio-button value="RUNNING">执行中</el-radio-button>
            <el-radio-button value="FAILED">失败</el-radio-button>
          </el-radio-group>
          <el-text size="small" type="info" style="margin-left:12px">自动刷新: <el-switch v-model="autoRefresh" size="small" /></el-text>
        </div>
        <div>
          <el-button size="small" :loading="loading" @click="load">刷新</el-button>
          <el-button type="warning" @click="router.push('/filesync/task/create')">＋ 新建文件同步任务</el-button>
        </div>
      </div>

      <el-table :data="filtered" stripe v-loading="loading">
        <el-table-column label="任务" min-width="180">
          <template #default="{ row }">
            <el-link type="primary" @click="router.push(`/filesync/task/${row.id}`)">{{ row.name }}</el-link>
            <el-tag :type="row.exec_host === 'agent' ? 'primary' : 'success'" size="small" effect="plain" style="margin-left:6px">
              {{ row.exec_host === 'agent' ? `Agent#${row.agent_id}` : '平台' }}
            </el-tag>
            <el-text v-if="row.exec_host === 'agent' && !row.online" size="small" type="danger" style="display:block;font-size:10px">Agent 离线(最后心跳滞后)</el-text>
          </template>
        </el-table-column>
        <el-table-column label="源 → 目标" min-width="230">
          <template #default="{ row }">
            <el-tag size="small" type="info" effect="plain">{{ row.source_ds_type || '?' }}</el-tag>
            <el-text size="small" style="margin-left:4px">{{ row.source_ds_name }}</el-text>
            <el-text size="small" type="info" style="display:block;font-size:11px">{{ row.source_path || '/(整源)' }} → {{ row.target_bucket }}{{ row.target_prefix ? '/' + row.target_prefix : '' }}</el-text>
          </template>
        </el-table-column>
        <el-table-column label="同步策略" min-width="180">
          <template #default="{ row }">
            <div v-if="row.strategy">
              <el-tag v-for="t in stratTags(row.strategy)" :key="t" size="small" effect="plain" :type="t.type" style="margin-right:4px">{{ t.text }}</el-tag>
              <el-text v-if="!stratTags(row.strategy).length" size="small" type="info">默认(补缺失增量)</el-text>
            </div>
            <el-text v-if="row.schedule_mode === 'interval'" size="small" type="info" style="display:block;font-size:10px">每 {{ row.interval_min }}min 自动</el-text>
            <el-text v-else-if="row.schedule_mode === 'cron'" size="small" type="info" style="display:block;font-size:10px">{{ row.cron_expr }} 自动</el-text>
            <el-text v-else size="small" type="info" style="display:block;font-size:10px">仅手动</el-text>
          </template>
        </el-table-column>
        <el-table-column label="状态" width="100">
          <template #default="{ row }">
            <el-tag size="small" :type="statusType(row.run_status)" effect="plain">{{ statusName(row.run_status) }}</el-tag>
          </template>
        </el-table-column>
        <el-table-column label="最近一轮" min-width="180">
          <template #default="{ row }">
            <template v-if="row.last_run_summary">
              <el-text size="small">{{ row.last_run_summary.transferred ?? 0 }} 文件 / {{ fmtBytes(row.last_run_summary.bytes) }}</el-text>
              <el-text v-if="row.last_run_summary.failed" size="small" type="danger" style="display:block">失败 {{ row.last_run_summary.failed }}</el-text>
            </template>
            <el-text v-else size="small" type="info">—</el-text>
            <el-text v-if="row.last_run_time" size="small" type="info" style="display:block;font-size:10px">{{ row.last_run_time }}</el-text>
          </template>
        </el-table-column>
        <el-table-column label="操作" width="190">
          <template #default="{ row }">
            <el-link type="primary" underline="never" @click="router.push(`/filesync/task/${row.id}`)">详情</el-link>
            <template v-if="Number(row.watch) === 1">
              <el-link v-if="row.watching || row.run_status === 'WATCHING'" type="danger" underline="never" style="margin-left:8px" @click="stopWatch(row)">停止实时</el-link>
              <el-link v-else type="success" underline="never" style="margin-left:8px" @click="run(row)">启动实时</el-link>
            </template>
            <el-link v-else type="success" underline="never" style="margin-left:8px" :disabled="row.run_status === 'RUNNING'" @click="run(row)">执行</el-link>
            <el-link type="danger" underline="never" style="margin-left:8px" @click="remove(row)">删除</el-link>
          </template>
        </el-table-column>
      </el-table>
      <div class="tip">执行 = 触发一轮 mc mirror(增量内建:补缺失/overwrite 覆盖变更/0 变化跳过);agent 任务由 Agent 在业务机本地执行并回传</div>
    </el-card>
  </div>
</template>

<script setup>
import { ref, computed, onMounted, onUnmounted } from 'vue'
import { useRouter } from 'vue-router'
import { ElMessage, ElMessageBox } from 'element-plus'
import request from '../../api/request'

const router = useRouter()
const filter = ref('')
const autoRefresh = ref(true)
const loading = ref(false)
const tasks = ref([])
let timer = null

function parseSummary(row) {
  if (!row.last_run_summary) return null
  if (typeof row.last_run_summary === 'string') {
    try { return JSON.parse(row.last_run_summary) } catch (e) { return null }
  }
  return row.last_run_summary
}

async function load() {
  loading.value = true
  try {
    tasks.value = ((await request.get('/filesync/tasks')) || []).map(t => ({ ...t, last_run_summary: parseSummary(t) }))
  } catch (e) {
    ElMessage.error('加载失败: ' + (e?.message || e))
  } finally {
    loading.value = false
  }
}
function startAuto() {
  stopAuto()
  timer = setInterval(() => { if (autoRefresh.value) load() }, 6000)
}
function stopAuto() { if (timer) { clearInterval(timer); timer = null } }
onMounted(() => { load(); startAuto() })
onUnmounted(stopAuto)

const filtered = computed(() => tasks.value.filter(t => !filter.value || t.run_status === filter.value))

const statusName = s => ({ IDLE: '未执行', RUNNING: '执行中', COMPLETED: '已完成', FAILED: '失败', WATCHING: '实时同步中' }[s] || s || '未执行')
const statusType = s => ({ RUNNING: 'warning', COMPLETED: 'success', FAILED: 'danger', WATCHING: 'danger' }[s] || 'info')

function stratTags(st) {
  const out = []
  if (st.overwrite) out.push({ text: '同名覆盖', type: 'warning' })
  if (st.remove) out.push({ text: '删除多余', type: 'danger' })
  if (st.exclude && st.exclude.length) out.push({ text: `排除×${st.exclude.length}`, type: '' })
  if (st.timeFilter) {
    const m = String(st.timeFilter.value || '').match(/^(\d+)([hd])$/)
    out.push({ text: `仅同步最近${m ? m[1] + (m[2] === 'h' ? '小时' : '天') : st.timeFilter.value}修改`, type: '' })
  }
  if (st.dryRun) out.push({ text: '演练', type: 'info' })
  return out
}
function fmtBytes(b) {
  const n = Number(b || 0)
  if (n >= 1048576) return (n / 1048576).toFixed(1) + 'MB'
  if (n >= 1024) return (n / 1024).toFixed(1) + 'KB'
  return n + 'B'
}

async function run(row) {
  try {
    const r = await request.post(`/filesync/tasks/${row.id}/run`)
    if (r?.watching) ElMessage.success('实时同步已启动(启动即全量一轮,之后持续监听)')
    else if (r?.online === false) ElMessage.warning('Agent 当前离线:执行指令已入队,Agent 重连后会自动执行')
    else ElMessage.success(r?.started ? (row.exec_host === 'agent' ? '已下发执行指令给 Agent' : '已开始执行(异步)') : '任务已在执行中')
    setTimeout(load, 2500)
  } catch (e) {
    ElMessage.error('执行失败: ' + (e?.message || e))
  }
}
async function stopWatch(row) {
  try {
    const r = await request.post(`/filesync/tasks/${row.id}/stop`)
    ElMessage.success(r?.dispatchedToAgent ? '已下发停止指令给 Agent' : '已停止实时同步')
    setTimeout(load, 2000)
  } catch (e) {
    ElMessage.error('停止失败: ' + (e?.message || e))
  }
}
async function remove(row) {
  await ElMessageBox.confirm(`删除任务「${row.name}」?(传输历史一并删除;目标桶已同步文件不受影响${row.exec_host === 'agent' ? '; 会向 Agent 下发删除指令' : ''})`, '提示', { type: 'warning' })
  try {
    await request.delete(`/filesync/tasks/${row.id}`)
    ElMessage.success('已删除')
    await load()
  } catch (e) {
    ElMessage.error('删除失败: ' + (e?.message || e))
  }
}
</script>

<style scoped>
.toolbar { display: flex; justify-content: space-between; margin-bottom: 14px; align-items: center; }
.tip { margin-top: 12px; font-size: 12px; color: #8f959e; }
</style>
