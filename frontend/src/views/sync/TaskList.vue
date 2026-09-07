<template>
  <div class="page-card">
    <el-alert type="success" :closable="false" show-icon style="margin-bottom: 16px"
      title="同步任务 = 采集段(CDC → Kafka)+ 写段(攒批 ODKU → TiDB 单表,单表即权威);多张源表选入同一目标模型 = 多表合并(宽表)" />

    <el-card shadow="never">
      <div class="toolbar">
        <div>
          <el-radio-group v-model="filter" size="small">
            <el-radio-button value="">全部</el-radio-button>
            <el-radio-button value="运行中">运行中</el-radio-button>
            <el-radio-button value="已停止">已停止</el-radio-button>
          </el-radio-group>
        </div>
        <div>
          <el-button size="small" :loading="loading" @click="load">刷新</el-button>
          <el-button type="primary" @click="router.push('/sync/task/create')">＋ 新建任务</el-button>
        </div>
      </div>
      <el-table :data="filtered" stripe v-loading="loading">
        <el-table-column label="任务" min-width="160">
          <template #default="{ row }"><el-link type="primary" @click="router.push(`/sync/task/${row.logical_table}`)">{{ row.name }}</el-link></template>
        </el-table-column>
        <el-table-column label="源表(多源 = 合并)" min-width="200">
          <template #default="{ row }">
            <el-tag v-for="s in row.sources" :key="s" size="small" style="margin-right: 4px">{{ s }}</el-tag>
            <el-tag v-if="row.sources.length > 1" size="small" type="success" effect="plain">→ {{ row.logical_table }} 宽表</el-tag>
            <el-text v-else size="small" type="info">{{ row.logical_table }}</el-text>
          </template>
        </el-table-column>
        <el-table-column label="模式" width="100">
          <template #default="{ row }">
            <el-tag size="small" :type="modeType(row.sync_mode)" effect="plain">{{ modeName(row.sync_mode) }}</el-tag>
            <el-text v-if="row.sync_mode === 'scheduled-batch' && row.cron_expr" size="small" type="info" style="display:block;font-size:10px">{{ row.cron_expr }}</el-text>
          </template>
        </el-table-column>
        <el-table-column prop="status" label="状态" width="90">
          <template #default="{ row }">
            <el-tag size="small" :type="row.running ? 'success' : 'info'" effect="plain">{{ runStatusName(row) }}</el-tag>
          </template>
        </el-table-column>
        <el-table-column prop="create_time" label="创建时间" width="170" />
        <el-table-column label="操作" width="230">
          <template #default="{ row }">
            <el-link type="primary" underline="never" @click="router.push(`/sync/task/${row.logical_table}`)">详情</el-link>
            <el-link v-if="row.running" type="danger" underline="never" style="margin-left: 10px" @click="stop(row)">停止采集</el-link>
            <el-link v-else type="success" underline="never" style="margin-left: 10px" @click="start(row)">启动采集</el-link>
          </template>
        </el-table-column>
      </el-table>
      <div class="tip">启动采集 = 每个源表一个 CDC 任务: 首次全量预导 + binlog 增量;同逻辑表多源字段级收敛到同一宽表(ODKU + seq 裁判),写入 TiDB 单表即权威</div>
    </el-card>
  </div>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue'
import { useRouter } from 'vue-router'
import { ElMessage } from 'element-plus'
import { listSyncTasks, cdcStart, cdcStop } from '../../api'

const router = useRouter()
const filter = ref('')
const tasks = ref([])
const loading = ref(false)

async function load() {
  loading.value = true
  try {
    const data = await listSyncTasks()
    tasks.value = (data || []).map(t => ({
      id: t.id,
      name: t.name,
      logical_table: t.logical_table,
      logicalTable: t.logical_table,
      sources: t.sources || [],
      running: !!t.running,
      run_status: t.run_status,
      sync_mode: t.sync_mode,
      cron_expr: t.cron_expr,
      status: t.status,
      create_time: String(t.create_time || '').slice(0, 19)
    }))
  } catch (e) {
    ElMessage.error('任务列表加载失败: ' + (e?.message || e))
  } finally {
    loading.value = false
  }
}
onMounted(load)

function modeName(m) { return { 'full-once': '一次性全量', 'scheduled-batch': '定时批量(纯增量)', 'full-cdc': '全量+CDC', 'incremental': '仅增量', 'full-scheduled': '全量+定时批量' }[m] || '全量+CDC' }
function modeType(m) { return { 'full-once': 'warning', 'scheduled-batch': 'danger', 'full-cdc': 'success', 'incremental': 'primary', 'full-scheduled': 'danger' }[m] || 'success' }
function runStatusName(row) {
  if (row.running) return '运行中'
  if (row.run_status === 'COMPLETED') return '已完成'
  if (row.run_status === 'FAILED') return '失败'
  return '已停止'
}
const filtered = computed(() => tasks.value.filter(t => {
  if (!filter.value) return true
  if (filter.value === '运行中') return t.running
  return !t.running
}))

async function start(row) {
  try {
    const r = await cdcStart(row.logical_table)
    ElMessage.success(`已启动采集: ${(r.sources || []).join(' + ')} (${r.count} 个源表任务)`)
    await load()
  } catch (e) {
    ElMessage.error('启动失败: ' + (e?.message || e))
  }
}
async function stop(row) {
  try {
    await cdcStop(row.logical_table)
    ElMessage.success(`已停止采集: ${row.logical_table}`)
    await load()
  } catch (e) {
    ElMessage.error('停止失败: ' + (e?.message || e))
  }
}
</script>

<style scoped>
.tip { margin-top: 12px; font-size: 12px; color: #8f959e; }
</style>
