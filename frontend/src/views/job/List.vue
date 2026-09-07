<template>
  <div class="page-card">
    <el-card shadow="never">
      <div class="toolbar">
        <div style="display: flex; gap: 8px; flex-wrap: wrap">
          <el-input v-model="query.keyword" placeholder="搜索任务名" clearable style="width: 180px" @keyup.enter="load" />
          <el-select v-model="query.status" placeholder="状态" clearable style="width: 120px" @change="load">
            <el-option v-for="s in statusOptions" :key="s.value" :label="s.label" :value="s.value" />
          </el-select>
          <el-select v-model="query.syncMode" placeholder="同步模式" clearable style="width: 120px" @change="load">
            <el-option label="全量" value="full" />
            <el-option label="增量" value="incremental" />
            <el-option label="CDC" value="cdc" />
          </el-select>
          <el-button type="primary" @click="load"><el-icon><Search /></el-icon>查询</el-button>
        </div>
        <el-button v-if="userStore.canWrite" type="success" @click="router.push('/job/create')">
          <el-icon><Plus /></el-icon>新建任务
        </el-button>
      </div>

      <el-table :data="rows" v-loading="loading" stripe>
        <el-table-column prop="id" label="ID" width="60" />
        <el-table-column prop="jobName" label="任务名" min-width="160" show-overflow-tooltip />
        <el-table-column label="同步模式" width="90">
          <template #default="{ row }">
            <el-tag size="small" :type="modeTag(row.syncMode)">{{ modeLabel(row.syncMode) }}</el-tag>
          </template>
        </el-table-column>
        <el-table-column prop="targetTableName" label="目标表" width="120" />
        <el-table-column label="调度" width="140">
          <template #default="{ row }">
            <el-tag v-if="row.cronExpr" size="small" type="info">{{ row.cronExpr }}</el-tag>
            <span v-else style="color: #909399">仅手动</span>
          </template>
        </el-table-column>
        <el-table-column label="状态" width="100">
          <template #default="{ row }">
            <el-tag :type="statusTag(row.status)" size="small">{{ row.status }}</el-tag>
          </template>
        </el-table-column>
        <el-table-column label="启停" width="80">
          <template #default="{ row }">
            <el-switch :model-value="row.enabled === 1" :disabled="!userStore.canWrite"
                       @change="v => toggle(row, v)" />
          </template>
        </el-table-column>
        <el-table-column label="最近执行" width="160">
          <template #default="{ row }">
            <div>{{ row.lastExecutionTime || '-' }}</div>
            <el-tag v-if="row.lastExecutionStatus" size="small" :type="statusTag(row.lastExecutionStatus)">
              {{ row.lastExecutionStatus }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column label="操作" width="300" fixed="right">
          <template #default="{ row }">
            <el-button v-if="row.status !== 'RUNNING'" size="small" type="success" :disabled="!userStore.canWrite"
                       @click="run(row)">立即执行</el-button>
            <el-button v-else size="small" type="danger" :disabled="!userStore.canWrite" @click="stop(row)">停止</el-button>
            <el-button size="small" @click="viewOffset(row)">位点</el-button>
            <el-button size="small" type="primary" @click="router.push(`/job/${row.id}/edit`)">编辑</el-button>
            <el-button size="small" type="warning" :disabled="!userStore.canWrite" @click="copy(row)">复制</el-button>
            <el-button size="small" type="danger" :disabled="!userStore.canWrite" @click="remove(row)">删除</el-button>
          </template>
        </el-table-column>
      </el-table>

      <div class="pagination">
        <el-pagination background layout="total, prev, pager, next" :total="total"
                       :page-size="query.pageSize" :current-page="query.page" @current-change="p => { query.page = p; load() }" />
      </div>
    </el-card>

    <!-- 位点对话框 -->
    <el-dialog v-model="offsetDialog" title="增量位点" width="440px">
      <template v-if="offset">
        <el-descriptions :column="1" border>
          <el-descriptions-item label="上次同步位点">{{ offset.lastSyncValue ?? '(未初始化, 首次将全量)' }}</el-descriptions-item>
          <el-descriptions-item label="位点类型">{{ offset.valueType }}</el-descriptions-item>
          <el-descriptions-item label="更新时间">{{ offset.updateTime }}</el-descriptions-item>
        </el-descriptions>
        <el-form style="margin-top: 14px" label-width="90px">
          <el-form-item label="重置为">
            <el-input v-model="resetValue" placeholder="留空 = 回退全量" />
          </el-form-item>
          <el-form-item label="类型">
            <el-select v-model="resetType" style="width: 100%">
              <el-option label="LONG" value="LONG" />
              <el-option label="DATETIME" value="DATETIME" />
              <el-option label="STRING" value="STRING" />
            </el-select>
          </el-form-item>
        </el-form>
      </template>
      <el-empty v-else description="尚无位点记录 (首次执行将全量同步)" />
      <template #footer>
        <el-button @click="offsetDialog = false">关闭</el-button>
        <el-button v-if="userStore.canWrite" type="primary" @click="saveOffset">保存位点</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup>
import { onMounted, reactive, ref } from 'vue'
import { useRouter } from 'vue-router'
import { ElMessage, ElMessageBox } from 'element-plus'
import { listJobs, runJob, stopJob, enableJob, disableJob, copyJob, deleteJob, getSyncState, resetSyncState } from '../../api'
import { useUserStore } from '../../store/user'

const router = useRouter()
const userStore = useUserStore()
const rows = ref([])
const total = ref(0)
const loading = ref(false)
const query = reactive({ page: 1, pageSize: 10, keyword: '', status: '', syncMode: '' })
const statusOptions = [
  { value: 'READY', label: 'READY' }, { value: 'RUNNING', label: 'RUNNING' },
  { value: 'SUCCESS', label: 'SUCCESS' }, { value: 'FAILED', label: 'FAILED' }, { value: 'STOPPED', label: 'STOPPED' }
]
const offsetDialog = ref(false)
const offset = ref(null)
const currentJob = ref(null)
const resetValue = ref('')
const resetType = ref('LONG')

function modeLabel(m) { return { full: '全量', incremental: '增量', cdc: 'CDC' }[m] || m }
function modeTag(m) { return { full: 'info', incremental: 'warning', cdc: 'danger' }[m] || 'info' }
function statusTag(s) {
  return { RUNNING: 'warning', SUCCESS: 'success', FAILED: 'danger', STOPPED: 'info', READY: 'primary' }[s] || 'info'
}

async function load() {
  loading.value = true
  try {
    const data = await listJobs(query)
    rows.value = data.list
    total.value = data.total
  } finally {
    loading.value = false
  }
}

async function run(row) {
  await ElMessageBox.confirm(`确定立即执行任务「${row.jobName}」?`, '执行确认', { type: 'info' })
  try {
    const data = await runJob(row.id)
    ElMessage.success(`已触发, 执行ID: ${data.executionId}`)
    setTimeout(load, 3000)
  } catch (e) { /* ignore */ }
}

async function stop(row) {
  try {
    await stopJob(row.id)
    ElMessage.success('已发送停止指令')
    setTimeout(load, 2000)
  } catch (e) { /* ignore */ }
}

async function toggle(row, v) {
  v ? await enableJob(row.id) : await disableJob(row.id)
  ElMessage.success(v ? '已启用调度' : '已禁用调度')
  load()
}

async function copy(row) {
  try {
    const id = await copyJob(row.id)
    ElMessage.success('已复制, 副本默认禁用')
    router.push(`/job/${id}/edit`)
  } catch (e) { /* ignore */ }
}

async function remove(row) {
  await ElMessageBox.confirm(`确定删除任务「${row.jobName}」吗?`, '提示', { type: 'warning' })
  try {
    await deleteJob(row.id)
    ElMessage.success('已删除')
    load()
  } catch (e) { /* ignore */ }
}

async function viewOffset(row) {
  currentJob.value = row
  resetValue.value = ''
  offset.value = await getSyncState(row.id)
  offsetDialog.value = true
}

async function saveOffset() {
  await resetSyncState(currentJob.value.id, { value: resetValue.value, valueType: resetType.value })
  ElMessage.success('位点已更新')
  offsetDialog.value = false
}

onMounted(load)
</script>
