<template>
  <div class="page-card">
    <el-card shadow="never">
      <div class="toolbar">
        <div style="display: flex; gap: 8px; flex-wrap: wrap">
          <el-input v-model="query.jobId" placeholder="任务ID" clearable style="width: 110px" @keyup.enter="load" />
          <el-select v-model="query.status" placeholder="状态" clearable style="width: 120px" @change="load">
            <el-option v-for="s in ['RUNNING', 'SUCCESS', 'FAILED', 'STOPPED', 'TIMEOUT']" :key="s" :label="s" :value="s" />
          </el-select>
          <el-date-picker v-model="range" type="datetimerange" start-placeholder="开始" end-placeholder="结束"
                          style="width: 340px" @change="load" />
          <el-button type="primary" @click="load"><el-icon><Search /></el-icon>查询</el-button>
        </div>
        <el-button @click="autoRefresh = !autoRefresh">{{ autoRefresh ? '停止自动刷新' : '自动刷新(3s)' }}</el-button>
      </div>

      <el-table :data="rows" v-loading="loading" stripe>
        <el-table-column prop="id" label="执行ID" width="90" />
        <el-table-column prop="jobName" label="任务" min-width="150" show-overflow-tooltip />
        <el-table-column prop="triggerType" label="触发" width="80" />
        <el-table-column label="状态" width="100">
          <template #default="{ row }">
            <el-tag :type="statusTag(row.status)" size="small">{{ row.status }}</el-tag>
          </template>
        </el-table-column>
        <el-table-column label="数据量" width="130">
          <template #default="{ row }">{{ row.sourceCount ?? '-' }} → {{ row.targetCount ?? '-' }}</template>
        </el-table-column>
        <el-table-column label="耗时" width="100">
          <template #default="{ row }">{{ fmtDuration(row.durationMs) }}</template>
        </el-table-column>
        <el-table-column label="重试" prop="retryCount" width="60" />
        <el-table-column prop="startTime" label="开始时间" width="160" />
        <el-table-column label="错误信息" min-width="160" show-overflow-tooltip>
          <template #default="{ row }">
            <span :style="{ color: row.errorMessage ? '#f56c6c' : '#909399' }">{{ row.errorMessage || '-' }}</span>
          </template>
        </el-table-column>
        <el-table-column label="操作" width="90" fixed="right">
          <template #default="{ row }">
            <el-button size="small" @click="viewDetail(row)">详情</el-button>
          </template>
        </el-table-column>
      </el-table>

      <div class="pagination">
        <el-pagination background layout="total, prev, pager, next" :total="total"
                       :page-size="query.pageSize" :current-page="query.page" @current-change="p => { query.page = p; load() }" />
      </div>
    </el-card>

    <!-- 详情抽屉 -->
    <el-drawer v-model="drawer" title="执行详情" size="560px">
      <template v-if="detail">
        <el-descriptions :column="2" border size="small">
          <el-descriptions-item label="执行ID">{{ detail.history.id }}</el-descriptions-item>
          <el-descriptions-item label="任务">{{ detail.jobName }}</el-descriptions-item>
          <el-descriptions-item label="状态">
            <el-tag :type="statusTag(detail.history.status)" size="small">{{ detail.history.status }}</el-tag>
          </el-descriptions-item>
          <el-descriptions-item label="触发">{{ detail.history.triggerType }}</el-descriptions-item>
          <el-descriptions-item label="源行数">{{ detail.history.sourceCount ?? '-' }}</el-descriptions-item>
          <el-descriptions-item label="目标行数">{{ detail.history.targetCount ?? '-' }}</el-descriptions-item>
          <el-descriptions-item label="开始">{{ detail.history.startTime }}</el-descriptions-item>
          <el-descriptions-item label="耗时">{{ fmtDuration(detail.history.durationMs) }}</el-descriptions-item>
          <el-descriptions-item label="错误" :span="2">
            <span style="color: #f56c6c">{{ detail.history.errorMessage || '-' }}</span>
          </el-descriptions-item>
        </el-descriptions>
        <el-divider>执行日志</el-divider>
        <el-timeline style="max-height: 420px; overflow: auto">
          <el-timeline-item v-for="l in logs" :key="l.id" :timestamp="l.logTime"
                            :type="l.logLevel === 'ERROR' ? 'danger' : 'primary'">
            <span :style="{ color: l.logLevel === 'ERROR' ? '#f56c6c' : '#303133' }">{{ l.logContent }}</span>
          </el-timeline-item>
        </el-timeline>
      </template>
    </el-drawer>
  </div>
</template>

<script setup>
import { onMounted, onUnmounted, reactive, ref } from 'vue'
import dayjs from 'dayjs'
import { listExecutions, getExecution, executionLogs } from '../../api'

const rows = ref([])
const total = ref(0)
const loading = ref(false)
const range = ref(null)
const autoRefresh = ref(false)
const drawer = ref(false)
const detail = ref(null)
const logs = ref([])
const query = reactive({ page: 1, pageSize: 10, jobId: '', status: '', startTime: '', endTime: '' })

function statusTag(s) {
  return { RUNNING: 'warning', SUCCESS: 'success', FAILED: 'danger', STOPPED: 'info', TIMEOUT: 'danger' }[s] || 'info'
}
function fmtDuration(ms) {
  if (ms == null) return '-'
  if (ms < 1000) return ms + 'ms'
  return (ms / 1000).toFixed(1) + 's'
}

let timer = null
async function load() {
  loading.value = true
  try {
    if (range.value && range.value.length === 2) {
      query.startTime = dayjs(range.value[0]).format('YYYY-MM-DD HH:mm:ss')
      query.endTime = dayjs(range.value[1]).format('YYYY-MM-DD HH:mm:ss')
    } else {
      query.startTime = ''
      query.endTime = ''
    }
    const data = await listExecutions(query)
    rows.value = data.list
    total.value = data.total
  } finally {
    loading.value = false
  }
}

async function viewDetail(row) {
  drawer.value = true
  detail.value = await getExecution(row.id)
  logs.value = await executionLogs(row.id)
}

onMounted(() => {
  load()
  timer = setInterval(() => { if (autoRefresh.value) load() }, 3000)
})
onUnmounted(() => clearInterval(timer))
</script>
