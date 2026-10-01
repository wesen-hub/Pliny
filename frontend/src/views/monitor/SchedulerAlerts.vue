<template>
  <div class="page-card">
    <el-card shadow="never">
      <div class="toolbar">
        <div style="display: flex; gap: 8px; flex-wrap: wrap">
          <el-input v-model="ns" placeholder="命名空间" clearable style="width: 140px" @keyup.enter="load" />
          <el-select v-model="f.level" placeholder="级别" clearable style="width: 110px">
            <el-option v-for="s in ['critical', 'warning']" :key="s" :label="s" :value="s" />
          </el-select>
          <el-select v-model="f.type" placeholder="类型" clearable style="width: 150px">
            <el-option v-for="(l, t) in TYPE_LABELS" :key="t" :label="l" :value="t" />
          </el-select>
          <el-select v-model="f.dim" placeholder="维度" clearable style="width: 110px">
            <el-option label="实例维度" value="instance" />
            <el-option label="任务维度" value="task" />
            <el-option label="分区维度" value="part" />
          </el-select>
          <el-input v-model="f.kw" placeholder="实例/任务/消息关键字" clearable style="width: 190px" @keyup.enter="load" />
          <el-button type="primary" @click="load"><el-icon><Search /></el-icon>查询</el-button>
        </div>
        <el-button @click="autoRefresh = !autoRefresh">{{ autoRefresh ? '停止自动刷新' : '自动刷新(5s)' }}</el-button>
      </div>

      <el-tabs v-model="tab" style="margin-top: 4px">
        <!-- 实例维度聚合: 一条事件带全维度字段, 页面默认按实例聚合(实例是任务/分区的公共载体) -->
        <el-tab-pane label="实例聚合" name="inst">
          <el-table :data="groups" v-loading="loading" stripe @row-click="drill">
            <el-table-column prop="instanceId" label="实例" min-width="180">
              <template #default="{ row }">{{ row.instanceId || '(任务/分区级告警)' }}</template>
            </el-table-column>
            <el-table-column label="未恢复" width="90" sortable prop="open">
              <template #default="{ row }">
                <el-tag :type="row.open > 0 ? 'danger' : 'success'" size="small">{{ row.open }}</el-tag>
              </template>
            </el-table-column>
            <el-table-column label="critical 数" width="110">
              <template #default="{ row }">
                <el-tag v-if="row.critical > 0" type="danger" size="small">{{ row.critical }}</el-tag>
                <span v-else>-</span>
              </template>
            </el-table-column>
            <el-table-column prop="total" label="事件总数" width="90" />
            <el-table-column label="最近告警" width="150">
              <template #default="{ row }">
                <span v-if="row.lastSince">{{ fmtTime(row.lastSince) }}</span>
                <span v-else>-</span>
              </template>
            </el-table-column>
            <el-table-column label="最近类型" min-width="140">
              <template #default="{ row }">{{ TYPE_LABELS[row.lastType] || row.lastType || '-' }}</template>
            </el-table-column>
          </el-table>
        </el-tab-pane>

        <!-- 事件流: 全维度字段, 可按维度/级别/类型/关键字筛选下钻 -->
        <el-tab-pane :label="`事件流(${filtered.length})`" name="events">
          <el-table :data="filtered" v-loading="loading" stripe>
            <el-table-column label="级别" width="90">
              <template #default="{ row }">
                <el-tag :type="row.level === 'critical' ? 'danger' : 'warning'" size="small">{{ row.level }}</el-tag>
              </template>
            </el-table-column>
            <el-table-column label="类型" width="140">
              <template #default="{ row }">{{ TYPE_LABELS[row.type] || row.type }}</template>
            </el-table-column>
            <el-table-column prop="instanceId" label="实例" min-width="140" show-overflow-tooltip />
            <el-table-column prop="taskId" label="任务" min-width="120" show-overflow-tooltip />
            <el-table-column label="分区" width="70">
              <template #default="{ row }">{{ row.partId ?? '-' }}</template>
            </el-table-column>
            <el-table-column prop="message" label="消息" min-width="220" show-overflow-tooltip />
            <el-table-column label="首次触发" width="150">
              <template #default="{ row }">{{ fmtTime(row.since) }}</template>
            </el-table-column>
            <el-table-column label="状态" width="90">
              <template #default="{ row }">
                <el-tag :type="row.resolvedAt ? 'success' : 'danger'" size="small">{{ row.resolvedAt ? '已恢复' : '未恢复' }}</el-tag>
              </template>
            </el-table-column>
            <el-table-column label="恢复时间" width="150">
              <template #default="{ row }">{{ row.resolvedAt ? fmtTime(row.resolvedAt) : '-' }}</template>
            </el-table-column>
          </el-table>
        </el-tab-pane>
      </el-tabs>
    </el-card>
  </div>
</template>

<script setup>
import { computed, onMounted, onUnmounted, reactive, ref } from 'vue'
import dayjs from 'dayjs'
import { listSchedulerAlerts } from '../../api'

const TYPE_LABELS = {
  SUSPECT: '疑似失联',
  EXEC_STUCK: '执行器卡死',
  DRAIN_TIMEOUT: '追平超时',
  LAG_HIGH: 'Lag 高位',
  ERROR_RATE: '错误率',
  CAPACITY_FULL: '容量水位',
  MIGRATE_TIMEOUT: '迁移超时',
  NO_INSTANCE: '无可用实例'
}

const ns = ref('demo')
const rows = ref([])
const loading = ref(false)
const autoRefresh = ref(true)
const tab = ref('inst')
const f = reactive({ level: '', type: '', dim: '', kw: '' })

function fmtTime(ts) { return dayjs(ts).format('MM-DD HH:mm:ss') }

function dimMatch(a) {
  if (!f.dim) return true
  if (f.dim === 'instance') return !!a.instanceId
  if (f.dim === 'task') return !!a.taskId
  return a.partId != null
}

const filtered = computed(() => rows.value.filter(a =>
  (!f.level || a.level === f.level) &&
  (!f.type || a.type === f.type) &&
  dimMatch(a) &&
  (!f.kw || (a.instanceId || '').includes(f.kw) || (a.taskId || '').includes(f.kw) || (a.message || '').includes(f.kw))
))

// 实例维度聚合: 未恢复数 / critical 数 / 最近告警
const groups = computed(() => {
  const m = new Map()
  for (const a of rows.value) {
    const key = a.instanceId || '__none__'
    if (!m.has(key)) {
      m.set(key, { instanceId: a.instanceId, open: 0, critical: 0, total: 0, lastSince: 0, lastType: '' })
    }
    const g = m.get(key)
    g.total++
    if (!a.resolvedAt) {
      g.open++
      if (a.level === 'critical') g.critical++
    }
    if (a.since > g.lastSince) { g.lastSince = a.since; g.lastType = a.type }
  }
  return [...m.values()].sort((x, y) => (y.open - x.open) || (y.lastSince - x.lastSince))
})

// 点击实例行 → 下钻事件流并过滤该实例
function drill(row) {
  if (!row.instanceId) return
  f.kw = row.instanceId
  f.level = ''
  f.type = ''
  f.dim = ''
  tab.value = 'events'
}

let timer = null
async function load() {
  loading.value = true
  try {
    rows.value = await listSchedulerAlerts(ns.value)
  } finally {
    loading.value = false
  }
}

onMounted(() => {
  load()
  timer = setInterval(() => { if (autoRefresh.value) load() }, 5000)
})
onUnmounted(() => clearInterval(timer))
</script>
