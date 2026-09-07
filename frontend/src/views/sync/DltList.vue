<template>
  <div class="page-card">
    <!-- 统计卡片 -->
    <el-row :gutter="16">
      <el-col :span="6" v-for="c in cards" :key="c.label">
        <el-card shadow="never" class="stat-card" @click="applyFilter(c.filter)">
          <div class="stat-label">{{ c.label }}</div>
          <div class="stat-value" :style="{ color: c.color }">{{ c.value }}</div>
        </el-card>
      </el-col>
    </el-row>

    <!-- 逻辑表 TOP 排行 -->
    <el-card shadow="never" style="margin-top: 16px">
      <template #header>死信分布(按逻辑表)</template>
      <div v-if="stats.topTables?.length" class="top-bar">
        <div v-for="t in stats.topTables" :key="t.logical_table" class="top-item">
          <span class="top-name">{{ t.logical_table }}</span>
          <div class="top-track"><div class="top-fill" :style="{ width: topPct(t.cnt) }"></div></div>
          <span class="top-cnt">{{ t.cnt }}</span>
        </div>
      </div>
      <el-text v-else type="info" size="small">暂无死信</el-text>
    </el-card>

    <!-- 死信列表 -->
    <el-card shadow="never" style="margin-top: 16px">
      <template #header>
        <div class="toolbar">
          <div>
            <el-radio-group v-model="filter.status" size="small" @change="load">
              <el-radio-button value="">全部</el-radio-button>
              <el-radio-button value="PENDING">待处理</el-radio-button>
              <el-radio-button value="REPLAYED">已重放</el-radio-button>
              <el-radio-button value="IGNORED">已忽略</el-radio-button>
            </el-radio-group>
            <el-input v-model="filter.keyword" placeholder="业务键/错误关键字" size="small" clearable
              style="width: 220px; margin-left: 12px" @keyup.enter="load" @clear="load" />
            <el-button size="small" style="margin-left: 8px" @click="load">查询</el-button>
          </div>
          <div>
            <el-button size="small" type="primary" :disabled="!selection.length" @click="replayBatch">批量重放</el-button>
            <el-button size="small" :loading="loading" @click="load">刷新</el-button>
          </div>
        </div>
      </template>
      <el-table :data="rows" stripe size="small" v-loading="loading" @selection-change="s => selection = s">
        <el-table-column type="selection" width="42" />
        <el-table-column prop="id" label="ID" width="70" />
        <el-table-column prop="logical_table" label="逻辑表" width="130" />
        <el-table-column label="业务键" min-width="160">
          <template #default="{ row }"><span class="mono">{{ row.business_key || '—' }}</span></template>
        </el-table-column>
        <el-table-column label="错误摘要" min-width="260">
          <template #default="{ row }"><span class="mono err">{{ (row.error_msg || '').slice(0, 60) }}</span></template>
        </el-table-column>
        <el-table-column prop="fail_count" label="失败" width="60" align="center" />
        <el-table-column label="状态" width="90">
          <template #default="{ row }">
            <el-tag size="small" :type="statusType(row.status)" effect="plain">{{ statusText(row.status) }}</el-tag>
          </template>
        </el-table-column>
        <el-table-column label="最近失败" width="160">
          <template #default="{ row }"><span class="mono">{{ String(row.last_fail_time || '').slice(0, 19) }}</span></template>
        </el-table-column>
        <el-table-column label="操作" width="150">
          <template #default="{ row }">
            <el-link type="primary" underline="never" @click="openDetail(row)">详情</el-link>
            <el-link v-if="row.status === 'PENDING'" type="success" underline="never" style="margin-left: 8px" @click="replay(row)">重放</el-link>
            <el-link v-if="row.status === 'PENDING'" type="info" underline="never" style="margin-left: 8px" @click="ignore(row)">忽略</el-link>
          </template>
        </el-table-column>
      </el-table>
      <el-pagination style="margin-top: 12px" layout="total, prev, pager, next" :total="total"
        :page-size="filter.pageSize" v-model:current-page="filter.page" @current-change="load" />
    </el-card>

    <!-- 详情抽屉 -->
    <el-drawer v-model="drawer" title="死信详情" size="46%">
      <template v-if="detail">
        <el-descriptions :column="1" border size="small">
          <el-descriptions-item label="ID">{{ detail.id }}</el-descriptions-item>
          <el-descriptions-item label="逻辑表">{{ detail.logical_table }}</el-descriptions-item>
          <el-descriptions-item label="Topic / 分区 / Offset">
            <span class="mono">{{ detail.topic }} / {{ detail.partition_id }} / {{ detail.offset_id }}</span>
          </el-descriptions-item>
          <el-descriptions-item label="业务键"><span class="mono">{{ detail.business_key || '—' }}</span></el-descriptions-item>
          <el-descriptions-item label="状态">
            <el-tag size="small" :type="statusType(detail.status)" effect="plain">{{ statusText(detail.status) }}</el-tag>
          </el-descriptions-item>
          <el-descriptions-item label="失败次数">{{ detail.fail_count }}</el-descriptions-item>
          <el-descriptions-item label="首次失败">{{ String(detail.first_fail_time || '').slice(0, 19) }}</el-descriptions-item>
          <el-descriptions-item label="最近失败">{{ String(detail.last_fail_time || '').slice(0, 19) }}</el-descriptions-item>
          <el-descriptions-item v-if="detail.replay_time" label="重放时间">{{ String(detail.replay_time).slice(0, 19) }}</el-descriptions-item>
        </el-descriptions>
        <div class="sec-title">错误信息</div>
        <pre class="err-block">{{ detail.error_msg }}</pre>
        <div class="sec-title">消息原文(CdcRecord)</div>
        <pre class="payload-block">{{ prettyPayload }}</pre>
        <div style="margin-top: 16px">
          <el-button v-if="detail.status === 'PENDING'" type="primary" @click="replay(detail)">重放此消息</el-button>
          <el-button v-if="detail.status === 'PENDING'" @click="ignore(detail)">忽略</el-button>
        </div>
      </template>
    </el-drawer>
  </div>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import { dltPage, dltStats, dltDetail, dltReplay, dltReplayBatch, dltIgnore } from '../../api'

const loading = ref(false)
const rows = ref([])
const total = ref(0)
const selection = ref([])
const stats = ref({})
const drawer = ref(false)
const detail = ref(null)
const filter = ref({ status: '', keyword: '', page: 1, pageSize: 20 })

const cards = computed(() => [
  { label: '待处理', value: stats.value.pending ?? '—', color: '#fa8c16', filter: { status: 'PENDING' } },
  { label: '今日新增', value: stats.value.today ?? '—', color: '#f5222d', filter: {} },
  { label: '近 7 天', value: stats.value.week ?? '—', color: '#1677ff', filter: {} },
  { label: '已重放', value: stats.value.replayed ?? '—', color: '#52c41a', filter: { status: 'REPLAYED' } }
])

function applyFilter(f) {
  filter.value.status = f.status || ''
  filter.value.page = 1
  load()
}
function topPct(cnt) {
  const max = Math.max(...(stats.value.topTables || []).map(t => t.cnt), 1)
  return Math.round((cnt / max) * 100) + '%'
}
function statusText(s) { return { PENDING: '待处理', REPLAYED: '已重放', IGNORED: '已忽略' }[s] || s }
function statusType(s) { return { PENDING: 'warning', REPLAYED: 'success', IGNORED: 'info' }[s] || 'info' }

async function load() {
  loading.value = true
  try {
    const [pageData, statData] = await Promise.all([
      dltPage({ ...filter.value, keyword: filter.value.keyword || undefined }),
      dltStats()
    ])
    rows.value = pageData.list || []
    total.value = pageData.total || 0
    stats.value = statData || {}
  } catch (e) {
    ElMessage.error('死信列表加载失败: ' + (e?.message || e))
  } finally {
    loading.value = false
  }
}
onMounted(load)

async function openDetail(row) {
  try {
    detail.value = await dltDetail(row.id)
    drawer.value = true
  } catch (e) {
    ElMessage.error('详情加载失败: ' + (e?.message || e))
  }
}
const prettyPayload = computed(() => {
  if (!detail.value?.payload) return '—'
  try {
    return JSON.stringify(JSON.parse(detail.value.payload), null, 2)
  } catch {
    return detail.value.payload
  }
})

async function replay(row) {
  try {
    const r = await dltReplay(row.id)
    ElMessage.success(`已重放 #${row.id} → ${r.topic}(消费端幂等吸收)`)
    await load()
    if (drawer.value) drawer.value = false
  } catch (e) {
    ElMessage.error('重放失败: ' + (e?.message || e))
  }
}
async function replayBatch() {
  const ids = selection.value.map(r => r.id)
  await ElMessageBox.confirm(`确认重放 ${ids.length} 条死信?原样重发到原 topic, 消费端幂等吸收。`, '批量重放', { type: 'warning' })
  const r = await dltReplayBatch(ids)
  ElMessage.success(`已重放 ${r.replayed} 条`)
  await load()
}
async function ignore(row) {
  await ElMessageBox.confirm(`确认忽略死信 #${row.id}?将标记为已忽略, 不再重放。`, '忽略', { type: 'info' })
  await dltIgnore(row.id)
  ElMessage.success('已忽略')
  await load()
}
</script>

<style scoped>
.stat-card { cursor: pointer; text-align: center; }
.stat-label { color: #8f959e; font-size: 13px; }
.stat-value { font-size: 30px; font-weight: 700; margin-top: 6px; }
.toolbar { display: flex; justify-content: space-between; align-items: center; }
.mono { font-family: ui-monospace, Menlo, Consolas, monospace; font-size: 12px; }
.err { color: #f5222d; }
.sec-title { font-size: 13px; font-weight: 600; margin: 14px 0 8px; }
.err-block { background: #fff1f0; border: 1px solid #ffccc7; border-radius: 6px; padding: 10px; font-size: 12px; white-space: pre-wrap; word-break: break-all; max-height: 160px; overflow: auto; }
.payload-block { background: #f6f8fa; border: 1px solid #e8e8e8; border-radius: 6px; padding: 10px; font-size: 12px; white-space: pre-wrap; word-break: break-all; max-height: 300px; overflow: auto; }
.top-bar { display: flex; flex-direction: column; gap: 8px; }
.top-item { display: flex; align-items: center; gap: 10px; }
.top-name { width: 160px; font-size: 12.5px; font-weight: 600; }
.top-track { flex: 1; background: #f0f0f0; border-radius: 4px; height: 14px; overflow: hidden; }
.top-fill { height: 100%; background: linear-gradient(90deg, #1677ff, #36cfc9); border-radius: 4px; }
.top-cnt { width: 40px; text-align: right; font-size: 12px; color: #646a73; }
</style>
