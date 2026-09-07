<template>
  <div class="page-card">
    <!-- ============ 头部: 任务身份 + 运行态 + 操作 ============ -->
    <el-card shadow="never" v-loading="loading">
      <div class="flex-between">
        <div style="min-width: 0">
          <div class="title">
            {{ task.logical_table || '…' }}
            <el-tag :type="runTagType" effect="plain">{{ runStatusLabel }}</el-tag>
            <el-tag v-if="task.sync_mode" :type="modeTag(task.sync_mode)" effect="plain">{{ modeName(task.sync_mode) }}</el-tag>
            <el-tag v-if="task.multi_source" type="warning" effect="plain">多表合并</el-tag>
            <el-tag v-if="asset && asset.sensitive" type="danger" effect="plain">含敏感字段</el-tag>
            <el-tag v-for="dt in dsTypes" :key="dt" type="info" effect="plain">{{ dt }}</el-tag>
          </div>
          <div class="meta">
            任务(逻辑表): <span class="mono">{{ task.logical_table }}</span>
            · 源表: {{ sourceText }} ({{ (asset && asset.bucketCount) || '—' }} 桶 / {{ (asset && asset.tableCount) || (task && cfg ? cfg.tableCount : '—') }} 物理表)
            <template v-if="keyText">· 业务键: <span class="mono">{{ keyText }}</span></template>
            <template v-if="task.sync_mode === 'scheduled-batch' || task.sync_mode === 'full-scheduled'">· cron: <span class="mono">{{ task.cron_expr }}</span></template>
            <template v-if="task.start_mode">· 起始位点: {{ startLabel(task.start_mode) }}</template>
            <template v-if="task.last_run_time">· 上次完成: {{ fmt(task.last_run_time) }}</template>
          </div>
        </div>
        <div class="head-actions">
          <el-link v-if="asset" type="primary" underline="never" style="margin-right: 14px"
                   @click="router.push(`/asset/${task.logical_table}`)">查看资产详情 →</el-link>
          <el-button plain :loading="loading" @click="loadAll">刷新</el-button>
          <el-button v-if="task.running" type="danger" plain :loading="acting" @click="stopCdc">停止采集</el-button>
          <el-button v-else-if="isAutoStopMode" type="success" :loading="acting" @click="startCdc">执行一次</el-button>
          <el-button v-else type="success" :loading="acting" @click="startCdc">启动采集</el-button>
        </div>
      </div>
    </el-card>

    <el-card shadow="never" style="margin-top: 16px">
      <el-tabs v-model="tab">
        <!-- ============ Tab1 任务信息 ============ -->
        <el-tab-pane label="任务信息" name="info">
          <!-- 采集段 -->
          <div class="sec-title">采集段: 源表 → Kafka(每源表一个 SeaTunnel CDC job)</div>
          <el-table :data="cdcTasks" stripe size="small">
            <el-table-column label="源表" min-width="200">
              <template #default="{ row }">
                <b>{{ row.sourceDb }}.{{ row.sourceTable }}</b>
                <el-tag size="small" type="info" effect="plain" style="margin-left:6px">实例 #{{ row.taskId }}</el-tag>
                <el-text v-if="row.ownerName && row.syncTaskId > 0" size="small" type="info" style="margin-left:4px">({{ row.ownerName }})</el-text>
              </template>
            </el-table-column>
            <el-table-column label="状态" width="90">
              <template #default="{ row }">
                <el-tag size="small" :type="row.running ? 'success' : 'info'" effect="plain">{{ row.running ? '采集中' : '已停止' }}</el-tag>
              </template>
            </el-table-column>
            <el-table-column label="源表主键/业务键" min-width="150">
              <template #default="{ row }">
                <span class="mono">{{ (row.businessKeyFields || []).join(', ') || (row.keyColumns || []).join(', ') || '—' }}</span>
              </template>
            </el-table-column>
            <el-table-column label="转换链" min-width="110">
              <template #default="{ row }">
                <el-tag v-if="row.fieldMapping" size="small" type="warning" effect="plain">字段映射</el-tag>
                <el-tag v-if="row.rowChain" size="small" type="warning" effect="plain" style="margin-left:4px">行链</el-tag>
                <span v-if="!row.fieldMapping && !row.rowChain" style="color:#8f959e">直通</span>
              </template>
            </el-table-column>
            <el-table-column label="SeaTunnel JobId" min-width="190">
              <template #default="{ row }">
                <span class="mono">{{ row.seaTunnelJobId || '—' }}</span>
                <el-tag v-if="row.running" size="small" type="success" effect="plain" style="margin-left:6px">job 运行中</el-tag>
              </template>
            </el-table-column>
            <el-table-column label="Topic" min-width="160">
              <template #default="{ row }"><span class="mono">{{ row.topic }}</span></template>
            </el-table-column>
            <el-table-column label="操作" width="180">
              <template #default="{ row }">
                <el-button v-if="row.running" size="small" type="danger" plain :loading="acting"
                           @click="stopInstance(row)">停止实例</el-button>
                <el-button v-else size="small" type="success" plain :loading="acting"
                           @click="startInstance(row)">启动实例</el-button>
              </template>
            </el-table-column>
          </el-table>
          <el-empty v-if="!cdcTasks.length" description="未注册 CDC 采集任务(先在向导中配置源表)" :image-size="60" />

          <!-- 合并段 -->
          <div class="sec-title mt16">写段: Kafka → TiDB 单表(攒批 ODKU · 字段级 seq 收敛)</div>
          <el-descriptions :column="3" border>
            <el-descriptions-item label="Topic">
              <span class="mono">{{ cfg.topic || '—' }} <el-tag size="small" effect="plain" style="margin-left:4px">{{ cfg.partitionCount || '—' }} 分区</el-tag></span>
            </el-descriptions-item>
            <el-descriptions-item label="消费组">
              <span class="mono">{{ cfg.consumerGroup || (task.consumerGroup) || '—' }}</span>
            </el-descriptions-item>
            <el-descriptions-item label="目标库">
              <span class="mono">{{ targetDb }}</span>
            </el-descriptions-item>
            <el-descriptions-item label="业务键(收敛键)">
              <span class="mono">{{ keyText || '—' }}</span>
            </el-descriptions-item>
            <el-descriptions-item label="物理表规划">
              预置 <span class="mono">{{ cfg.tableCount || '—' }}</span> 张 · 实际 {{ shardCountLabel }}
            </el-descriptions-item>
            <el-descriptions-item label="数据面(TiDB 单表即权威)">
              <span class="mono">{{ authorityText }}</span>
            </el-descriptions-item>
            <el-descriptions-item label="Kafka 积压(lag)">
              <el-tag size="small" :type="lagType" effect="plain">{{ lagText }}</el-tag>
            </el-descriptions-item>
            <el-descriptions-item label="最近更新">
              <span>{{ updatedText }}</span>
            </el-descriptions-item>
          </el-descriptions>
          <div class="tip">
            消费端攒批(有界队列 + 共享写线程池 + 按物理表多值 INSERT)· offset 由消费者线程按已写游标提交 ·
            幂等重放安全(ODKU + seq 裁判)· lag 长期 >0 通常是死信尾巴消息, 可在下方「死信」页处理
          </div>

          <!-- 分区级 lag 明细 -->
          <div class="sec-title mt16">分区积压明细(消费组 <span class="mono">{{ cfg.consumerGroup || '—' }}</span>)</div>
          <el-table :data="lagPartitions" stripe size="small" v-loading="loading">
            <el-table-column label="分区" width="90">
              <template #default="{ row }"><el-tag size="small" effect="plain">p{{ row.partition }}</el-tag></template>
            </el-table-column>
            <el-table-column label="消息末尾 offset" min-width="140">
              <template #default="{ row }"><span class="mono">{{ fmtOffset(row.endOffset) }}</span></template>
            </el-table-column>
            <el-table-column label="已提交游标" min-width="140">
              <template #default="{ row }">
                <span class="mono">{{ fmtOffset(row.committedOffset) }}</span>
                <el-tooltip v-if="row.endOffset > 0 && row.committedOffset === 0" content="该分区从未提交过位点, 重启后会从最早重放(幂等吸收)" placement="top">
                  <el-tag size="small" type="warning" effect="plain" style="margin-left:6px">未提交过</el-tag>
                </el-tooltip>
              </template>
            </el-table-column>
            <el-table-column label="积压 lag" width="150">
              <template #default="{ row }">
                <el-tag size="small" :type="pRowType(row).tag" effect="plain">{{ pRowType(row).label }}</el-tag>
              </template>
            </el-table-column>
            <el-table-column label="进度(已提交/末尾)" min-width="200">
              <template #default="{ row }">
                <div v-if="row.endOffset > 0" class="lagbar">
                  <div class="lagbar-fill" :class="row.lag > 0 ? 'lagbar-behind' : 'lagbar-ok'"
                       :style="{ width: Math.min(100, Math.round((row.committedOffset / row.endOffset) * 100)) + '%' }"></div>
                  <span class="mono">{{ Math.min(100, Math.round((row.committedOffset / row.endOffset) * 100)) }}%</span>
                </div>
                <span v-else style="color:#8f959e">—</span>
              </template>
            </el-table-column>
          </el-table>
          <el-empty v-if="!loading && !lagPartitions.length" description="无分区数据(任务未启动或查询失败)" :image-size="50" />
          <div class="tip">
            lag = 消息末尾 offset − 已提交游标;提交按已写游标由消费者线程执行。分区有积压的常见原因:
            写库追不上(背压)/ 死信尾巴消息 / 实例重平衡或重启后的重放窗口。
            指标汇总:{{ pLagText }} · 共 {{ lagPartitionCount }} 分区
          </div>

          <!-- 模式与调度 -->
          <div class="sec-title mt16">模式与调度</div>
          <el-descriptions :column="3" border>
            <el-descriptions-item label="同步模式">
              <el-tag :type="modeTag(task.sync_mode)" effect="plain">{{ modeName(task.sync_mode) }}</el-tag>
            </el-descriptions-item>
            <el-descriptions-item label="执行状态">
              <el-tag :type="runTagType" effect="plain">{{ runStatusLabel }}</el-tag>
            </el-descriptions-item>
            <el-descriptions-item label="SeaTunnel 状态">
              <span>{{ engineText }}</span>
            </el-descriptions-item>
            <el-descriptions-item label="全量范围" v-if="['full-once', 'full-scheduled', 'full-cdc'].includes(task.sync_mode)">
              {{ fullScopeLabel }}
            </el-descriptions-item>
            <el-descriptions-item label="cron 表达式" v-if="task.cron_expr">
              <span class="mono">{{ task.cron_expr }}</span>
            </el-descriptions-item>
            <el-descriptions-item label="起始位点" v-if="task.start_mode">
              {{ startLabel(task.start_mode) }}<template v-if="task.start_value"> · <span class="mono">{{ task.start_value }}</span></template>
            </el-descriptions-item>
            <el-descriptions-item label="上次完成">{{ fmt(task.last_run_time) || '—' }}</el-descriptions-item>
            <el-descriptions-item label="模式说明" :span="task.cron_expr || task.start_mode ? 2 : 3">
              <span style="color:#8f959e">{{ modeDesc }}</span>
            </el-descriptions-item>
          </el-descriptions>
        </el-tab-pane>

        <!-- ============ Tab2 同步历史 ============ -->
        <el-tab-pane :label="`同步历史${history.length ? '(' + history.length + ')' : ''}`" name="history">
          <div class="toolbar" style="margin-bottom: 10px">
            <el-button size="small" :loading="loading" @click="loadHistory">刷新</el-button>
            <span style="color:#8f959e;font-size:12px;margin-left:8px">动作:启动/停止/自动完成(full-once 跑完自动停) · 最多展示最近 50 条</span>
          </div>
          <el-table :data="history" stripe size="small">
            <el-table-column label="开始时间" width="170">
              <template #default="{ row }">{{ fmt(row.startTime) }}</template>
            </el-table-column>
            <el-table-column label="结束时间" width="170">
              <template #default="{ row }">
                <template v-if="row.endTime">{{ fmt(row.endTime) }}</template>
                <el-tag v-else-if="row.status === 'RUNNING'" size="small" type="primary" effect="plain">进行中</el-tag>
                <span v-else>—</span>
              </template>
            </el-table-column>
            <el-table-column label="动作" width="110">
              <template #default="{ row }"><el-tag size="small" effect="plain">{{ actionName(row.action) }}</el-tag></template>
            </el-table-column>
            <el-table-column label="状态" width="100">
              <template #default="{ row }">
                <el-tag size="small" :type="histStatusType(row.status)" effect="plain">{{ row.status }}</el-tag>
              </template>
            </el-table-column>
            <el-table-column prop="syncMode" label="模式" width="130">
              <template #default="{ row }">{{ modeName(row.syncMode) }}</template>
            </el-table-column>
            <el-table-column prop="jobId" label="JobId" min-width="180">
              <template #default="{ row }"><span class="mono">{{ row.jobId || '—' }}</span></template>
            </el-table-column>
            <el-table-column prop="message" label="说明" min-width="220" show-overflow-tooltip>
              <template #default="{ row }">{{ row.message || '—' }}</template>
            </el-table-column>
          </el-table>
          <el-empty v-if="!history.length" description="暂无执行记录(启动/停止任务后生成)" />
        </el-tab-pane>

        <!-- ============ Tab3 死信 ============ -->
        <el-tab-pane :label="`死信${dltTotal ? '(' + dltTotal + ')' : ''}`" name="dlt">
          <div class="toolbar" style="margin-bottom: 10px">
            <el-select v-model="dltStatus" size="small" style="width: 150px" @change="loadDlt">
              <el-option label="全部状态" value="" />
              <el-option label="待处理" value="PENDING" /><el-option label="已重放" value="REPLAYED" /><el-option label="已忽略" value="IGNORED" />
            </el-select>
            <el-button size="small" :loading="dltLoading" @click="loadDlt">刷新</el-button>
          </div>
          <el-table :data="dlts" stripe size="small">
            <el-table-column label="时间" width="160">
              <template #default="{ row }">{{ fmt(row.last_fail_time) }}</template>
            </el-table-column>
            <el-table-column label="源表" width="140">
              <template #default="{ row }">{{ row.source_table || '—' }}</template>
            </el-table-column>
            <el-table-column label="失败原因" min-width="260" show-overflow-tooltip>
              <template #default="{ row }"><span class="mono" style="color:#cf1322;font-size:12px">{{ row.error_msg }}</span></template>
            </el-table-column>
            <el-table-column label="失败次数" width="80" prop="fail_count" />
            <el-table-column label="状态" width="90">
              <template #default="{ row }">
                <el-tag size="small" :type="row.status === 'PENDING' ? 'danger' : row.status === 'REPLAYED' ? 'success' : 'info'" effect="plain">{{ row.status }}</el-tag>
              </template>
            </el-table-column>
            <el-table-column label="操作" width="160">
              <template #default="{ row }">
                <el-link v-if="row.status === 'PENDING'" type="primary" underline="never" @click="replayDlt(row)">重放</el-link>
                <el-link v-if="row.status === 'PENDING'" type="warning" underline="never" style="margin-left:8px" @click="ignoreDlt(row)">忽略</el-link>
              </template>
            </el-table-column>
          </el-table>
          <el-empty v-if="!dlts.length && !dltLoading" description="无死信消息" />
        </el-tab-pane>
      </el-tabs>
    </el-card>
  </div>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { ElMessage } from 'element-plus'
import {
  listSyncTasks, cdcStatus, cdcStart, cdcStop, cdcStartInstance, cdcStopInstance, getLogicalTable,
  listRunHistory, dltPage, dltReplay, dltIgnore, listAssets, lakeStatus, scaleLag,
} from '../../api'

const route = useRoute()
const router = useRouter()
const lt = computed(() => route.params.id)

const loading = ref(false)
const acting = ref(false)
const tab = ref('info')
const task = ref({})
const cfg = ref({})
const cdcTasks = ref([])
const asset = ref(null)
const history = ref([])
const dlts = ref([])
const dltTotal = ref(0)
const dltStatus = ref('')
const dltLoading = ref(false)
const lagObj = ref(null)
const lake = ref(null)

/* ---------- 派生 ---------- */
const runStatusLabel = computed(() => {
  if (task.value.running) return '运行中'
  const m = { COMPLETED: '已完成', FAILED: '失败', STOPPED: '已停止', IDLE: '空闲(未启动)', PENDING: '等待中' }
  return m[task.value.run_status] || '已停止'
})
const runTagType = computed(() => {
  if (task.value.running) return 'success'
  return { COMPLETED: 'success', FAILED: 'danger', IDLE: 'info', PENDING: 'warning', STOPPED: 'info' }[task.value.run_status] || 'info'
})
const isAutoStopMode = computed(() => ['full-once', 'full-scheduled', 'scheduled-batch'].includes(task.value.sync_mode))
const sourceText = computed(() => (task.value.sources || []).join(' + ') || '—')
const dsTypes = computed(() => {
  const set = new Set()
  ;(asset.value?.sources || []).forEach(s => s && s.dsType && set.add(s.dsType))
  return [...set]
})
const keyText = computed(() => {
  const k = (cfg.value.businessKey && cfg.value.businessKey.keyFields) || cfg.value.keyColumns
  if (k && k.length) return k.join(', ')
  const c = cdcTasks.value[0]
  if (c && (c.businessKeyFields || []).length) return c.businessKeyFields.join(', ')
  return ''
})
const targetDb = computed(() => cfg.value.target_database || 'target_db')
const shardCountLabel = computed(() => {
  const n = asset.value?.shardCount
  return n == null ? '—' : `${n} 张${n > (cfg.value.tableCount || 0) ? '(含权威表)' : ''}`
})
const authorityText = computed(() => {
  if (!lake.value) return '—'
  const rows = lake.value.authorityRows
  const mx = lake.value.authorityMaxSeq
  if (rows == null && mx == null) return '—'
  return `${rows ?? 0} 行 · 已追平到 seq ${mx ?? 0}`
})
const updatedText = computed(() => (asset.value?.updated ? String(asset.value.updated).replace('T', ' ').slice(0, 19) : '—'))
const engineText = computed(() => {
  const runningSrc = cdcTasks.value.filter(c => c.running).length
  if (!cdcTasks.value.length) return '未启动'
  return `${runningSrc}/${cdcTasks.value.length} 个采集 job 运行中`
})
const totalLag = computed(() => (lagObj.value ? lagObj.value.totalLag ?? null : null))
const lagPartitions = computed(() => (lagObj.value && lagObj.value.partitions) || [])
const lagPartitionCount = computed(() => (lagObj.value ? lagObj.value.partitionCount : 0))
const lagText = computed(() => (totalLag.value == null ? '—' : `${totalLag.value}`))
const lagType = computed(() => (totalLag.value == null ? 'info' : totalLag.value > 0 ? 'warning' : 'success'))
const pLagText = computed(() => (totalLag.value == null ? '无数据' : totalLag.value > 0 ? `总积压 ${totalLag.value}` : '无积压'))
const fullScopeLabel = computed(() => {
  if (task.value.full_scope === 'snapshot-only') return '纯快照(采完自动停)'
  if (task.value.sync_mode === 'full-cdc') return '全量快照 + binlog 持续增量'
  return '快照 + 运行中增量'
})
const modeDesc = computed(() => ({
  'full-once': '一次性全量:快照完成后任务自动停止(autoStop),不常驻占 binlog',
  'full-scheduled': '全量 + 定时:按 cron 周期执行,每轮带上次位点续采',
  'scheduled-batch': '定时批量:仅增量,按 cron 触发,位点续采,空闲自动停',
  'full-cdc': '全量+CDC:先快照再持续采集 binlog,常驻(推荐实时链路)',
  incremental: '仅增量:从起始位点持续采集,常驻',
})[task.value.sync_mode] || '—')

function modeName(m) { return { 'full-once': '一次性全量', 'scheduled-batch': '定时批量(仅增量)', 'full-cdc': '全量+CDC', incremental: '仅增量', 'full-scheduled': '全量+定时批量' }[m] || '—' }
function modeTag(m) { return { 'full-once': 'warning', 'scheduled-batch': 'danger', 'full-cdc': 'success', incremental: 'primary', 'full-scheduled': 'danger' }[m] || 'info' }
function actionName(a) { return { start: '启动', stop: '停止', auto_complete: '自动完成', fail: '失败' }[a] || a }
function histStatusType(s) { return { RUNNING: 'primary', COMPLETED: 'success', STOPPED: 'info', FAILED: 'danger' }[s] || 'info' }
function startLabel(m) { return { earliest: '最早可用位点', latest: '最新位点', timestamp: '指定时间点', 'specific-offsets': 'binlog 位点' }[m] || m }
function pRowType(row) {
  if (row.lag > 0) return { tag: 'warning', label: `积压 ${row.lag}` }
  if (row.endOffset > 0) return { tag: 'success', label: '已同步' }
  return { tag: 'info', label: '无消息' }
}
function fmtOffset(v) { return v == null ? '—' : String(v) }
function fmt(t) { return t ? String(t).slice(0, 19) : '—' }

/* ---------- 加载 ---------- */
async function loadAll() {
  loading.value = true
  try {
    let list
    try {
      list = await listSyncTasks() || []
    } catch (e) {
      // 认证/网络失败: 全局拦截器已提示(401 会自动跳登录), 不要误报"任务不存在"
      return
    }
    const found = list.find(t => t.logical_table === lt.value)
    if (!found) {
      // 任务已删除/不存在(如全量清理后旧页面): 提示并回列表
      ElMessage.warning('同步任务不存在或已被删除')
      router.push('/sync/task')
      return
    }
    task.value = found
    try { cdcTasks.value = ((await cdcStatus()) || []).filter(c => c.logicalTable === lt.value) } catch { cdcTasks.value = [] }
    try { cfg.value = (await getLogicalTable(lt.value)) || {} } catch { cfg.value = {} }
    try { asset.value = ((await listAssets()) || []).find(a => a.table === lt.value) || null } catch { asset.value = null }
    try { const l = await scaleLag(lt.value); lagObj.value = l || null } catch { lagObj.value = null }
    try { lake.value = await lakeStatus(lt.value) || null } catch { lake.value = null }
  } finally {
    loading.value = false
  }
}

async function loadHistory() {
  try { history.value = await listRunHistory(lt.value, 50) || [] } catch { history.value = [] }
}
async function loadDlt() {
  dltLoading.value = true
  try {
    const res = await dltPage({ logicalTable: lt.value, status: dltStatus.value || undefined, page: 1, pageSize: 50 })
    dlts.value = (res && res.list) || []
    dltTotal.value = (res && res.total) || dlts.value.length
  } catch {
    dlts.value = []
    dltTotal.value = 0
  } finally {
    dltLoading.value = false
  }
}

async function startCdc() {
  acting.value = true
  try {
    await cdcStart(lt.value)
    ElMessage.success(isAutoStopMode.value ? '已触发执行' : '已启动')
    await Promise.all([loadAll(), loadHistory()])
  } finally {
    acting.value = false
  }
}
async function stopCdc() {
  acting.value = true
  try {
    await cdcStop(lt.value)
    ElMessage.success('已停止, 已写位点将保留')
    await Promise.all([loadAll(), loadHistory()])
  } finally {
    acting.value = false
  }
}

/** 实例级启/停(同(逻辑,源)多实例并存时用; 实例行专属按钮) */
async function startInstance(row) {
  acting.value = true
  try {
    await cdcStartInstance(row.taskId)
    ElMessage.success(`实例 #${row.taskId} 已启动(独立 seq/写者列)`)
    await Promise.all([loadAll(), loadHistory()])
  } finally {
    acting.value = false
  }
}
async function stopInstance(row) {
  acting.value = true
  try {
    await cdcStopInstance(row.taskId)
    ElMessage.success(`实例 #${row.taskId} 已停止`)
    await Promise.all([loadAll(), loadHistory()])
  } finally {
    acting.value = false
  }
}
async function replayDlt(row) {
  await dltReplay(row.id)
  ElMessage.success('已重放')
  loadDlt()
}
async function ignoreDlt(row) {
  await dltIgnore(row.id)
  ElMessage.success('已忽略')
  loadDlt()
}

onMounted(() => { loadAll(); loadHistory(); loadDlt() })
</script>

<style scoped>
.flex-between { display: flex; justify-content: space-between; align-items: flex-start; gap: 16px; }
.head-actions { display: flex; align-items: center; flex-shrink: 0; }
.title { font-size: 18px; font-weight: 700; display: flex; align-items: center; gap: 8px; flex-wrap: wrap; }
.meta { color: #8f959e; font-size: 12px; margin-top: 4px; line-height: 1.7; }
.sec-title { font-size: 13px; font-weight: 600; margin: 12px 0 8px; }
.mt16 { margin-top: 16px; }
.mono { font-family: ui-monospace, Menlo, Consolas, monospace; font-size: 12px; }
.tip { margin-top: 10px; font-size: 12px; color: #8f959e; line-height: 1.7; }
.toolbar { display: flex; align-items: center; }
.lagbar { display: flex; align-items: center; gap: 8px; }
.lagbar-fill { height: 6px; border-radius: 3px; background: #67c23a; min-width: 2px; }
.lagbar-fill.lagbar-behind { background: #e6a23c; }
.lagbar-fill.lagbar-ok { background: #67c23a; }
</style>
