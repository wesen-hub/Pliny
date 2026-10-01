<template>
  <div class="page-card">
    <el-card shadow="never">
      <div class="toolbar">
        <div style="display: flex; gap: 8px; flex-wrap: wrap; align-items: center">
          <el-input v-model="ns" placeholder="命名空间" clearable style="width: 140px" @keyup.enter="load" />
          <el-button type="primary" @click="load"><el-icon><Search /></el-icon>查询</el-button>
          <el-button @click="autoRefresh = !autoRefresh">{{ autoRefresh ? '停止自动刷新' : '自动刷新(5s)' }}</el-button>
          <span style="color: #909399; font-size: 12px">ops 指令由调度主消费执行,PENDING → DONE(删除)/FAILED(保留原因)</span>
        </div>
      </div>

      <!-- L1: ns 大盘(监控子文档 §二:ns 级聚合直读) -->
      <el-row :gutter="16" style="margin-top: 12px">
        <el-col :span="4" v-for="c in cards" :key="c.label">
          <div class="stat-card">
            <div class="stat-value">{{ c.value }}</div>
            <div class="stat-label">{{ c.label }}</div>
          </div>
        </el-col>
      </el-row>

      <el-tabs v-model="tab" style="margin-top: 8px">
        <!-- L0: 命名空间管理(创建/删除/默认规则) -->
        <el-tab-pane :label="`命名空间(${nss.length})`" name="namespaces">
          <div style="margin-bottom: 8px; display: flex; gap: 8px; align-items: center">
            <el-button type="primary" size="small" @click="openCreateNs">创建 namespace</el-button>
            <span style="font-size: 12px; color: #909399">ns = etcd 前缀硬隔离;默认规则由主消费 SET_RULES 热生效;删除需无任务且无在线实例</span>
          </div>
          <el-table :data="nss" v-loading="loading" stripe>
            <el-table-column prop="ns" label="命名空间" min-width="130" show-overflow-tooltip />
            <el-table-column label="主" width="70">
              <template #default="{ row }"><el-tag size="small" :type="row.hasLeader ? 'success' : 'info'">{{ row.hasLeader ? '在线' : '无' }}</el-tag></template>
            </el-table-column>
            <el-table-column prop="tasks" label="任务" width="70" />
            <el-table-column prop="instances" label="实例" width="70" />
            <el-table-column prop="parts" label="分区" width="70" />
            <el-table-column label="总 rps" width="90">
              <template #default="{ row }">{{ fmtNum(row.rps) }}</template>
            </el-table-column>
            <el-table-column label="总 lag" width="100">
              <template #default="{ row }">{{ fmtLag(row.lag) }}</template>
            </el-table-column>
            <el-table-column label="规则摘要" min-width="220" show-overflow-tooltip>
              <template #default="{ row }">
                <span v-if="row.rules">up {{ row.rules.up }}×{{ row.rules.upPeriods }} / down {{ row.rules.down }}×{{ row.rules.downPeriods }} / 冷却 {{ row.rules.cooldownMinutes }}min / 缩容{{ row.rules.downEnabled ? '开' : '关' }}</span>
                <span v-else style="color: #909399">内置缺省(未落键)</span>
              </template>
            </el-table-column>
            <el-table-column label="操作" width="130" fixed="right">
              <template #default="{ row }">
                <el-button link type="primary" size="small" @click="openRules(row)">默认规则</el-button>
                <el-button link type="danger" size="small" @click="confirmDeleteNs(row)">删除</el-button>
              </template>
            </el-table-column>
          </el-table>
        </el-tab-pane>

        <!-- L2: 任务(点击行下钻分区) -->
        <el-tab-pane :label="`任务(${metrics.tasks?.length || 0})`" name="tasks">
          <el-table :data="metrics.tasks || []" v-loading="loading" stripe @row-click="drillTask">
            <el-table-column prop="taskId" label="任务" min-width="180" show-overflow-tooltip />
            <el-table-column label="状态" width="110">
              <template #default="{ row }">
                <el-tag size="small" :type="row.state === 'RUNNING' ? 'success' : 'info'">{{ row.state }}</el-tag>
              </template>
            </el-table-column>
            <el-table-column label="分区/期望" width="100">
              <template #default="{ row }">{{ row.parts }} / {{ row.expectedParts }}</template>
            </el-table-column>
            <el-table-column label="rps" width="90">
              <template #default="{ row }">{{ fmtNum(row.rps) }}</template>
            </el-table-column>
            <el-table-column label="错误率" width="90">
              <template #default="{ row }">{{ fmtPct(row.errorRate) }}</template>
            </el-table-column>
            <el-table-column label="时延(ms)" width="90">
              <template #default="{ row }">{{ fmtNum(row.avgLatencyMs) }}</template>
            </el-table-column>
            <el-table-column label="lag" width="100">
              <template #default="{ row }">{{ fmtLag(row.lag) }}</template>
            </el-table-column>
            <el-table-column label="累计处理" width="110">
              <template #default="{ row }">{{ row.processedCount ?? 0 }}</template>
            </el-table-column>
          </el-table>
        </el-tab-pane>

        <!-- L3: 实例(注册 + 意图合并视图,行操作:下架/暂停/调权) -->
        <el-tab-pane :label="`实例(${instRows.length})`" name="instances">
          <el-table :data="instRows" v-loading="loading" stripe>
            <el-table-column prop="id" label="实例" min-width="160" show-overflow-tooltip />
            <el-table-column prop="addr" label="地址" min-width="130" show-overflow-tooltip />
            <el-table-column label="状态" width="180">
              <template #default="{ row }">
                <el-tag size="small" :type="row.status === 'ONLINE' ? 'success' : 'danger'">{{ row.status }}</el-tag>
                <el-tag v-if="row.drained" size="small" type="warning" style="margin-left: 4px">下架中</el-tag>
                <el-tag v-if="row.paused" size="small" type="info" style="margin-left: 4px">暂停接单</el-tag>
              </template>
            </el-table-column>
            <el-table-column label="权重" width="80">
              <template #default="{ row }">{{ row.weight?.toFixed(2) ?? '-' }}</template>
            </el-table-column>
            <el-table-column label="槽位占用" width="100">
              <template #default="{ row }">{{ row.parts }} / {{ row.capacity }}</template>
            </el-table-column>
            <el-table-column label="rps" width="90">
              <template #default="{ row }">{{ fmtNum(row.rps) }}</template>
            </el-table-column>
            <el-table-column label="CPU%" width="80">
              <template #default="{ row }">{{ row.cpu ?? '-' }}</template>
            </el-table-column>
            <el-table-column label="MEM%" width="80">
              <template #default="{ row }">{{ row.mem ?? '-' }}</template>
            </el-table-column>
            <el-table-column label="操作" width="250" fixed="right">
              <template #default="{ row }">
                <el-button v-if="!row.drained" link type="warning" size="small" @click.stop="confirmInstOp(row, 'DRAIN_INSTANCE')">下架</el-button>
                <el-button v-else link type="success" size="small" @click.stop="confirmInstOp(row, 'UNDRAIN_INSTANCE')">取消下架</el-button>
                <el-button v-if="!row.paused" link type="info" size="small" @click.stop="confirmInstOp(row, 'PAUSE_INSTANCE')">暂停</el-button>
                <el-button v-else link type="success" size="small" @click.stop="confirmInstOp(row, 'UNPAUSE_INSTANCE')">恢复</el-button>
                <el-button link type="primary" size="small" @click.stop="openWeight(row)">调权重</el-button>
              </template>
            </el-table-column>
          </el-table>
        </el-tab-pane>

        <!-- L4: 分区(行操作:钉住/解锁/定向迁移;taskFilter 由任务行下钻注入) -->
        <el-tab-pane :label="`分区(${partRows.length})`" name="parts">
          <div style="margin-bottom: 8px; display: flex; gap: 8px; align-items: center">
            <span style="font-size: 12px; color: #606266">任务过滤:</span>
            <el-select v-model="taskFilter" clearable placeholder="全部任务" style="width: 220px" size="small">
              <el-option v-for="t in metrics.tasks || []" :key="t.taskId" :label="t.taskId" :value="t.taskId" />
            </el-select>
          </div>
          <el-table :data="partRows" v-loading="loading" stripe>
            <el-table-column prop="taskId" label="任务" min-width="160" show-overflow-tooltip />
            <el-table-column prop="partId" label="分区" width="70" />
            <el-table-column label="状态" width="110">
              <template #default="{ row }">
                <el-tag size="small" :type="PART_STATE[row.state] || 'info'">{{ row.state }}</el-tag>
              </template>
            </el-table-column>
            <el-table-column prop="owner" label="owner" min-width="130" show-overflow-tooltip />
            <el-table-column label="rps" width="90">
              <template #default="{ row }">{{ fmtNum(row.rps) }}</template>
            </el-table-column>
            <el-table-column label="lag" width="100">
              <template #default="{ row }">{{ fmtLag(row.lag) }}</template>
            </el-table-column>
            <el-table-column label="错误率" width="90">
              <template #default="{ row }">{{ fmtPct(row.errorRate) }}</template>
            </el-table-column>
            <el-table-column label="累计处理" width="110">
              <template #default="{ row }">{{ row.processedCount ?? 0 }}</template>
            </el-table-column>
            <el-table-column label="操作" width="220" fixed="right">
              <template #default="{ row }">
                <el-button link type="warning" size="small" :disabled="row.state !== 'ASSIGNED'" @click.stop="openPartOp(row, 'PIN')">钉住</el-button>
                <el-button link type="success" size="small" :disabled="row.state !== 'ASSIGNED'" @click.stop="openPartOp(row, 'UNPIN')">解锁</el-button>
                <el-button link type="primary" size="small" :disabled="row.state !== 'ASSIGNED'" @click.stop="openPartOp(row, 'MIGRATE_PIN')">迁移</el-button>
              </template>
            </el-table-column>
          </el-table>
        </el-tab-pane>

        <!-- ops 指令队列(PENDING 待消费 / FAILED 保留原因;DONE 由主删除) -->
        <el-tab-pane :label="`指令(${ops.length})`" name="ops">
          <el-table :data="ops" v-loading="loading" stripe>
            <el-table-column prop="opId" label="opId" min-width="200" show-overflow-tooltip />
            <el-table-column label="类型" width="110">
              <template #default="{ row }">{{ OP_TYPE_LABELS[row.type] || row.type }}</template>
            </el-table-column>
            <el-table-column label="目标" min-width="170" show-overflow-tooltip>
              <template #default="{ row }">{{ row.target || (row.taskId && row.partId != null ? `${row.taskId}/${row.partId}` : '-') }}</template>
            </el-table-column>
            <el-table-column label="参数" width="110">
              <template #default="{ row }">
                <span v-if="row.weight != null">权重 {{ row.weight }}</span>
                <span v-else-if="row.ttlSec != null">TTL {{ row.ttlSec }}s</span>
                <span v-else>-</span>
              </template>
            </el-table-column>
            <el-table-column prop="operator" label="操作人" width="100" />
            <el-table-column prop="reason" label="原因" min-width="140" show-overflow-tooltip />
            <el-table-column label="状态" width="90">
              <template #default="{ row }">
                <el-tag size="small" :type="row.state === 'PENDING' ? 'warning' : 'danger'">{{ row.state }}</el-tag>
              </template>
            </el-table-column>
            <el-table-column prop="detail" label="失败原因" min-width="160" show-overflow-tooltip />
          </el-table>
        </el-tab-pane>
      </el-tabs>
    </el-card>

    <!-- 分区类 op 表单(钉住/迁移) -->
    <el-dialog v-model="partDlg.visible" :title="partDlg.title" width="440px">
      <el-form label-width="90px">
        <el-form-item label="目标">
          <span>{{ partDlg.taskId }}/{{ partDlg.partId }}</span>
        </el-form-item>
        <el-form-item v-if="partDlg.mode === 'PIN'" label="TTL(秒)">
          <el-input v-model="partDlg.ttl" placeholder="留空 = 永久锁" clearable />
        </el-form-item>
        <el-form-item v-if="partDlg.mode === 'MIGRATE_PIN'" label="迁移至实例">
          <el-select v-model="partDlg.target" placeholder="选择目标实例(存活且可接单)" style="width: 100%">
            <el-option v-for="i in instRows.filter(x => !x.drained && !x.paused && x.status === 'ONLINE')"
                       :key="i.id" :label="i.id" :value="i.id" />
          </el-select>
        </el-form-item>
        <el-form-item label="原因">
          <el-input v-model="partDlg.reason" type="textarea" :rows="2" placeholder="操作原因(审计用)" />
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="partDlg.visible = false">取消</el-button>
        <el-button type="primary" :loading="submitting" @click="submitPartOp">提交</el-button>
      </template>
    </el-dialog>

    <!-- 调权重表单 -->
    <el-dialog v-model="weightDlg.visible" title="调权重" width="440px">
      <el-form label-width="90px">
        <el-form-item label="实例">
          <span>{{ weightDlg.instId }}</span>
        </el-form-item>
        <el-form-item label="权重">
          <el-slider v-model="weightDlg.weight" :min="0.5" :max="2.0" :step="0.1" show-input :show-input-controls="false" style="width: 100%" />
        </el-form-item>
        <el-form-item label="原因">
          <el-input v-model="weightDlg.reason" type="textarea" :rows="2" placeholder="操作原因(审计用)" />
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="weightDlg.visible = false">取消</el-button>
        <el-button type="primary" :loading="submitting" @click="submitWeight">提交</el-button>
      </template>
    </el-dialog>

    <!-- 创建 namespace 表单 -->
    <el-dialog v-model="createDlg.visible" title="创建 namespace" width="440px">
      <el-form label-width="90px">
        <el-form-item label="名称">
          <el-input v-model="createDlg.ns" placeholder="小写字母开头,可含数字/下划线/中划线" />
        </el-form-item>
        <el-form-item label="原因">
          <el-input v-model="createDlg.reason" type="textarea" :rows="2" placeholder="创建原因(审计用)" />
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="createDlg.visible = false">取消</el-button>
        <el-button type="primary" :loading="submitting" @click="submitCreateNs">创建(默认规则为内置缺省)</el-button>
      </template>
    </el-dialog>

    <!-- ns 默认规则表单(7 字段,提交走 SET_RULES ops,主消费热生效) -->
    <el-dialog v-model="ruleDlg.visible" :title="`默认规则(${ruleDlg.ns})`" width="520px">
      <el-form label-width="130px">
        <el-form-item label="扩容水位 up">
          <el-input-number v-model="ruleDlg.r.up" :min="0.1" :max="1.0" :step="0.05" :precision="2" style="width: 160px" />
          <span style="margin-left: 8px; font-size: 12px; color: #909399">持续周期</span>
          <el-input-number v-model="ruleDlg.r.upPeriods" :min="1" :max="20" style="width: 110px; margin-left: 4px" />
        </el-form-item>
        <el-form-item label="缩容水位 down">
          <el-input-number v-model="ruleDlg.r.down" :min="0.0" :max="0.95" :step="0.05" :precision="2" style="width: 160px" />
          <span style="margin-left: 8px; font-size: 12px; color: #909399">持续周期</span>
          <el-input-number v-model="ruleDlg.r.downPeriods" :min="1" :max="20" style="width: 110px; margin-left: 4px" />
        </el-form-item>
        <el-form-item label="冷却(分钟)">
          <el-input-number v-model="ruleDlg.r.cooldownMinutes" :min="1" :max="120" style="width: 160px" />
        </el-form-item>
        <el-form-item label="步长护栏 maxStep">
          <el-input-number v-model="ruleDlg.r.maxStep" :min="1.0" :max="3.0" :step="0.5" :precision="1" style="width: 160px" />
          <span style="margin-left: 8px; font-size: 12px; color: #909399">单次最多翻倍/减半上限</span>
        </el-form-item>
        <el-form-item label="缩容总开关">
          <el-switch v-model="ruleDlg.r.downEnabled" active-text="开" inactive-text="关" />
          <span style="margin-left: 8px; font-size: 12px; color: #909399">关闭后所有任务缩容被闸住</span>
        </el-form-item>
        <el-form-item label="原因">
          <el-input v-model="ruleDlg.reason" type="textarea" :rows="2" placeholder="操作原因(审计用)" />
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="ruleDlg.visible = false">取消</el-button>
        <el-button type="primary" :loading="submitting" @click="submitRules">提交 SET_RULES</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup>
import { computed, onMounted, onUnmounted, reactive, ref } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import { listSchedulerMetrics, listSchedulerInstances, listSchedulerOps, submitSchedulerOp,
  listSchedulerNamespaces, createSchedulerNs, deleteSchedulerNs } from '../../api'

const PART_STATE = { ASSIGNED: 'success', MIGRATING: 'warning', PENDING: 'info', DELETING: 'danger' }
const OP_TYPE_LABELS = {
  PIN: '钉住分区', UNPIN: '解锁分区', MIGRATE_PIN: '定向迁移',
  DRAIN_INSTANCE: '实例下架', UNDRAIN_INSTANCE: '取消下架',
  PAUSE_INSTANCE: '暂停接单', UNPAUSE_INSTANCE: '恢复接单', SET_WEIGHT: '调权重'
}

const ns = ref('sync')
const metrics = ref({})
const instances = ref([])
const ops = ref([])
const nss = ref([])
const loading = ref(false)
const submitting = ref(false)
const autoRefresh = ref(true)
const tab = ref('tasks')
const taskFilter = ref('')

const partDlg = reactive({ visible: false, mode: '', title: '', taskId: '', partId: null, target: '', ttl: '', reason: '' })
const weightDlg = reactive({ visible: false, instId: '', weight: 1.0, reason: '' })
const createDlg = reactive({ visible: false, ns: '', reason: '' })
const ruleDlg = reactive({ visible: false, ns: '', reason: '', r: {} })

// L1 大盘卡(缺数据展示 0 / -)
const cards = computed(() => {
  const m = metrics.value.ns || {}
  return [
    { label: '任务数', value: m.tasks ?? '-' },
    { label: '实例数', value: m.instances ?? '-' },
    { label: '分区(已分配/总)', value: m.parts != null ? `${m.assignedParts ?? 0}/${m.parts}` : '-' },
    { label: '总 rps', value: m.rps != null ? Number(m.rps).toFixed(1) : '-' },
    { label: '总 lag', value: m.lag != null ? fmtLag(m.lag) : '-' },
    { label: '容量占用', value: m.occupancy != null ? `${(m.occupancy * 100).toFixed(0)}%` : '-' }
  ]
})

// L3 实例行:运维视图(注册 + 意图)join 实例级指标(addr/cpu/mem/rps)
const instRows = computed(() => {
  const m = new Map((metrics.value.instances || []).map(i => [i.instanceId, i]))
  return instances.value.map(v => {
    const im = m.get(v.id) || {}
    return { ...v, addr: im.addr, parts: im.parts ?? 0, rps: im.rps ?? 0, cpu: im.cpu, mem: im.mem }
  })
})

// L4 分区行:taskFilter 过滤(任务行下钻注入)
const partRows = computed(() => {
  const all = metrics.value.parts || []
  return taskFilter.value ? all.filter(p => p.taskId === taskFilter.value) : all
})

function fmtNum(v) { return v == null || Number.isNaN(v) ? '-' : Number(v).toFixed(1) }
function fmtPct(v) { return v == null ? '-' : `${(v * 100).toFixed(2)}%` }
function fmtLag(v) {
  if (v == null || v < 0) return '-'
  if (v >= 10000) return `${(v / 10000).toFixed(1)}w`
  return String(v)
}

// L2 → L4 下钻:点击任务行过滤分区并切 tab
function drillTask(row) {
  taskFilter.value = row.taskId
  tab.value = 'parts'
}

// ── 分区类 op ──
function openPartOp(row, mode) {
  partDlg.mode = mode
  partDlg.taskId = row.taskId
  partDlg.partId = row.partId
  partDlg.target = ''
  partDlg.ttl = ''
  partDlg.reason = ''
  partDlg.title = OP_TYPE_LABELS[mode] + `(${row.taskId}/${row.partId})`
  if (mode === 'UNPIN') {
    ElMessageBox.confirm(`确认解锁分区 ${row.taskId}/${row.partId}?解除后恢复调度算法管理。`, '解锁确认', { type: 'warning' })
      .then(() => submitPartOp()).catch(() => {})
    return
  }
  partDlg.visible = true
}

async function submitPartOp() {
  if (partDlg.mode === 'MIGRATE_PIN' && !partDlg.target) {
    ElMessage.warning('请选择目标实例')
    return
  }
  const ttl = partDlg.ttl ? Number(partDlg.ttl) : null
  if (partDlg.mode === 'PIN' && ttl != null && !(ttl > 0)) {
    ElMessage.warning('TTL 需为正整数秒,留空为永久锁')
    return
  }
  submitting.value = true
  try {
    const r = await submitSchedulerOp({
      ns: ns.value, type: partDlg.mode, taskId: partDlg.taskId, partId: partDlg.partId,
      target: partDlg.mode === 'MIGRATE_PIN' ? partDlg.target : null,
      ttlSec: partDlg.mode === 'PIN' ? ttl : null, reason: partDlg.reason || null
    })
    ElMessage.success(`指令已提交 ${r.opId},主将消费执行`)
    partDlg.visible = false
    afterOp()
  } finally {
    submitting.value = false
  }
}

// ── 实例类 op(确认式) ──
function confirmInstOp(row, mode) {
  const title = OP_TYPE_LABELS[mode]
  ElMessageBox.confirm(`确认对实例 ${row.id} 执行「${title}」?`, title, { type: 'warning' })
    .then(async () => {
      try {
        const r = await submitSchedulerOp({ ns: ns.value, type: mode, target: row.id, reason: null })
        ElMessage.success(`指令已提交 ${r.opId}`)
        afterOp()
      } catch (e) { /* 错误已由拦截器提示 */ }
    }).catch(() => {})
}

// ── 调权重 ──
function openWeight(row) {
  weightDlg.instId = row.id
  weightDlg.weight = row.weight || 1.0
  weightDlg.reason = ''
  weightDlg.visible = true
}

async function submitWeight() {
  submitting.value = true
  try {
    const r = await submitSchedulerOp({ ns: ns.value, type: 'SET_WEIGHT', target: weightDlg.instId, weight: Number(weightDlg.weight), reason: weightDlg.reason || null })
    ElMessage.success(`指令已提交 ${r.opId}`)
    weightDlg.visible = false
    afterOp()
  } finally {
    submitting.value = false
  }
}

// ── namespace 管理(创建/删除/默认规则 SET_RULES) ──
const DEFAULT_RULES = () => ({ up: 0.8, upPeriods: 3, down: 0.4, downPeriods: 6, cooldownMinutes: 10, maxStep: 2.0, downEnabled: true })

function openCreateNs() {
  createDlg.ns = ''
  createDlg.reason = ''
  createDlg.visible = true
}

async function submitCreateNs() {
  if (!createDlg.ns || !/^[a-z][a-z0-9_-]{0,62}$/.test(createDlg.ns)) {
    ElMessage.warning('名称非法:小写字母开头,可含数字/下划线/中划线,最长 63')
    return
  }
  submitting.value = true
  try {
    await createSchedulerNs({ ns: createDlg.ns, reason: createDlg.reason || null })
    ElMessage.success(`namespace ${createDlg.ns} 已创建(默认规则为内置缺省)`)
    createDlg.visible = false
    loadNs()
  } finally {
    submitting.value = false
  }
}

function openRules(row) {
  ruleDlg.ns = row.ns
  ruleDlg.reason = ''
  ruleDlg.r = { ...DEFAULT_RULES(), ...(row.rules || {}) }
  ruleDlg.visible = true
}

async function submitRules() {
  const r = ruleDlg.r
  if (!(r.up > r.down && r.up > 0 && r.up <= 1 && r.down >= 0 && r.down < 1)) {
    ElMessage.warning('水位非法:需 0 ≤ down < up ≤ 1')
    return
  }
  submitting.value = true
  try {
    const resp = await submitSchedulerOp({ ns: ruleDlg.ns, type: 'SET_RULES', rules: r, reason: ruleDlg.reason || null })
    ElMessage.success(`SET_RULES 已提交 ${resp.opId},主消费后热生效`)
    ruleDlg.visible = false
    afterOp()
    setTimeout(loadNs, 5000)
  } finally {
    submitting.value = false
  }
}

function confirmDeleteNs(row) {
  const extra = row.instances > 0 ? `该 ns 仍有 ${row.instances} 个在线实例,将被拒绝删除。` : ''
  ElMessageBox.confirm(
    `确认删除 namespace ${row.ns}?前置校验:无任务且无在线实例,清空 /dcp/${row.ns}/ 前缀。${extra}`,
    '删除确认', { type: 'warning' }
  ).then(async () => {
    try {
      await deleteSchedulerNs(row.ns)
      ElMessage.success(`namespace ${row.ns} 已删除`)
      loadNs()
      if (ns.value === row.ns) ns.value = 'sync'
    } catch (e) { /* 错误(如残留拒绝)已由拦截器提示 */ }
  }).catch(() => {})
}

// 提交后:先刷指令表,5s 后刷监控(等主决策轮消费)
function afterOp() {
  loadOps()
  setTimeout(load, 5000)
}

let timer = null
async function load() {
  loading.value = true
  try {
    const [m, inst, o] = await Promise.all([
      listSchedulerMetrics(ns.value),
      listSchedulerInstances(ns.value),
      listSchedulerOps(ns.value)
    ])
    metrics.value = m || {}
    instances.value = inst || []
    ops.value = o || []
  } finally {
    loading.value = false
  }
}

async function loadNs() {
  try {
    nss.value = (await listSchedulerNamespaces()) || []
  } catch (e) { /* 拦截器已提示 */ }
}

async function loadOps() {
  ops.value = await listSchedulerOps(ns.value)
}

onMounted(() => {
  load()
  loadNs()
  timer = setInterval(() => { if (autoRefresh.value) load() }, 5000)
})
onUnmounted(() => clearInterval(timer))
</script>

<style scoped>
.toolbar { display: flex; justify-content: space-between; align-items: center; }
.stat-card {
  background: #f7f9fc; border: 1px solid #e4e7ed; border-radius: 6px;
  padding: 12px 16px; text-align: center;
}
.stat-value { font-size: 22px; font-weight: 600; color: #303133; }
.stat-label { font-size: 12px; color: #909399; margin-top: 4px; }
</style>
