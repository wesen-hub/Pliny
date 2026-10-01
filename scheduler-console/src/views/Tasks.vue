<script setup>
import { ref, computed } from 'vue'
import { store, nsColor, stateLabel, handoverLabel } from '../store'
import { ElMessage } from 'element-plus'

// 下钻路径:level = ns | task | inst | part
const level = ref('ns')
const curNs = ref(null)
const curTask = ref(null)
const curInst = ref(null)

const migrate = ref(null)
const partDetail = ref(null) // 分区详情抽屉

// 实例维度聚合指标(从分区派生:该实例上所有分区求和)
const instAgg = id => {
  let rps = 0, lag = 0
  store.tasks.forEach(t => t.partitions.forEach(p => {
    if (p.owner === id) { rps += p.rps; lag += p.lag }
  }))
  return { rps, lag }
}

// 任务在其各实例上的分布
const taskOwners = computed(() => {
  if (!curTask.value) return []
  const byOwner = {}
  curTask.value.partitions.forEach(p => { (byOwner[p.owner] ||= []).push(p) })
  return Object.entries(byOwner).map(([owner, parts]) => ({
    inst: store.instances.find(i => i.id === owner),
    parts,
    taskRps: parts.reduce((s, p) => s + p.rps, 0),
    taskLag: parts.reduce((s, p) => s + p.lag, 0),
    latency: +(parts.reduce((s, p) => s + p.latency, 0) / parts.length).toFixed(1),
    errors: +(parts.reduce((s, p) => s + p.errorRate, 0) / parts.length).toFixed(2),
  }))
})

// 该实例上该任务的分区
const instParts = computed(() => {
  if (!curInst.value || !curTask.value) return []
  return curTask.value.partitions.filter(p => p.owner === curInst.value.id)
})

const statusMeta = { ONLINE: { label: '在线', type: 'success' }, DRAINING: { label: '迁空中', type: 'warning' }, SUSPECT: { label: '疑似离线', type: 'danger' } }

function now() {
  const d = new Date(); const p = n => String(n).padStart(2, '0')
  return `${p(d.getMonth() + 1)}-${p(d.getDate())} ${p(d.getHours())}:${p(d.getMinutes())}`
}

function drillNs(ns) { curNs.value = ns; curTask.value = null; curInst.value = null; level.value = 'task' }
function drillTask(t) { curTask.value = t; curInst.value = null; level.value = 'inst' }
function drillInst(owner) { curInst.value = owner.inst; level.value = 'part' }
function goTo(lv) {
  level.value = lv
  if (lv === 'ns') { curNs.value = null; curTask.value = null; curInst.value = null }
  if (lv === 'task') { curTask.value = null; curInst.value = null }
  if (lv === 'inst') { curInst.value = null }
}

function openMigrate(task, part) { migrate.value = { task, part } }
const targetInsts = computed(() => store.instances.filter(i => i.ns === migrate.value?.task.ns && i.status === 'ONLINE'))
function doMigrate(target) {
  const { task, part } = migrate.value
  store.audits.unshift({ time: now(), user: 'admin', ns: task.ns, action: '手动迁移', detail: `${task.topic} 分区${part.id}:${part.owner}→${target}` })
  part.owner = target
  migrate.value = null
  ElMessage.success('演示:归属已变更,目标实例 watch 到事件后起线程')
}
</script>

<template>
  <div>
    <h1 class="page-title">任务与分区</h1>
    <p class="page-desc">列表逐级下钻:<b>namespace → 任务 → 实例 → 分区</b>;实例维度与分区维度各有独立监控指标(实例维度后续扩展 JVM 级指标)。</p>

    <!-- 面包屑 -->
    <div class="crumb card">
      <span class="crumb-item" :class="{ on: level === 'ns' }" @click="goTo('ns')">namespace</span>
      <template v-if="level !== 'ns'">
        <span class="crumb-sep">›</span>
        <span class="crumb-item on" @click="goTo('task')">{{ curNs.label }}({{ curNs.name }})</span>
      </template>
      <template v-if="level === 'inst' || level === 'part'">
        <span class="crumb-sep">›</span>
        <span class="crumb-item on" @click="goTo('inst')">{{ curTask.unit || curTask.topic }}</span>
      </template>
      <template v-if="level === 'part'">
        <span class="crumb-sep">›</span>
        <span class="crumb-item on">{{ curInst.id }}</span>
      </template>
      <div class="crumb-right" v-if="level !== 'ns'">
        <el-button size="small" text @click="goTo(level === 'part' ? 'inst' : level === 'inst' ? 'task' : 'ns')">← 返回上一级</el-button>
      </div>
    </div>

    <!-- 第一级:ns 列表 -->
    <div v-if="level === 'ns'" class="card">
      <h3 class="sec-title">选择 namespace(ns 维度概览指标)</h3>
      <el-table :data="store.namespaces" @row-click="drillNs" class="clickable">
        <el-table-column label="namespace" width="140">
          <template #default="{ row }"><span :style="{ color: nsColor[row.name] || '#0f172a', fontWeight: 700 }">{{ row.name }}</span></template>
        </el-table-column>
        <el-table-column prop="label" label="场景" width="130" />
        <el-table-column label="任务" width="80"><template #default="{ row }">{{ row.tasks }}</template></el-table-column>
        <el-table-column label="实例" width="80"><template #default="{ row }">{{ store.instances.filter(i => i.ns === row.name).length }}</template></el-table-column>
        <el-table-column label="分区" width="80"><template #default="{ row }">{{ row.partitions }}</template></el-table-column>
        <el-table-column label="总 rps" width="110"><template #default="{ row }"><b>{{ (row.rps / 1000).toFixed(1) }}k</b></template></el-table-column>
        <el-table-column label="总 lag" width="110"><template #default="{ row }">{{ row.lag.toLocaleString() }}</template></el-table-column>
        <el-table-column label="操作" width="110">
          <template #default="{ row }"><el-button size="small" type="primary" text @click.stop="drillNs(row)">下钻任务 →</el-button></template>
        </el-table-column>
      </el-table>
    </div>

    <!-- 第二级:任务列表 -->
    <div v-else-if="level === 'task'" class="card">
      <h3 class="sec-title">
        <span class="ns-dot" :style="{ background: nsColor[curNs.name] }"></span>
        {{ curNs.label }}({{ curNs.name }})的任务(任务维度指标)
      </h3>
      <el-table :data="store.tasks.filter(t => t.ns === curNs.name)" @row-click="drillTask" class="clickable">
        <el-table-column label="任务(topic)" min-width="230">
          <template #default="{ row }"><b>{{ row.topic }}</b><span v-if="row.unit" class="unit-tag">单元</span></template>
        </el-table-column>
        <el-table-column label="装载" width="170"><template #default="{ row }"><el-tag size="small" :type="row.handlerRef.startsWith('spi') ? 'success' : 'primary'">{{ row.handlerRef }}</el-tag></template></el-table-column>
        <el-table-column label="并发边界" width="110"><template #default="{ row }">[{{ row.min }}, {{ row.max }}]</template></el-table-column>
        <el-table-column label="交接" width="80"><template #default="{ row }">{{ handoverLabel[row.handover] }}</template></el-table-column>
        <el-table-column label="rps" width="100"><template #default="{ row }"><b>{{ row.currentRps.toLocaleString() }}</b></template></el-table-column>
        <el-table-column label="lag" width="100"><template #default="{ row }">{{ row.partitions.reduce((s, p) => s + p.lag, 0).toLocaleString() }}</template></el-table-column>
        <el-table-column label="容量水位" min-width="170">
          <template #default="{ row }">
            <el-progress
              :percentage="Math.min(Math.round(row.currentRps / (row.partitions.length * row.targetRps) * 100), 100)"
              :color="row.currentRps / (row.partitions.length * row.targetRps) > 0.8 ? '#ef4444' : row.currentRps / (row.partitions.length * row.targetRps) > 0.5 ? '#f59e0b' : '#10b981'"
              :format="() => `${row.partitions.length} 分区`"
            />
          </template>
        </el-table-column>
        <el-table-column label="操作" width="110">
          <template #default="{ row }"><el-button size="small" type="primary" text @click.stop="drillTask(row)">下钻实例 →</el-button></template>
        </el-table-column>
      </el-table>
    </div>

    <!-- 第三级:实例列表(实例维度指标) -->
    <div v-else-if="level === 'inst'" class="card">
      <h3 class="sec-title">
        任务 {{ curTask.unit || curTask.topic }} 的实例分布
        <el-tag size="small" type="warning" style="margin-left:8px">实例维度指标:框架聚合快照 + 机器指标(JVM 级指标后续扩展)</el-tag>
      </h3>
      <el-table :data="taskOwners" @row-click="drillInst" class="clickable">
        <el-table-column label="实例" width="120">
          <template #default="{ row }"><b>{{ row.inst.id }}</b></template>
        </el-table-column>
        <el-table-column prop="inst.addr" label="地址" width="130" />
        <el-table-column label="状态" width="90">
          <template #default="{ row }"><el-tag size="small" :type="statusMeta[row.inst.status].type">{{ statusMeta[row.inst.status].label }}</el-tag></template>
        </el-table-column>
        <el-table-column label="此任务分区" width="100">
          <template #default="{ row }">{{ row.parts.length }} 线程</template>
        </el-table-column>
        <el-table-column label="聚合 rps" width="100"><template #default="{ row }"><b>{{ instAgg(row.inst.id).rps.toLocaleString() }}</b></template></el-table-column>
        <el-table-column label="聚合 lag" width="100"><template #default="{ row }">{{ instAgg(row.inst.id).lag.toLocaleString() }}</template></el-table-column>
        <el-table-column label="延迟 ms" width="90"><template #default="{ row }">{{ row.latency }}</template></el-table-column>
        <el-table-column label="错误率 %" width="90"><template #default="{ row }">{{ row.errors }}</template></el-table-column>
        <el-table-column label="CPU %" width="90">
          <template #default="{ row }"><span :style="{ color: row.inst.cpu > 70 ? '#ef4444' : '#334155' }">{{ row.inst.cpu }}</span></template>
        </el-table-column>
        <el-table-column label="内存 %" width="90"><template #default="{ row }">{{ row.inst.mem }}</template></el-table-column>
        <el-table-column label="JVM 线程" width="90"><template #default="{ row }">{{ row.inst.jvmThreads }}</template></el-table-column>
        <el-table-column label="负载" min-width="120">
          <template #default="{ row }"><el-progress :percentage="row.inst.load" :color="row.inst.load > 70 ? '#ef4444' : row.inst.load > 50 ? '#f59e0b' : '#10b981'" /></template>
        </el-table-column>
        <el-table-column label="操作" width="110">
          <template #default="{ row }"><el-button size="small" type="primary" text @click.stop="drillInst(row)">下钻分区 →</el-button></template>
        </el-table-column>
      </el-table>
      <div class="mini-note">
        实例维度指标 = 框架对该实例所有执行器快照的聚合(rps/lag/latency/errorRate)+ 实例宿主机器指标(CPU/内存/负载);<b>JVM 级指标(GC 次数/堆内存/线程池状态)为后续扩展方向</b>,同样走快照上报通道,业务侧无需感知。
      </div>
    </div>

    <!-- 第四级:分区列表(分区维度指标) -->
    <div v-else class="card">
      <h3 class="sec-title">
        {{ curInst.id }} 上任务 {{ curTask.unit || curTask.topic }} 的分区
        <el-tag size="small" type="success" style="margin-left:8px">分区维度指标:四个派生指标(框架差值计算)</el-tag>
      </h3>
      <el-table :data="instParts" class="clickable">
        <el-table-column label="分区" width="100"><template #default="{ row }"><b>分区 {{ row.id }}</b></template></el-table-column>
        <el-table-column label="rps" width="110"><template #default="{ row }"><b>{{ row.rps.toLocaleString() }}</b></template></el-table-column>
        <el-table-column label="lag" width="110">
          <template #default="{ row }"><span :style="{ color: row.lag > row.rps * 5 ? '#ef4444' : '#334155' }">{{ row.lag.toLocaleString() }}</span></template>
        </el-table-column>
        <el-table-column label="latency(ms)" width="120"><template #default="{ row }">{{ row.latency }}</template></el-table-column>
        <el-table-column label="errorRate(%)" width="120"><template #default="{ row }">{{ row.errorRate }}</template></el-table-column>
        <el-table-column label="状态" width="100">
          <template #default="{ row }"><el-tag size="small" :type="row.state === 'ASSIGNED' ? 'success' : 'warning'">{{ stateLabel[row.state] }}</el-tag></template>
        </el-table-column>
        <el-table-column label="操作" width="180">
          <template #default="{ row }">
            <el-button size="small" @click.stop="partDetail = row">详情</el-button>
            <el-button size="small" type="primary" text @click.stop="openMigrate(curTask, row)">迁移 →</el-button>
          </template>
        </el-table-column>
      </el-table>
      <div class="mini-note">
        分区维度指标 = rps(Δ条数/Δt)· lag(endOffset − 已提交位点)· latency(Δ耗时/Δ条数)· errorRate(Δ错误/Δ条数);业务只报四个原始计数器,框架查 Kafka 水位计算。
      </div>
    </div>

    <!-- 分区详情抽屉 -->
    <el-drawer :model-value="!!partDetail" @closed="partDetail = null" title="分区详情" size="420">
      <template v-if="partDetail">
        <div class="pd-row"><span>任务</span><b>{{ curTask.unit || curTask.topic }}</b></div>
        <div class="pd-row"><span>分区</span><b>分区 {{ partDetail.id }}</b></div>
        <div class="pd-row"><span>归属实例</span><b>{{ partDetail.owner }}</b></div>
        <div class="pd-row"><span>状态</span><b>{{ stateLabel[partDetail.state] }}</b></div>
        <div class="pd-divider">分区维度监控指标</div>
        <div class="pd-row"><span>rps(流量)</span><b>{{ partDetail.rps.toLocaleString() }}</b></div>
        <div class="pd-row"><span>lag(积压)</span><b>{{ partDetail.lag.toLocaleString() }}</b></div>
        <div class="pd-row"><span>latency(延迟)</span><b>{{ partDetail.latency }} ms</b></div>
        <div class="pd-row"><span>errorRate(错误率)</span><b>{{ partDetail.errorRate }}%</b></div>
        <div class="pd-divider">执行线程(框架不感知)</div>
        <div class="pd-row"><span>循环</span><b>poll → 业务处理 → 提交位点</b></div>
        <div class="pd-row"><span>上报</span><b>4 个原始计数器 / 10s</b></div>
        <el-button type="primary" plain style="margin-top:14px" @click="openMigrate(curTask, partDetail)">手动迁移此分区</el-button>
      </template>
    </el-drawer>

    <!-- 迁移对话框 -->
    <el-dialog :model-value="!!migrate" @closed="migrate = null" title="手动迁移分区" width="440">
      <p style="font-size:13px;color:#475569;margin-bottom:14px">
        迁移 <b>{{ migrate?.task?.topic }}</b> 的 <b>分区 {{ migrate?.part?.id }}</b>(当前:{{ migrate?.part?.owner }})
      </p>
      <div class="target-list">
        <div v-for="t in targetInsts" :key="t.id" class="target-item" @click="doMigrate(t.id)">
          <b>{{ t.id }}</b>
          <span class="t-meta">{{ t.addr }} · 负载 {{ t.load }}% · CPU {{ t.cpu }}% · 权重 {{ t.weight.toFixed(1) }}</span>
        </div>
      </div>
    </el-dialog>
  </div>
</template>

<style scoped>
.page-title { font-size: 22px; font-weight: 700; color: #0f172a; }
.page-desc { color: #64748b; font-size: 13px; margin: 8px 0 18px; }
.card { background: #fff; border-radius: 12px; padding: 18px 20px; box-shadow: 0 1px 3px rgba(15,23,42,.06); margin-bottom: 14px; }
.crumb { display: flex; align-items: center; gap: 6px; padding: 12px 18px; }
.crumb-item { font-size: 13.5px; color: #94a3b8; cursor: pointer; }
.crumb-item:hover { color: #2563eb; }
.crumb-item.on { color: #0f172a; font-weight: 600; }
.crumb-sep { color: #cbd5e1; }
.crumb-right { margin-left: auto; }
.sec-title { font-size: 15px; color: #0f172a; margin-bottom: 12px; display: flex; align-items: center; }
.ns-dot { width: 9px; height: 9px; border-radius: 50%; display: inline-block; margin-right: 8px; }
.unit-tag { font-size: 11px; background: #ecfdf5; color: #047857; border-radius: 4px; padding: 1px 6px; margin-left: 6px; }
.clickable :deep(.el-table__row) { cursor: pointer; }
.mini-note { font-size: 12px; color: #64748b; line-height: 1.8; background: #f8fafc; border-radius: 8px; padding: 8px 12px; margin-top: 12px; }
.pd-row { display: flex; justify-content: space-between; padding: 9px 0; border-bottom: 1px solid #f1f5f9; font-size: 13px; }
.pd-row span { color: #94a3b8; }
.pd-row b { color: #0f172a; }
.pd-divider { margin: 14px 0 4px; font-size: 12.5px; font-weight: 700; color: #334155; }
.target-list { display: flex; flex-direction: column; gap: 8px; }
.target-item { border: 1px solid #e2e8f0; border-radius: 8px; padding: 10px 14px; cursor: pointer; display: flex; justify-content: space-between; align-items: center; }
.target-item:hover { border-color: #2563eb; background: #eff6ff; }
.t-meta { font-size: 12px; color: #64748b; }
</style>
