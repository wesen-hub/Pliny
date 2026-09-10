<template>
  <div class="wizard">
    <el-steps :active="step" align-center finish-status="success" style="margin-bottom: 28px">
      <el-step title="源与目标" />
      <el-step title="同步模式" />
      <el-step title="同步策略" />
      <el-step :title="editId ? '确认修改' : '确认创建'" />
    </el-steps>

    <!-- ═══════ Step 1 源与目标 ═══════ -->
    <div v-if="step === 0" class="section">
      <div class="sec-title">① 源:文件从哪来</div>
      <el-form label-width="130px" style="max-width:1000px">
        <el-form-item label="文件数据源" required>
          <el-select v-model="form.sourceDatasourceId" filterable placeholder="选择数据源(本地目录 / S3/MinIO)" style="width:460px" @change="onSourceChange">
            <el-option v-for="d in dsOptions" :key="d.id" :value="d.id"
              :label="`${d.name} · ${typeLabel(d.type)}${d.database_name ? ' / ' + d.database_name : ''}`" />
          </el-select>
        </el-form-item>
        <el-form-item :label="pathLabel" required>
          <el-input v-model="form.sourcePath" :placeholder="pathPlaceholder" style="width:460px" />
          <el-text size="small" type="info" style="display:block;font-size:11px">{{ pathHint }}</el-text>
        </el-form-item>
        <el-form-item v-if="srcIsLocal" label="执行端">
          <el-radio-group v-model="form.execHost">
            <el-radio-button value="platform">平台(挂载目录)</el-radio-button>
            <el-radio-button value="agent">采集 Agent(业务机)</el-radio-button>
          </el-radio-group>
          <el-text size="small" type="info" style="display:block;font-size:11px">
            目录在平台容器内可见选「平台」;在远程/业务机器上选「采集 Agent」,文件由 Agent 直推
          </el-text>
        </el-form-item>
        <el-form-item v-if="form.execHost === 'agent'" label="Agent" required>
          <el-select v-model="form.agentId" filterable placeholder="选择已注册 Agent" style="width:460px">
            <el-option v-for="a in agents" :key="a.id" :value="a.id"
              :label="`#${a.id} ${a.name}(${a.host || '?'}) ${a.online ? '● 在线' : '○ 离线'}`" />
          </el-select>
          <el-text v-if="!agents.length" size="small" type="warning" style="display:block;font-size:11px">
            还没有 Agent:数据源管理 → 注册 Agent 拿注册码 → 在业务机启动 dcp-agent
          </el-text>
        </el-form-item>
      </el-form>

      <div class="sec-title mt16">② 目标:文件同步到哪</div>
      <el-form label-width="130px" style="max-width:1000px">
        <el-form-item label="目标存储">
          <el-select v-model="form.targetDatasourceId" clearable placeholder="默认 = 平台 MinIO(raw 等演示桶)" style="width:460px">
            <el-option v-for="d in s3Targets" :key="d.id" :value="d.id" :label="d.name" />
          </el-select>
        </el-form-item>
        <el-form-item label="目标桶" required>
          <el-input v-model="form.targetBucket" placeholder="如 raw / warehouse" style="width:200px" />
        </el-form-item>
        <el-form-item label="目标目录(前缀)">
          <el-input v-model="form.targetPrefix" placeholder="可空 = 桶根,如 orders/2026" style="width:320px" />
          <el-text size="small" type="info" style="margin-left:8px">源目录内容将放在 桶/目录 下,保留原有结构</el-text>
        </el-form-item>
      </el-form>
    </div>

    <!-- ═══════ Step 2 同步模式 ═══════ -->
    <div v-if="step === 1" class="section">
      <div class="sec-title">同步模式(引擎怎么搬)</div>
      <div class="mode-cards">
        <div class="mode-card" :class="{ sel: form.syncMode === 'full_then_incr' }" @click="setMode('full_then_incr')">
          <div class="mc-head">
            <b>全量+定时增量</b><el-tag size="small" type="success" effect="plain">推荐 · 常用</el-tag>
          </div>
          <div class="mc-desc">首轮 <code>rclone sync</code> <b>全量</b>对齐(目标收敛为源的镜像),之后按调度 <code>rclone copy</code> <b>只搬新增/变更</b>;适合持续采集、按天落文件的场景。</div>
        </div>
        <div class="mode-card" :class="{ sel: form.syncMode === 'full_once' }" @click="setMode('full_once')">
          <div class="mc-head"><b>一次性全量</b></div>
          <div class="mc-desc">与源对齐<b>一次</b>,跑完即止,<b>不需要调度</b>;适合整体搬迁、目录镜像(改完手动再执行即可)。</div>
        </div>
        <div class="mode-card" :class="{ sel: form.syncMode === 'incr_scheduled' }" @click="setMode('incr_scheduled')">
          <div class="mc-head"><b>仅定时增量</b></div>
          <div class="mc-desc">目标已有基线(或由其他任务建立),只按调度 <code>rclone copy</code> 搬新增/变更,<b>从不删除目标文件</b>;适合别人的全量任务已经建好底账的场景。</div>
        </div>
      </div>

      <div class="sec-title mt16">同步调度(什么时候触发)</div>
      <el-form label-width="130px" style="max-width:1000px">
        <template v-if="form.syncMode === 'full_once'">
          <el-alert type="info" :closable="false" show-icon style="max-width:820px"
            title="一次性全量无需调度:执行一轮后即结束,之后可随时手动再执行" />
        </template>
        <template v-else>
          <el-form-item label="调度方式" required>
            <el-radio-group v-model="form.scheduleMode">
              <el-radio-button value="interval">定时(间隔)</el-radio-button>
              <el-radio-button value="cron">定时(cron)</el-radio-button>
            </el-radio-group>
            <el-text size="small" type="info" style="display:block;font-size:11px">
              该模式靠周期触发增量,必须配置调度(interval 或 cron)
            </el-text>
          </el-form-item>
          <el-form-item v-if="form.scheduleMode === 'interval'" label="间隔(分钟)">
            <el-input-number v-model="form.intervalMin" :min="1" :max="1440" />
            <el-text size="small" type="info" style="margin-left:10px">每隔 N 分钟自动同步一轮(最小 1 分钟;0 变化时 rclone 自动跳过)</el-text>
          </el-form-item>
          <el-form-item v-if="form.scheduleMode === 'cron'" label="cron 表达式">
            <el-input v-model="form.cronExpr" style="width:300px" placeholder="如 0 0 2 * * ?(每天 02:00)" />
            <el-text size="small" type="info" style="display:block;font-size:11px">格式:秒 分 时 日 月 周;例:每天凌晨 2 点 = <code>0 0 2 * * ?</code></el-text>
          </el-form-item>
        </template>
      </el-form>
    </div>

    <!-- ═══════ Step 3 同步策略 ═══════ -->
    <div v-if="step === 2" class="section">
      <div class="sec-title">同步策略(直模 rclone 参数,全部可选)</div>
      <el-form label-width="130px" style="max-width:1000px">
        <el-form-item label="比较方式">
          <el-radio-group v-model="form.compare">
            <el-radio-button value="size_mtime">大小+修改时间(默认)</el-radio-button>
            <el-radio-button value="size_only">仅大小</el-radio-button>
            <el-radio-button value="checksum">校验和</el-radio-button>
          </el-radio-group>
          <el-text size="small" type="info" style="display:block;font-size:11px">
            决定"要不要搬":size_mtime 最稳;size_only 忽略时间;checksum 对比内容哈希(最准但更慢)
          </el-text>
        </el-form-item>
        <el-form-item label="覆盖策略">
          <el-radio-group v-model="form.updateMode">
            <el-radio-button value="overwrite">不同即覆盖(默认)</el-radio-button>
            <el-radio-button value="update">只搬更新的</el-radio-button>
            <el-radio-button value="ignore_existing">跳过已存在</el-radio-button>
          </el-radio-group>
          <el-text size="small" type="info" style="display:block;font-size:11px">
            增量模式建议「只搬更新的」;「跳过已存在」仅补缺失
          </el-text>
        </el-form-item>
        <el-form-item v-if="hasFull" label="删除目标多余文件">
          <div style="display:flex;align-items:center;flex-wrap:wrap;gap:8px">
            <el-switch v-model="form.deleteExtras" />
            <el-text size="small" type="info">全量轮把目标收敛为源的镜像:源已不存在的文件也删掉</el-text>
          </div>
        </el-form-item>
        <el-alert v-if="hasFull && form.deleteExtras && form.syncMode === 'full_then_incr'" type="warning" :closable="false" show-icon style="max-width:680px;margin:0 0 12px 130px"
          title="仅首轮全量会删除目标多余文件;之后增量从不删除" />
        <el-alert v-else-if="hasFull && form.deleteExtras" type="error" :closable="false" show-icon style="max-width:680px;margin:0 0 12px 130px"
          title="注意:开启后目标中源没有的文件会被删除,请确认目标目录没有需要保留的历史文件" />
        <el-text v-if="!hasFull" size="small" type="info" style="display:block;margin:0 0 12px 130px;font-size:11px">
          仅定时增量模式从不删除目标文件(执行 copy),无「删除目标多余」选项
        </el-text>

        <el-form-item label="包含文件(白名单)">
          <div class="tag-input">
            <el-tag v-for="(x, i) in form.includes" :key="i" closable size="small" @close="form.includes.splice(i, 1)">{{ x }}</el-tag>
            <el-input v-model="includeInput" size="small" style="width:240px" placeholder="glob,如 data/** 或 *.csv"
              @keyup.enter="pushTag('includes', includeInput, x => includeInput = x)" @blur="pushTag('includes', includeInput, x => includeInput = x)" />
            <el-text size="small" type="info" style="flex-basis:100%">只同步匹配的文件,可多条;留空 = 全部</el-text>
          </div>
        </el-form-item>
        <el-form-item label="排除文件(黑名单)">
          <div class="tag-input">
            <el-tag v-for="(x, i) in form.excludes" :key="i" closable size="small" type="warning" @close="form.excludes.splice(i, 1)">{{ x }}</el-tag>
            <el-input v-model="excludeInput" size="small" style="width:240px" placeholder="glob,如 *.tmp 或 **/temp/**"
              @keyup.enter="pushTag('excludes', excludeInput, x => excludeInput = x)" @blur="pushTag('excludes', excludeInput, x => excludeInput = x)" />
            <el-text size="small" type="info" style="flex-basis:100%">支持通配符:*.log = 所有 .log 文件;**/temp/** = 任意层级的 temp 目录</el-text>
          </div>
        </el-form-item>

        <el-form-item label="时间过滤">
          <div class="inline-fields">
            <span class="fl">仅最近</span>
            <el-input v-model="form.minAge" size="small" style="width:120px" placeholder="如 7d" />
            <span class="fl">,仅早于</span>
            <el-input v-model="form.maxAge" size="small" style="width:140px" placeholder="如 2026-01-01" />
            <el-text size="small" type="info">min-age / max-age;留空表示不限制</el-text>
          </div>
        </el-form-item>
        <el-form-item label="大小过滤">
          <div class="inline-fields">
            <span class="fl">最小</span>
            <el-input v-model="form.minSize" size="small" style="width:110px" placeholder="如 1M" />
            <span class="fl">最大</span>
            <el-input v-model="form.maxSize" size="small" style="width:110px" placeholder="如 10G" />
            <el-text size="small" type="info">min-size / max-size;支持 K/M/G/T,留空表示不限制</el-text>
          </div>
        </el-form-item>

        <el-form-item label="并发与限速">
          <div class="inline-fields">
            <span class="fl">传输并发</span>
            <el-input-number v-model="form.transfers" :min="1" :max="256" size="small" style="width:120px" />
            <span class="fl">比对并发</span>
            <el-input-number v-model="form.checkers" :min="1" :max="256" size="small" style="width:120px" />
            <span class="fl">限速</span>
            <el-input v-model="form.bwlimit" size="small" style="width:110px" placeholder="如 10M" />
          </div>
        </el-form-item>
        <el-form-item label="可靠性与日志">
          <div class="inline-fields">
            <span class="fl">重试</span>
            <el-input-number v-model="form.retries" :min="1" :max="100" size="small" style="width:110px" />
            <span class="fl">底层重试</span>
            <el-input-number v-model="form.lowLevelRetries" :min="1" :max="100" size="small" style="width:110px" />
            <span class="fl">试运行</span>
            <el-switch v-model="form.dryRun" />
            <el-text size="small" type="info">试运行只打印将要传输的文件,不真正写入目标</el-text>
          </div>
        </el-form-item>
      </el-form>
    </div>

    <!-- ═══════ Step 4 确认 ═══════ -->
    <div v-if="step === 3" class="section">
      <el-form label-width="130px" style="max-width:1000px">
        <el-form-item label="任务名称" required>
          <el-input v-model="form.name" placeholder="给这个同步任务起个名字,如:订单文件每日采集" style="width:380px" />
        </el-form-item>
        <el-form-item label="描述">
          <el-input v-model="form.description" placeholder="可选" style="width:480px" />
        </el-form-item>
      </el-form>
      <el-descriptions :column="1" border size="small" style="max-width:1000px">
        <el-descriptions-item label="源">{{ srcDesc }}</el-descriptions-item>
        <el-descriptions-item label="目标">{{ dstDesc }}</el-descriptions-item>
        <el-descriptions-item label="同步模式">{{ modeName }}<template v-if="form.execHost === 'agent'"> · 由 Agent#{{ form.agentId }} 执行</template></el-descriptions-item>
        <el-descriptions-item label="调度">{{ planName }}</el-descriptions-item>
        <el-descriptions-item label="策略">
          <template v-if="policyTags.length">
            <el-tag v-for="t in policyTags" :key="t.text" :type="t.type" size="small" effect="plain" style="margin:0 6px 6px 0">{{ t.text }}</el-tag>
          </template>
          <el-text v-else size="small" type="info">全部默认(rclone 默认口径)</el-text>
        </el-descriptions-item>
      </el-descriptions>
      <el-alert v-if="form.dryRun" type="warning" :closable="false" show-icon style="max-width:1000px;margin-top:12px"
        title="已开启试运行:本轮只打印将要传输的文件,不会真正写入目标" />
    </div>

    <div class="nav-bar">
      <el-button :disabled="step === 0" @click="step--">上一步</el-button>
      <el-button v-if="step < 3" type="primary" :disabled="!canNext" @click="step++">下一步</el-button>
      <el-button v-else :loading="saving" type="primary" @click="submit">{{ editId ? '保存修改' : '创建任务' }}</el-button>
      <el-text v-if="!canNext" size="small" type="warning">{{ nextHint }}</el-text>
      <div style="flex:1"></div>
      <el-text size="small" type="info">第 {{ step + 1 }} / 4 步{{ editId ? ` · 编辑任务 #${editId}` : '' }}</el-text>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue'
import { useRouter, useRoute } from 'vue-router'
import { ElMessage } from 'element-plus'
import request from '../../../api/request'

const router = useRouter()
const route = useRoute()
const editId = computed(() => (route.params.id ? Number(route.params.id) : null))
const step = ref(0)
const saving = ref(false)
const includeInput = ref('')
const excludeInput = ref('')
const datasources = ref([])
const agents = ref([])

const form = ref({
  name: '', description: '',
  sourceDatasourceId: null, sourcePath: '',
  execHost: 'platform', agentId: null,
  targetDatasourceId: null, targetBucket: 'raw', targetPrefix: '',
  // 模式三态 + 调度(与后端 create 入参一致: syncMode / scheduleMode / intervalMin / cronExpr)
  syncMode: 'full_then_incr', scheduleMode: 'interval', intervalMin: 5, cronExpr: '0 0 2 * * ?',
  // 策略(直模 rclone 参数,与后端 strategy 结构一致)
  compare: 'size_mtime', updateMode: 'overwrite', deleteExtras: true,
  includes: [], excludes: [],
  minAge: '', maxAge: '', minSize: '', maxSize: '',
  transfers: 8, checkers: 8, bwlimit: '', retries: 3, lowLevelRetries: 10, dryRun: false
})

const typeLabel = t => ({ s3: 'S3/MinIO 对象存储', local: '本地目录' }[t] || t)
const dsOptions = computed(() => datasources.value.filter(d => d.type === 's3' || d.type === 'local'))
const s3Targets = computed(() => datasources.value.filter(d => d.type === 's3'))
const srcDs = computed(() => dsOptions.value.find(x => x.id === form.value.sourceDatasourceId))
const srcIsLocal = computed(() => srcDs.value?.type === 'local')
// 模式是否包含"全量"(删除能力只存在于全量)
const hasFull = computed(() => form.value.syncMode !== 'incr_scheduled')

const pathLabel = computed(() => (srcIsLocal.value ? '源目录' : '源目录(桶内前缀)'))
const pathPlaceholder = computed(() => (srcIsLocal.value ? '目录路径,如 /data/orders 或 orders' : '整桶留空;子目录如 incoming/2026'))
const pathHint = computed(() => {
  if (!srcDs.value) return '请先选择文件数据源'
  if (srcIsLocal.value) return 'Agent 执行时填业务机上的目录;平台执行时填容器内可见路径'
  return '源对象存储下要同步的目录前缀,留空表示整个桶'
})

const canNext = computed(() => {
  switch (step.value) {
    case 0:
      return !!(form.value.sourceDatasourceId && String(form.value.sourcePath || '').trim() && form.value.targetBucket.trim())
    case 1:
      return form.value.syncMode === 'full_once' || ['interval', 'cron'].includes(form.value.scheduleMode)
    case 3:
      return !!form.value.name.trim()
    default:
      return true
  }
})
const nextHint = computed(() => {
  if (step.value === 0) return '请选择源数据源、填写源目录与目标桶'
  if (step.value === 3) return '请填写任务名称'
  return ''
})

const modeName = computed(() => ({
  full_then_incr: '全量+定时增量',
  full_once: '一次性全量',
  incr_scheduled: '仅定时增量'
}[form.value.syncMode] || form.value.syncMode))
const planName = computed(() => {
  if (form.value.syncMode === 'full_once') return '无需调度(跑完即止)'
  return form.value.scheduleMode === 'cron'
    ? `定时 ${form.value.cronExpr}`
    : `每 ${form.value.intervalMin} 分钟自动`
})
const srcDesc = computed(() => {
  const d = srcDs.value
  return d ? `${d.name}(${typeLabel(d.type)}) / ${form.value.sourcePath || '(整源)'}` : '—'
})
const dstDesc = computed(() => {
  const d = s3Targets.value.find(x => x.id === form.value.targetDatasourceId)
  return `${d ? d.name + ' · ' : '平台 MinIO · '}s3://${form.value.targetBucket}${form.value.targetPrefix ? '/' + form.value.targetPrefix : ''}`
})
const policyTags = computed(() => strategyTags(buildStrategy()))

function setMode(m) {
  form.value.syncMode = m
  // 仅全量可删目标多余: 增量模式关掉 deleteExtras,避免后端 400
  if (m === 'incr_scheduled') form.value.deleteExtras = false
}
function onSourceChange() {
  // 本地目录 → 默认 agent 执行; S3 → 平台
  if (srcIsLocal.value) {
    form.value.execHost = 'agent'
  } else {
    form.value.execHost = 'platform'
    form.value.agentId = null
  }
}
function pushTag(key, value, clear) {
  const v = String(value || '').trim()
  if (v && !form.value[key].includes(v)) form.value[key].push(v)
  clear('')
}

/** 策略 → 后端 strategy 结构(仅提交新 schema 字段;旧 overwrite/remove/exclude 等已废弃) */
function buildStrategy() {
  const s = {
    compare: form.value.compare,
    updateMode: form.value.updateMode,
    includes: [...form.value.includes],
    excludes: [...form.value.excludes],
    transfers: form.value.transfers,
    checkers: form.value.checkers,
    retries: form.value.retries,
    lowLevelRetries: form.value.lowLevelRetries,
    dryRun: form.value.dryRun
  }
  // deleteExtras 仅全量相关;incr_scheduled 提交会被后端 400,故该模式不带该字段
  if (hasFull.value) s.deleteExtras = form.value.deleteExtras
  for (const k of ['minAge', 'maxAge', 'minSize', 'maxSize', 'bwlimit']) {
    const v = String(form.value[k] || '').trim()
    if (v) s[k] = v
  }
  return s
}

/** 策略标签(列表/详情/确认步共用同一口径) */
function strategyTags(st) {
  const out = []
  if (!st || typeof st !== 'object') return out
  if (st.compare === 'checksum') out.push({ text: '校验=校验和', type: '' })
  else if (st.compare === 'size_only') out.push({ text: '校验=仅大小', type: '' })
  if (st.updateMode === 'update') out.push({ text: '覆盖=只搬更新', type: 'warning' })
  else if (st.updateMode === 'ignore_existing') out.push({ text: '覆盖=跳过已存在', type: 'warning' })
  if (st.deleteExtras === true) out.push({ text: '删除=删除目标多余', type: 'danger' })
  else if (st.deleteExtras === false) out.push({ text: '删除=保留目标多余', type: '' })
  if (st.includes?.length) out.push({ text: `包含×${st.includes.length}`, type: '' })
  if (st.excludes?.length) out.push({ text: `排除×${st.excludes.length}`, type: 'warning' })
  if (st.minAge) out.push({ text: `仅最近${st.minAge}`, type: '' })
  if (st.maxAge) out.push({ text: `截至${st.maxAge}`, type: '' })
  if (st.minSize) out.push({ text: `最小${st.minSize}`, type: '' })
  if (st.maxSize) out.push({ text: `最大${st.maxSize}`, type: '' })
  if (st.transfers || st.checkers) out.push({ text: `并发${st.transfers ?? 8}/${st.checkers ?? 8}`, type: 'info' })
  if (st.bwlimit) out.push({ text: `限速${st.bwlimit}`, type: 'info' })
  if (st.dryRun) out.push({ text: '试运行', type: 'info' })
  return out
}

onMounted(async () => {
  try {
    const all = (await request.get('/datasources', { params: { page: 1, pageSize: 200 } }))?.list || []
    datasources.value = all
  } catch (e) { /* 静默 */ }
  try {
    agents.value = (await request.get('/agents')) || []
  } catch (e) { /* 静默 */ }
  if (editId.value) {
    try {
      const t = await request.get(`/filesync/tasks/${editId.value}`)
      const st = typeof t.strategy === 'string' ? JSON.parse(t.strategy) : (t.strategy || {})
      form.value.name = t.name || ''
      form.value.description = t.description || ''
      form.value.sourceDatasourceId = t.source_datasource_id
      form.value.sourcePath = t.source_path || ''
      form.value.execHost = t.exec_host === 'agent' ? 'agent' : 'platform'
      form.value.agentId = t.agent_id || null
      form.value.targetDatasourceId = t.target_datasource_id || null
      form.value.targetBucket = t.target_bucket || 'raw'
      form.value.targetPrefix = t.target_prefix || ''
      // 模式三态 + 调度
      form.value.syncMode = ['full_once', 'incr_scheduled', 'full_then_incr'].includes(t.sync_mode) ? t.sync_mode : 'full_then_incr'
      form.value.scheduleMode = t.schedule_mode === 'cron' ? 'cron' : 'interval'
      form.value.intervalMin = t.interval_min || 5
      form.value.cronExpr = t.cron_expr || '0 0 2 * * ?'
      // 策略(新 schema)
      form.value.compare = ['size_only', 'checksum'].includes(st.compare) ? st.compare : 'size_mtime'
      form.value.updateMode = ['update', 'ignore_existing'].includes(st.updateMode) ? st.updateMode : 'overwrite'
      form.value.deleteExtras = st.deleteExtras !== false
      form.value.includes = [...(st.includes || [])]
      form.value.excludes = [...(st.excludes || [])]
      form.value.minAge = st.minAge || ''
      form.value.maxAge = st.maxAge || ''
      form.value.minSize = st.minSize || ''
      form.value.maxSize = st.maxSize || ''
      form.value.transfers = st.transfers || 8
      form.value.checkers = st.checkers || 8
      form.value.bwlimit = st.bwlimit || ''
      form.value.retries = st.retries || 3
      form.value.lowLevelRetries = st.lowLevelRetries || 10
      form.value.dryRun = !!st.dryRun
    } catch (e) {
      ElMessage.error('任务加载失败: ' + (e?.message || e))
    }
  }
})

async function submit() {
  if (!form.value.name.trim()) return ElMessage.warning('请填写任务名称')
  if (!form.value.sourceDatasourceId) return ElMessage.warning('请选择源数据源')
  if (!String(form.value.sourcePath || '').trim()) return ElMessage.warning('请填写源目录')
  if (!form.value.targetBucket.trim()) return ElMessage.warning('请填写目标桶')
  if (form.value.execHost === 'agent' && !form.value.agentId) return ElMessage.warning('本地目录由 Agent 执行,请选择 Agent')
  if (form.value.syncMode !== 'full_once' && !['interval', 'cron'].includes(form.value.scheduleMode))
    return ElMessage.warning('该同步模式必须配置定时调度(间隔或 cron)')
  if (form.value.scheduleMode === 'cron' && form.value.syncMode !== 'full_once' && !String(form.value.cronExpr || '').trim())
    return ElMessage.warning('请填写 cron 表达式')

  // 与后端契约一致: syncMode/scheduleMode/intervalMin/cronExpr;full_once 一律 scheduleMode=none
  const scheduleMode = form.value.syncMode === 'full_once' ? 'none' : form.value.scheduleMode
  const body = {
    name: form.value.name.trim(), description: form.value.description || null,
    sourceDatasourceId: form.value.sourceDatasourceId,
    sourcePath: String(form.value.sourcePath).trim(),
    execHost: form.value.execHost, agentId: form.value.execHost === 'agent' ? form.value.agentId : null,
    targetDatasourceId: form.value.targetDatasourceId || null,
    targetBucket: form.value.targetBucket.trim(), targetPrefix: form.value.targetPrefix.trim() || '',
    syncMode: form.value.syncMode,
    scheduleMode,
    intervalMin: scheduleMode === 'interval' ? form.value.intervalMin : 0,
    cronExpr: scheduleMode === 'cron' ? form.value.cronExpr : null,
    strategy: buildStrategy()
  }
  saving.value = true
  try {
    if (editId.value) {
      await request.put(`/filesync/tasks/${editId.value}`, body)
      ElMessage.success('修改已保存')
      router.push(`/filesync/task/${editId.value}`)
    } else {
      await request.post('/filesync/tasks', body)
      ElMessage.success('任务已创建')
      router.push('/filesync/task')
    }
  } catch (e) {
    ElMessage.error((editId.value ? '保存' : '创建') + '失败: ' + (e?.message || e))
  } finally {
    saving.value = false
  }
}
</script>

<style scoped>
.wizard { width: 100%; }
.section { border: 1px solid #e4e7ed; border-radius: 10px; padding: 26px 28px 18px; background: #fff; min-height: 360px; box-sizing: border-box; }
.sec-title { font-weight: 600; margin: 0 0 16px; font-size: 15px; color: #1f2d3d; }
.mt16 { margin-top: 24px; }
.mode-cards { display: flex; gap: 18px; flex-wrap: wrap; margin-bottom: 10px; max-width: 1000px; }
.mode-card { flex: 1 1 300px; max-width: 380px; min-width: 0; border: 1px solid #e4e7ed; border-radius: 10px; padding: 18px 20px; cursor: pointer; background: #fff; transition: all .15s; box-sizing: border-box; }
.mode-card:hover { border-color: #409eff; box-shadow: 0 2px 8px rgba(64, 158, 255, .12); }
.mode-card.sel { border-color: #409eff; background: #ecf5ff; }
.mc-head { display: flex; align-items: center; justify-content: space-between; gap: 8px; margin-bottom: 10px; font-size: 15px; }
.mc-desc { font-size: 13px; color: #6b7785; line-height: 1.8; word-break: break-word; }
.tag-input { display: flex; align-items: center; flex-wrap: wrap; gap: 8px; width: 100%; }
.inline-fields { display: flex; align-items: center; flex-wrap: wrap; gap: 8px; width: 100%; }
.inline-fields .fl { font-size: 12px; color: #6b7785; }
.nav-bar { position: sticky; bottom: 0; background: #fff; border-top: 1px solid #ebeef5; margin-top: 18px; padding: 14px 2px 6px; display: flex; align-items: center; gap: 12px; z-index: 5; }
</style>
