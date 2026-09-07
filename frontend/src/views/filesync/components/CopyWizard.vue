<template>
  <div class="wizard">
    <el-steps :active="step" align-center finish-status="success" style="margin-bottom: 28px">
      <el-step title="源与目标" />
      <el-step title="同步方式" />
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
            目录在平台容器内可见选「平台」;在远程/业务机器上选「采集 Agent」,文件由 Agent 直推</el-text>
        </el-form-item>
        <el-form-item v-if="form.execHost === 'agent'" label="Agent" required>
          <el-select v-model="form.agentId" filterable placeholder="选择已注册 Agent" style="width:460px">
            <el-option v-for="a in agents" :key="a.id" :value="a.id"
              :label="`#${a.id} ${a.name}(${a.host || '?'}) ${a.online ? '● 在线' : '○ 离线'}`" />
          </el-select>
          <el-text v-if="!agents.length" size="small" type="warning" style="display:block;font-size:11px">
            还没有 Agent:数据源管理 → 注册 Agent 拿注册码 → 在业务机启动 dcp-agent</el-text>
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

    <!-- ═══════ Step 2 同步方式 ═══════ -->
    <div v-if="step === 1" class="section">
      <div class="sec-title">同步方式(内容怎么同步)</div>
      <div class="mode-cards">
        <div class="mode-card" :class="{ sel: form.scope === 'incremental' }" @click="form.scope = 'incremental'">
          <div class="mc-head">
            <b>增量同步</b><el-tag size="small" type="success" effect="plain">推荐 · 常用</el-tag>
          </div>
          <div class="mc-desc">只把<b>新增</b>和<b>变更过</b>的文件同步过去;目标里已有的文件不重复传输。适合持续采集、按天落文件的场景,省流量、速度快。</div>
        </div>
        <div class="mode-card" :class="{ sel: form.scope === 'full' }" @click="form.scope = 'full'">
          <div class="mc-head"><b>全量同步</b></div>
          <div class="mc-desc">把源目录<b>整体同步</b>到目标,同名文件以源为准覆盖;可进一步选择是否删除目标多余文件。适合整体搬迁、目录镜像。</div>
        </div>
      </div>
      <div class="sec-title mt16">同步计划(什么时候同步)</div>
      <el-form label-width="130px" style="max-width:1000px">
        <el-form-item label="计划">
          <el-radio-group v-model="form.plan">
            <el-radio-button value="manual">仅手动</el-radio-button>
            <el-radio-button value="interval">定时(间隔)</el-radio-button>
            <el-radio-button value="cron">定时(cron)</el-radio-button>
            <el-radio-button value="watch">实时同步</el-radio-button>
          </el-radio-group>
          <el-text size="small" type="info" style="display:block;font-size:11px">
            「定时」到点自动同步一轮;「实时同步」常驻监听源目录,新文件/变更近实时自动同步(需 mc watch 可用:对象存储源、真实文件系统均支持;容器内挂载目录受 Docker Desktop 事件限制可能不触发)</el-text>
        </el-form-item>
        <el-form-item v-if="form.plan === 'interval'" label="间隔(分钟)">
          <el-input-number v-model="form.intervalMin" :min="1" :max="1440" />
          <el-text size="small" type="info" style="margin-left:10px">每隔 N 分钟自动同步一轮(0 变化时自动跳过)</el-text>
        </el-form-item>
        <el-form-item v-if="form.plan === 'cron'" label="cron 表达式">
          <el-input v-model="form.cronExpr" style="width:300px" placeholder="如 0 0 2 * * ?(每天 02:00)" />
          <el-text size="small" type="info" style="display:block;font-size:11px">格式:秒 分 时 日 月 周;例:每天凌晨 2 点 = <code>0 0 2 * * ?</code></el-text>
        </el-form-item>
      </el-form>
    </div>

    <!-- ═══════ Step 3 同步策略 ═══════ -->
    <div v-if="step === 2" class="section">
      <div class="sec-title">同步策略</div>
      <el-form label-width="130px" style="max-width:1000px">
        <el-form-item label="同名文件处理">
          <el-radio-group v-model="form.overwritePolicy">
            <el-radio-button :value="'keep'" :disabled="form.scope === 'full'">保留目标已有</el-radio-button>
            <el-radio-button :value="'overwrite'">用源文件覆盖</el-radio-button>
          </el-radio-group>
          <el-text size="small" type="info" style="display:block;font-size:11px">
            增量同步默认「保留已有」(只搬新增);全量同步默认「覆盖」。选择会随同步方式自动调整,可手动改</el-text>
        </el-form-item>
        <el-form-item label="删除目标多余文件">
          <div style="display:flex;align-items:center;flex-wrap:wrap;gap:8px">
            <el-switch v-model="form.deleteExtra" />
            <el-text size="small" type="info">目标里源已不存在的文件也删掉,两边完全一致</el-text>
          </div>
        </el-form-item>
        <el-alert v-if="form.deleteExtra" type="error" :closable="false" show-icon style="max-width:620px;margin:0 0 12px 130px"
          title="注意:开启后目标中源没有的文件会被删除,请确认目标目录没有需要保留的历史文件" />
        <el-form-item label="排除文件(不同步)">
          <div style="display:flex;align-items:center;flex-wrap:wrap;gap:8px">
            <el-tag v-for="(x, i) in form.exclude" :key="i" closable size="small" @close="form.exclude.splice(i, 1)">{{ x }}</el-tag>
            <el-input v-model="excludeInput" size="small" style="width:220px" placeholder="如 *.tmp 或 **/temp/**"
              @keyup.enter="pushExclude" @blur="pushExclude" />
            <el-text size="small" type="info" style="flex-basis:100%">支持通配符:*.log = 所有 .log 文件;**/temp/** = 任意层级的 temp 目录</el-text>
          </div>
        </el-form-item>
        <el-form-item label="仅同步最近修改">
          <div style="display:flex;align-items:center;flex-wrap:wrap;gap:8px">
            <el-switch v-model="timeOn" />
            <template v-if="timeOn">
              <el-input-number v-model="form.timeNum" :min="1" :max="3650" size="small" style="width:110px" />
              <el-select v-model="form.timeUnit" size="small" style="width:90px">
                <el-option value="h" label="小时前" />
                <el-option value="d" label="天前" />
              </el-select>
            </template>
            <el-text size="small" type="info">只同步修改时间在最近这段时间内的文件,更早的不动</el-text>
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
        <el-descriptions-item label="同步方式">{{ scopeName }}<template v-if="form.execHost === 'agent'"> · 由 Agent#{{ form.agentId }} 执行</template></el-descriptions-item>
        <el-descriptions-item label="计划">{{ planName }}</el-descriptions-item>
        <el-descriptions-item label="策略">{{ policySummary || '默认(只搬新增,不覆盖)' }}</el-descriptions-item>
      </el-descriptions>
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
import { ref, computed, onMounted, watch } from 'vue'
import { useRouter, useRoute } from 'vue-router'
import { ElMessage } from 'element-plus'
import request from '../../../api/request'

const router = useRouter()
const route = useRoute()
const editId = computed(() => (route.params.id ? Number(route.params.id) : null))
const step = ref(0)
const saving = ref(false)
const excludeInput = ref('')
const timeOn = ref(false)
const datasources = ref([])
const agents = ref([])

const form = ref({
  name: '', description: '',
  sourceDatasourceId: null, sourcePath: '',
  execHost: 'platform', agentId: null,
  targetDatasourceId: null, targetBucket: 'raw', targetPrefix: '',
  scope: 'incremental', plan: 'manual', intervalMin: 5, cronExpr: '0 0 2 * * ?',
  overwritePolicy: 'keep', deleteExtra: false, exclude: [],
  timeNum: 7, timeUnit: 'd'
})

const typeLabel = t => ({ s3: 'S3/MinIO 对象存储', local: '本地目录' }[t] || t)
const dsOptions = computed(() => datasources.value.filter(d => d.type === 's3' || d.type === 'local'))
const s3Targets = computed(() => datasources.value.filter(d => d.type === 's3'))
const srcDs = computed(() => dsOptions.value.find(x => x.id === form.value.sourceDatasourceId))
const srcIsLocal = computed(() => srcDs.value?.type === 'local')

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

const scopeName = computed(() => (form.value.scope === 'full' ? '全量同步(同名覆盖)' : '增量同步(只搬新增)'))
const planName = computed(() => form.value.plan === 'interval' ? `每 ${form.value.intervalMin} 分钟自动` : form.value.plan === 'cron' ? `定时 ${form.value.cronExpr}` : form.value.plan === 'watch' ? '实时同步(常驻监听)' : '仅手动执行')
const srcDesc = computed(() => {
  const d = srcDs.value
  return d ? `${d.name}(${typeLabel(d.type)}) / ${form.value.sourcePath || '(整源)'}` : '—'
})
const dstDesc = computed(() => {
  const d = s3Targets.value.find(x => x.id === form.value.targetDatasourceId)
  return `${d ? d.name + ' · ' : '平台 MinIO · '}s3://${form.value.targetBucket}${form.value.targetPrefix ? '/' + form.value.targetPrefix : ''}`
})
const policySummary = computed(() => {
  const parts = []
  if (form.value.overwritePolicy === 'overwrite') parts.push('同名文件覆盖')
  else parts.push('保留目标已有')
  if (form.value.deleteExtra) parts.push('删除目标多余文件')
  if (form.value.exclude.length) parts.push(`排除 ${form.value.exclude.length} 条规则`)
  if (timeOn.value) parts.push(`仅最近 ${form.value.timeNum}${form.value.timeUnit === 'h' ? '小时' : '天'}修改`)
  return parts.join(' · ')
})

function onSourceChange() {
  // 本地目录 → 默认 agent 执行; S3 → 平台
  if (srcIsLocal.value) {
    form.value.execHost = 'agent'
  } else {
    form.value.execHost = 'platform'
    form.value.agentId = null
  }
}
function onScopeChange() {
  // 模式联动同名处理默认值
  if (form.value.scope === 'full') form.value.overwritePolicy = 'overwrite'
  else form.value.overwritePolicy = 'keep'
}
watch(() => form.value.scope, onScopeChange)
function pushExclude() {
  const v = excludeInput.value.trim()
  if (v && !form.value.exclude.includes(v)) form.value.exclude.push(v)
  excludeInput.value = ''
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
      form.value.scheduleMode = t.schedule_mode || 'none'
      form.value.intervalMin = t.interval_min || 5
      form.value.cronExpr = t.cron_expr || '0 0 2 * * ?'
      form.value.overwritePolicy = !!st.overwrite ? 'overwrite' : 'keep'
      form.value.deleteExtra = !!st.remove
      form.value.exclude = [...(st.exclude || [])]
      form.value.scope = !!st.overwrite ? 'full' : 'incremental'
      form.value.plan = t.schedule_mode === 'interval' ? 'interval' : t.schedule_mode === 'cron' ? 'cron' : (t.watch ? 'watch' : 'manual')
      if (st.timeFilter) {
        timeOn.value = true
        const m = String(st.timeFilter.value || '').match(/^(\d+)([hd])$/)
        if (m) { form.value.timeNum = Number(m[1]); form.value.timeUnit = m[2] }
      }
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
  if (form.value.plan === 'cron' && !form.value.cronExpr.trim()) return ElMessage.warning('请填写 cron 表达式')

  const strategy = {
    overwrite: form.value.overwritePolicy === 'overwrite',
    remove: form.value.deleteExtra,
    exclude: [...form.value.exclude],
    timeFilter: timeOn.value ? { dir: 'newer', value: `${form.value.timeNum}${form.value.timeUnit}` } : null
  }
  const isWatch = form.value.plan === 'watch'
  const scheduleMode = isWatch ? 'none' : form.value.plan === 'interval' ? 'interval' : form.value.plan === 'cron' ? 'cron' : 'none'
  const body = {
    name: form.value.name.trim(), description: form.value.description || null,
    sourceDatasourceId: form.value.sourceDatasourceId,
    sourcePath: String(form.value.sourcePath).trim(),
    execHost: form.value.execHost, agentId: form.value.execHost === 'agent' ? form.value.agentId : null,
    targetDatasourceId: form.value.targetDatasourceId || null,
    targetBucket: form.value.targetBucket.trim(), targetPrefix: form.value.targetPrefix.trim() || '',
    strategy,
    watch: isWatch,
    scheduleMode, intervalMin: form.value.intervalMin,
    cronExpr: scheduleMode === 'cron' ? form.value.cronExpr : null
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
.mode-card { flex: 1 1 360px; max-width: 560px; min-width: 0; border: 1px solid #e4e7ed; border-radius: 10px; padding: 18px 20px; cursor: pointer; background: #fff; transition: all .15s; box-sizing: border-box; }
.mode-card:hover { border-color: #409eff; box-shadow: 0 2px 8px rgba(64, 158, 255, .12); }
.mode-card.sel { border-color: #409eff; background: #ecf5ff; }
.mc-head { display: flex; align-items: center; justify-content: space-between; gap: 8px; margin-bottom: 10px; font-size: 15px; }
.mc-desc { font-size: 13px; color: #6b7785; line-height: 1.8; word-break: break-word; }
.nav-bar { position: sticky; bottom: 0; background: #fff; border-top: 1px solid #ebeef5; margin-top: 18px; padding: 14px 2px 6px; display: flex; align-items: center; gap: 12px; z-index: 5; }
.nav-hint { color: #e6a23c; }
</style>
