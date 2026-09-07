<template>
  <div class="page-card">
    <el-card shadow="never">
      <el-steps :active="step" align-center style="margin-bottom: 24px">
        <el-step title="基本信息" description="源/目标/模式" />
        <el-step title="抽取配置" description="表或 SQL" />
        <el-step title="字段映射" description="转换链" />
        <el-step title="调度与确认" description="Cron/重试" />
      </el-steps>

      <!-- 步骤1: 基本信息 -->
      <div v-show="step === 0">
        <el-form :model="job" label-width="110px" style="max-width: 720px">
          <el-form-item label="任务名称" required>
            <el-input v-model="job.jobName" placeholder="如: 订单增量同步" />
          </el-form-item>
          <el-form-item label="源数据源" required>
            <el-select v-model="job.sourceDatasourceId" filterable style="width: 100%" @change="loadSourceMeta">
              <el-option v-for="d in datasources" :key="d.id" :label="`${d.name} (${d.type})`" :value="d.id" />
            </el-select>
          </el-form-item>
          <el-form-item label="目标数据源" required>
            <el-select v-model="job.targetDatasourceId" filterable style="width: 100%">
              <el-option v-for="d in datasources" :key="d.id" :label="`${d.name} (${d.type})`" :value="d.id" />
            </el-select>
          </el-form-item>
          <el-form-item label="目标表名" required>
            <el-input v-model="job.targetTableName" placeholder="如: dw_order (不存在会自动建吗? 不会, 需提前建好)" />
          </el-form-item>
          <el-form-item label="同步模式" required>
            <el-radio-group v-model="job.syncMode">
              <el-radio-button value="full">全量</el-radio-button>
              <el-radio-button value="incremental">增量</el-radio-button>
              <el-radio-button value="cdc">CDC(实时)</el-radio-button>
            </el-radio-group>
          </el-form-item>
          <el-form-item v-if="job.syncMode === 'incremental'" label="增量字段" required>
            <el-input v-model="job.incrementalField" placeholder="如: id / update_time, 需已建索引" />
          </el-form-item>
          <el-form-item label="超时(分钟)">
            <el-input-number v-model="job.timeoutMinutes" :min="1" :max="1440" />
          </el-form-item>
          <el-form-item label="并行度">
            <el-input-number v-model="job.parallelism" :min="1" :max="64" />
          </el-form-item>
          <el-form-item label="失败重试次数">
            <el-input-number v-model="job.retryTimes" :min="0" :max="10" />
          </el-form-item>
          <el-form-item label="重试间隔(秒)">
            <el-input-number v-model="job.retryIntervalSeconds" :min="5" :max="600" />
          </el-form-item>
          <el-form-item label="失败告警">
            <el-switch v-model="job.alarmEnabled" :active-value="1" :inactive-value="0" />
          </el-form-item>
          <el-form-item label="描述">
            <el-input v-model="job.description" type="textarea" :rows="2" />
          </el-form-item>
        </el-form>
      </div>

      <!-- 步骤2: 抽取配置 -->
      <div v-show="step === 1">
        <el-form label-width="110px" style="max-width: 960px">
          <el-form-item label="抽取方式">
            <el-radio-group v-model="extract.sourceType">
              <el-radio-button value="table">单表抽取</el-radio-button>
              <el-radio-button value="sql">自定义 SQL</el-radio-button>
            </el-radio-group>
          </el-form-item>
          <el-form-item v-if="extract.sourceType === 'table'" label="源表" required>
            <el-select v-model="extract.content" filterable style="width: 360px" placeholder="从源库拉取表清单">
              <el-option v-for="t in sourceTables" :key="t.name" :label="t.name" :value="t.name" />
            </el-select>
            <el-button style="margin-left: 10px" @click="loadSourceTables">刷新表清单</el-button>
          </el-form-item>
          <el-form-item v-else label="抽取 SQL" required>
            <el-input v-model="extract.content" type="textarea" :rows="6" style="width: 100%"
                      placeholder="SELECT id, user_id, price, quantity, discount, status, created_at FROM t_order" />
          </el-form-item>
          <el-form-item label="操作">
            <el-button type="primary" @click="previewData">数据预览 (前 20 行)</el-button>
          </el-form-item>
        </el-form>
        <el-table v-if="preview.columns.length" :data="previewRows" size="small" border max-height="320">
          <el-table-column v-for="c in preview.columns" :key="c" :prop="c" :label="c" min-width="100" show-overflow-tooltip />
        </el-table>
      </div>

      <!-- 步骤3: 字段映射 -->
      <div v-show="step === 2">
        <div class="toolbar">
          <div>
            <el-button type="primary" plain @click="autoMap">自动映射(按名称)</el-button>
            <el-button type="success" plain @click="addMapping">新增行</el-button>
            <el-button type="warning" plain @click="previewTransform">转换预览</el-button>
          </div>
          <el-text type="info" size="small">转换链: 直接映射 → 字典(CASE WHEN) → Aviator 表达式, 表达式可引用前置输出字段</el-text>
        </div>
        <el-table :data="job.fieldMappings" border size="small">
          <el-table-column label="排序" width="60">
            <template #default="{ $index }">{{ $index + 1 }}</template>
          </el-table-column>
          <el-table-column label="源字段" width="190">
            <template #default="{ row }">
              <el-select v-model="row.sourceField" filterable allow-create default-first-option style="width: 100%">
                <el-option v-for="c in sourceColumns" :key="c" :label="c" :value="c" />
              </el-select>
            </template>
          </el-table-column>
          <el-table-column label="目标字段" width="180">
            <template #default="{ row }">
              <el-input v-model="row.targetField" placeholder="目标列名" />
            </template>
          </el-table-column>
          <el-table-column label="转换类型" width="130">
            <template #default="{ row }">
              <el-select v-model="row.transformType" style="width: 100%">
                <el-option label="直接映射" value="direct" />
                <el-option label="字典转换" value="dictionary" />
                <el-option label="表达式" value="expression" />
              </el-select>
            </template>
          </el-table-column>
          <el-table-column label="规则" min-width="280">
            <template #default="{ row }">
              <el-select v-if="row.transformType === 'dictionary'" v-model="row.transformRule" placeholder="选择字典" style="width: 100%">
                <el-option v-for="d in dicts" :key="d.id" :label="d.dictName" :value="d.dictName" />
              </el-select>
              <el-input v-else-if="row.transformType === 'expression'" v-model="row.transformRule"
                        placeholder="Aviator 表达式, 如: price * quantity * (1 - discount)" />
              <span v-else style="color: #909399">-</span>
            </template>
          </el-table-column>
          <el-table-column label="操作" width="80">
            <template #default="{ $index }">
              <el-button size="small" type="danger" text @click="job.fieldMappings.splice($index, 1)">删除</el-button>
            </template>
          </el-table-column>
        </el-table>
      </div>

      <!-- 步骤4: 调度与确认 -->
      <div v-show="step === 3">
        <el-form :model="job" label-width="110px" style="max-width: 720px">
          <el-form-item label="Cron 表达式">
            <el-input v-model="job.cronExpr" placeholder="留空=仅手动触发, 如 0 2 * * *" style="width: 260px" />
            <el-button style="margin-left: 10px" @click="previewCron">最近5次触发</el-button>
          </el-form-item>
          <el-form-item v-if="cronTimes.length" label="触发预览">
            <el-tag v-for="t in cronTimes" :key="t" size="small" style="margin-right: 6px">{{ t }}</el-tag>
          </el-form-item>
          <el-form-item label="misfire 策略">
            <el-select v-model="job.misfirePolicy" style="width: 260px">
              <el-option label="DO_NOTHING (错过跳过)" value="DO_NOTHING" />
              <el-option label="FIRE_AND_PROCEED" value="FIRE_AND_PROCEED" />
            </el-select>
          </el-form-item>
        </el-form>
        <el-descriptions :column="2" border style="max-width: 720px">
          <el-descriptions-item label="任务名">{{ job.jobName }}</el-descriptions-item>
          <el-descriptions-item label="同步模式">{{ job.syncMode }}</el-descriptions-item>
          <el-descriptions-item label="源">
            {{ datasourceName(job.sourceDatasourceId) }}
          </el-descriptions-item>
          <el-descriptions-item label="目标">
            {{ datasourceName(job.targetDatasourceId) }}.{{ job.targetTableName }}
          </el-descriptions-item>
          <el-descriptions-item label="抽取">{{ extract.sourceType === 'table' ? '表 ' + extract.content : 'SQL' }}</el-descriptions-item>
          <el-descriptions-item label="映射数">{{ job.fieldMappings.length }}</el-descriptions-item>
        </el-descriptions>
      </div>

      <div style="margin-top: 24px; display: flex; justify-content: space-between">
        <el-button v-if="step > 0" @click="step--">上一步</el-button>
        <div>
          <el-button v-if="step < 3" type="primary" @click="nextStep">下一步</el-button>
          <el-button v-else type="success" :loading="saving" @click="save">保 存</el-button>
        </div>
      </div>
    </el-card>

    <!-- 转换预览对话框 -->
    <el-dialog v-model="transformDialog" title="转换预览 (源数据前 5 行)" width="900px">
      <el-table :data="transformPreviewRows" size="small" border max-height="420">
        <el-table-column v-for="c in transformPreviewColumns" :key="c" :prop="c" :label="c" min-width="110" show-overflow-tooltip />
      </el-table>
    </el-dialog>
  </div>
</template>

<script setup>
import { onMounted, reactive, ref } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { ElMessage, ElMessageBox } from 'element-plus'
import {
  listDatasources, datasourceTables, datasourceColumns, datasourcePreview,
  getJob, createJob, updateJob, allDicts, validateExpression, cronPreview, autoMapping
} from '../../api'

const route = useRoute()
const router = useRouter()
const editingId = route.params.id
const step = ref(0)
const saving = ref(false)
const datasources = ref([])
const sourceTables = ref([])
const sourceColumns = ref([])
const dicts = ref([])
const preview = reactive({ columns: [], rows: [] })
const previewRows = ref([])
const cronTimes = ref([])
const transformDialog = ref(false)
const transformPreviewColumns = ref([])
const transformPreviewRows = ref([])

const job = reactive({
  id: null, jobName: '', sourceDatasourceId: null, targetDatasourceId: null,
  targetTableName: '', syncMode: 'incremental', incrementalField: 'id', cronExpr: '',
  misfirePolicy: 'DO_NOTHING', enabled: 1, timeoutMinutes: 30, parallelism: 2,
  retryTimes: 2, retryIntervalSeconds: 10, alarmEnabled: 1, description: '',
  fieldMappings: []
})
const extract = reactive({ sourceType: 'table', content: '', previewLimit: 100 })

function datasourceName(id) {
  return datasources.value.find(d => d.id === id)?.name || id
}

async function loadDatasources() {
  const data = await listDatasources({ page: 1, pageSize: 100 })
  datasources.value = data.list
}

async function loadSourceMeta() {
  sourceTables.value = []
  sourceColumns.value = []
  if (job.sourceDatasourceId) {
    loadSourceTables()
  }
}

async function loadSourceTables() {
  if (!job.sourceDatasourceId) return
  try {
    const tables = await datasourceTables(job.sourceDatasourceId)
    sourceTables.value = tables
  } catch (e) { /* ignore */ }
}

async function loadSourceColumns() {
  if (!job.sourceDatasourceId || !extract.content) return
  if (extract.sourceType === 'table') {
    try {
      const cols = await datasourceColumns(job.sourceDatasourceId, extract.content)
      sourceColumns.value = cols.map(c => c.name)
    } catch (e) { /* ignore */ }
  }
}

async function previewData() {
  if (!job.sourceDatasourceId || !extract.content) {
    ElMessage.warning('请先选择源数据源并填写抽取内容')
    return
  }
  const sql = extract.sourceType === 'table' ? `SELECT * FROM ${extract.content}` : extract.content
  try {
    const data = await datasourcePreview(job.sourceDatasourceId, sql, 20)
    preview.columns = data.columns
    preview.rows = data.rows
    previewRows.value = data.rows.map(r => {
      const o = {}
      data.columns.forEach((c, i) => { o[c] = r[i] })
      return o
    })
    ElMessage.success(`预览成功, 耗时 ${data.durationMs}ms`)
  } catch (e) { /* ignore */ }
}

async function autoMap() {
  await loadSourceColumns()
  if (!sourceColumns.value.length) {
    ElMessage.warning('请先在「抽取配置」中选择源表/SQL 并拉取字段')
    return
  }
  const targetCols = await ElMessageBox.prompt('目标表字段(逗号分隔, 可留空自动同名)', '自动映射', {
    inputValue: '', inputPlaceholder: '如: id,user_id,price,quantity,discount,status_desc,order_date,total_amount'
  }).then(r => r.value).catch(() => null)
  if (targetCols === null) return
  const targetList = targetCols.split(',').map(s => s.trim()).filter(Boolean)
  const maps = await autoMapping({ sourceColumns: sourceColumns.value, targetColumns: targetList })
  job.fieldMappings = maps.map(m => ({ ...m, sortNo: 0 }))
  ElMessage.success(`自动生成 ${maps.length} 条映射, 可手动调整`)
}

function addMapping() {
  job.fieldMappings.push({ sourceField: '', targetField: '', transformType: 'direct', transformRule: '', sortNo: 0 })
}

async function previewTransform() {
  if (!job.fieldMappings.length) {
    ElMessage.warning('请先配置字段映射')
    return
  }
  // 用源数据预览前5行模拟转换链
  if (!job.sourceDatasourceId || !extract.content) {
    ElMessage.warning('请先配置抽取')
    return
  }
  const sql = extract.sourceType === 'table' ? `SELECT * FROM ${extract.content}` : extract.content
  const data = await datasourcePreview(job.sourceDatasourceId, sql, 5)
  const cols = data.columns
  const targetCols = job.fieldMappings.map(m => m.targetField)
  transformPreviewColumns.value = targetCols
  transformPreviewRows.value = data.rows.map(raw => {
    const row = {}
    cols.forEach((c, i) => { row[c] = raw[i] })
    const out = {}
    for (const m of job.fieldMappings) {
      if (m.transformType === 'dictionary') {
        out[m.targetField] = row[m.sourceField]
      } else if (m.transformType === 'expression') {
        out[m.targetField] = `≈ ${m.transformRule}`
      } else {
        out[m.targetField] = row[m.sourceField]
      }
    }
    return out
  })
  transformDialog.value = true
}

async function previewCron() {
  if (!job.cronExpr) return
  try {
    cronTimes.value = await cronPreview(job.cronExpr)
  } catch (e) { /* ignore */ }
}

async function nextStep() {
  if (step.value === 0) {
    if (!job.jobName || !job.sourceDatasourceId || !job.targetDatasourceId || !job.targetTableName) {
      ElMessage.warning('请填写基本信息')
      return
    }
    if (job.syncMode === 'incremental' && !job.incrementalField) {
      ElMessage.warning('增量模式必须填写增量字段')
      return
    }
  }
  if (step.value === 1) {
    if (!extract.content) {
      ElMessage.warning('请填写抽取内容')
      return
    }
    if (extract.sourceType === 'table' && !sourceColumns.value.length) {
      await loadSourceColumns()
    }
  }
  if (step.value === 2) {
    if (!job.fieldMappings.length) {
      ElMessage.warning('请至少配置一条字段映射')
      return
    }
    // 表达式本地校验
    for (const m of job.fieldMappings) {
      if (m.transformType === 'expression' && m.transformRule) {
        const fields = [...sourceColumns.value, ...job.fieldMappings.map(x => x.targetField)]
        const r = await validateExpression({ expression: m.transformRule, availableFields: fields })
        if (!r.valid) {
          ElMessage.error(`表达式「${m.targetField}」校验失败: ${r.message}`)
          return
        }
      }
    }
  }
  step.value++
}

async function save() {
  if (!job.fieldMappings.length) {
    ElMessage.warning('请配置字段映射')
    return
  }
  saving.value = true
  try {
    const payload = {
      ...job,
      cronExpr: job.cronExpr || null,
      extractConfig: { ...extract },
      fieldMappings: job.fieldMappings.map((m, i) => ({ ...m, sortNo: i + 1 }))
    }
    if (editingId) {
      await updateJob(editingId, payload)
      ElMessage.success('任务已更新')
    } else {
      await createJob(payload)
      ElMessage.success('任务已创建')
    }
    router.push('/job')
  } finally {
    saving.value = false
  }
}

onMounted(async () => {
  await Promise.all([loadDatasources(), allDicts().then(d => { dicts.value = d })])
  if (editingId) {
    const detail = await getJob(editingId)
    const j = detail.job
    Object.assign(job, {
      id: j.id, jobName: j.jobName, sourceDatasourceId: j.sourceDatasourceId,
      targetDatasourceId: j.targetDatasourceId, targetTableName: j.targetTableName,
      syncMode: j.syncMode, incrementalField: j.incrementalField, cronExpr: j.cronExpr || '',
      misfirePolicy: j.misfirePolicy, enabled: j.enabled, timeoutMinutes: j.timeoutMinutes,
      parallelism: j.parallelism, retryTimes: j.retryTimes, retryIntervalSeconds: j.retryIntervalSeconds,
      alarmEnabled: j.alarmEnabled, description: j.description, fieldMappings: detail.mappings || []
    })
    if (detail.extract) {
      extract.sourceType = detail.extract.sourceType
      extract.content = detail.extract.content
      extract.previewLimit = detail.extract.previewLimit
    }
    await loadSourceMeta()
    await loadSourceColumns()
  }
})
</script>
