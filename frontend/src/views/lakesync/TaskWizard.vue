<template>
  <div class="page-card">
    <el-card shadow="never">
      <template #header>
        {{ editId ? '编辑同步任务' : '新建同步任务' }}
        <el-text type="info" style="margin-left: 8px">一任务一表 · 数据获取统一走消费侧逻辑</el-text>
      </template>

      <el-steps :active="step" align-center finish-status="success" style="margin-bottom: 26px">
        <el-step title="选源表" />
        <el-step title="目标湖表" />
        <el-step title="执行与调度" />
        <el-step :title="editId ? '确认修改' : '确认创建'" />
      </el-steps>

      <!-- ① 选源表 -->
      <el-form v-if="step === 0" label-width="110px" style="max-width: 760px">
        <el-form-item label="源表" required>
          <el-select v-model="form.logicalTable" filterable allow-create placeholder="选择要同步的 TiDB 逻辑表"
                     style="width: 380px" :disabled="!!editId" @change="onTableChange">
            <el-option v-for="t in tables" :key="t.name" :label="`${t.name} · ${t.rows ?? 0} 行`" :value="t.name" />
          </el-select>
          <el-text type="info" style="margin-left: 8px">一任务一表:同一源表在未停用任务中唯一</el-text>
        </el-form-item>
        <el-form-item label="业务键" required>
          <el-select v-model="form.entityKeyColumns" multiple filterable allow-create default-first-option
                     placeholder="湖表 MERGE 主键(默认取源表主键)" style="width: 380px">
            <el-option v-for="c in columns" :key="c" :label="c" :value="c" />
          </el-select>
        </el-form-item>
        <el-form-item label="同步列">
          <el-select v-model="form.syncColumns" multiple filterable allow-create default-first-option
                     placeholder="留空 = 全部列" style="width: 560px">
            <el-option v-for="c in columns" :key="c" :label="c" :value="c" />
          </el-select>
        </el-form-item>
        <el-form-item label="行过滤">
          <el-input v-model="form.rowFilter" placeholder="可选,简单谓词如 status = 'PAID'" style="width: 560px" />
          <el-text type="info">交给消费侧下推,不允许子查询</el-text>
        </el-form-item>
        <el-form-item label="删除墓碑">
          <el-switch v-model="form.tombstoneEnabled" />
          <el-input v-if="form.tombstoneEnabled" v-model="form.tombstoneColumn" placeholder="墓碑列名" style="width: 200px; margin-left: 12px" />
        </el-form-item>
      </el-form>

      <!-- ② 目标湖表 -->
      <el-form v-else-if="step === 1" label-width="110px" style="max-width: 760px">
        <el-form-item label="catalog" required>
          <el-select v-model="form.lakeCatalog" style="width: 240px">
            <el-option label="demo" value="demo" />
          </el-select>
        </el-form-item>
        <el-form-item label="namespace" required>
          <el-input v-model="form.lakeNamespace" style="width: 240px" />
        </el-form-item>
        <el-form-item label="表名" required>
          <el-input v-model="form.lakeTable" style="width: 240px" />
        </el-form-item>
        <el-form-item label="写入模式" required>
          <el-radio-group v-model="form.writeMode">
            <el-radio value="merge">MERGE 整行镜像(推荐)</el-radio>
            <el-radio value="append">APPEND</el-radio>
          </el-radio-group>
        </el-form-item>
        <el-form-item label="湖表键">
          <el-input :model-value="form.entityKeyColumns.join(', ')" disabled style="width: 380px" />
        </el-form-item>

        <el-card shadow="never" class="readpath">
          <div class="rp-title">读路径(平台自动,不可配)</div>
          <div class="rp-row"><span>读入口</span><code>契约视图 v_outlet_{{ consumerId }}_{{ form.logicalTable || '—' }}</code></div>
          <div class="rp-row"><span>位点</span><code>TSO(consumer_offset,跨进程可续)</code></div>
          <div class="rp-row"><span>每轮区间</span><code>[位点, WATERMARK]</code></div>
          <div class="rp-row"><span>首轮</span><code>AS OF T0 快照 + 主键游标分页</code></div>
        </el-card>
      </el-form>

      <!-- ③ 执行与调度 -->
      <el-form v-else-if="step === 2" label-width="110px" style="max-width: 760px">
        <el-form-item label="执行器">
          <el-radio-group v-model="form.executor">
            <el-radio value="auto">自动(推荐)</el-radio>
            <el-radio value="batch">批(B)</el-radio>
            <el-radio value="stream">流式(A)</el-radio>
          </el-radio-group>
          <el-text type="info" style="margin-left: 8px">自动 = 平台按数据量路由</el-text>
        </el-form-item>
        <el-form-item label="首载策略" required>
          <el-radio-group v-model="form.firstLoadPolicy">
            <el-radio value="full">首轮全量(推荐)</el-radio>
            <el-radio value="latest">从最新起(仅增量)</el-radio>
            <el-radio value="tso">指定 TSO</el-radio>
          </el-radio-group>
        </el-form-item>
        <el-form-item v-if="form.firstLoadPolicy === 'tso'" label="起始 TSO">
          <el-input v-model="form.boundaryTso" placeholder="必须在 GC 窗口内(168h)" style="width: 300px" />
        </el-form-item>
        <el-form-item label="调度" required>
          <el-radio-group v-model="form.schedule">
            <el-radio value="manual">手动</el-radio>
            <el-radio value="interval">间隔</el-radio>
            <el-radio value="cron">cron</el-radio>
          </el-radio-group>
        </el-form-item>
        <el-form-item v-if="form.schedule === 'interval'" label="间隔(分钟)">
          <el-input-number v-model="form.intervalMin" :min="1" :max="1440" />
        </el-form-item>
        <el-form-item v-if="form.schedule === 'cron'" label="cron 表达式">
          <el-input v-model="form.cronExpr" placeholder="0 2 * * *" style="width: 240px" />
        </el-form-item>
        <el-form-item v-if="!editId" label="创建后启用">
          <el-switch v-model="form.enabled" />
        </el-form-item>

        <el-alert type="info" :closable="false" show-icon
          title="单批最大行数 / 并发分片数 / 失败重试等由后台策略统一控制(不在页面暴露),避免每个任务各配一套数。"
          style="max-width: 760px" />
      </el-form>

      <!-- ④ 确认 -->
      <div v-else class="confirm">
        <el-descriptions :column="2" border>
          <el-descriptions-item label="源表">{{ form.logicalTable || '—' }}</el-descriptions-item>
          <el-descriptions-item label="业务键">{{ form.entityKeyColumns.join(', ') || '—' }}</el-descriptions-item>
          <el-descriptions-item label="同步列">{{ form.syncColumns.length ? form.syncColumns.join(', ') : '全部列' }}</el-descriptions-item>
          <el-descriptions-item label="行过滤">{{ form.rowFilter || '无' }}</el-descriptions-item>
          <el-descriptions-item label="目标湖表">{{ form.lakeCatalog }}.{{ form.lakeNamespace }}.{{ form.lakeTable }}</el-descriptions-item>
          <el-descriptions-item label="写入模式">{{ form.writeMode === 'merge' ? 'MERGE 整行镜像' : 'APPEND' }}</el-descriptions-item>
          <el-descriptions-item label="执行器">{{ executorText }}</el-descriptions-item>
          <el-descriptions-item label="首载策略">{{ firstLoadText }}</el-descriptions-item>
          <el-descriptions-item label="调度">{{ scheduleText }}</el-descriptions-item>
          <el-descriptions-item label="创建后启用">{{ form.enabled ? '是' : '否' }}</el-descriptions-item>
        </el-descriptions>

        <el-card shadow="never" class="readpath" style="margin-top: 14px">
          <div class="rp-title">平台将自动生成(无需人工配置)</div>
          <div class="rp-row"><span>消费方</span><code>{{ consumerId }}(内部契约,免审批)</code></div>
          <div class="rp-row"><span>契约</span><code>流={{ form.logicalTable || '—' }} · 列白名单={{ form.syncColumns.length ? form.syncColumns.join(',') : '全部列' }} · 行条件={{ form.rowFilter || '无' }}</code></div>
          <div class="rp-row"><span>位点起点</span><code>{{ form.firstLoadPolicy === 'tso' ? ('TSO=' + (form.boundaryTso || '—')) : (form.firstLoadPolicy === 'full' ? 'T0 = 当前 WATERMARK(首轮快照)' : '当前 WATERMARK(仅增量)') }}</code></div>
          <div class="rp-row"><span>首轮作业</span><code>{{ form.firstLoadPolicy === 'full' ? 'bootstrap(AS OF T0)→ 之后 incr_batch' : 'incr_batch' }}</code></div>
        </el-card>
      </div>

      <div class="foot">
        <el-button v-if="step > 0" @click="step--">上一步</el-button>
        <el-button v-if="step < 3" type="primary" :disabled="!canNext" @click="step++">下一步</el-button>
        <el-button v-else type="primary" :loading="saving" @click="submit">{{ editId ? '保存修改' : '创建任务' }}</el-button>
        <el-button @click="$router.back()">取消</el-button>
      </div>
    </el-card>
  </div>
</template>

<script setup>
import { ref, reactive, computed, onMounted } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { ElMessage } from 'element-plus'
import { lakeSyncCatalog, createLakeSyncTask, updateLakeSyncTask, getLakeSyncTask } from '../../api'

const route = useRoute()
const router = useRouter()
const editId = ref(route.params.id || null)
const step = ref(0)
const saving = ref(false)
const tables = ref([])
const columns = ref([])

const form = reactive({
  logicalTable: '',
  entityKeyColumns: [],
  syncColumns: [],
  rowFilter: '',
  tombstoneEnabled: false,
  tombstoneColumn: '_is_deleted',
  lakeCatalog: 'demo',
  lakeNamespace: 'dcp',
  lakeTable: '',
  writeMode: 'merge',
  executor: 'auto',
  firstLoadPolicy: 'full',
  boundaryTso: '',
  schedule: 'interval',
  intervalMin: 15,
  cronExpr: '',
  enabled: true
})

const consumerId = computed(() => `lake-${form.logicalTable || '—'}`)
const executorText = computed(() => ({ auto: '自动(平台路由)', batch: '批(B)', stream: '流式(A)' }[form.executor]))
const firstLoadText = computed(() => ({ full: '首轮全量(AS OF T0)', latest: '从最新起', tso: `指定 TSO` }[form.firstLoadPolicy]))
const scheduleText = computed(() => form.schedule === 'manual' ? '手动'
  : form.schedule === 'cron' ? `cron ${form.cronExpr || '—'}` : `每 ${form.intervalMin} 分钟`)

const canNext = computed(() => {
  if (step.value === 0) return !!form.logicalTable && form.entityKeyColumns.length > 0
  if (step.value === 1) return !!form.lakeNamespace && !!form.lakeTable
  if (step.value === 2) return form.schedule !== 'cron' || !!form.cronExpr
  return true
})

function onTableChange(name) {
  const t = tables.value.find(x => x.name === name)
  columns.value = t?.cols || []
  if (t && !form.syncColumns.length) form.syncColumns = [...columns.value]
  if (t && !form.entityKeyColumns.length) form.entityKeyColumns = ['id']
  if (!form.lakeTable) form.lakeTable = name
  // 增量键信息仅供参考(旧链路的 _seq 概念;消费侧位点走 TSO)
  form.incrementKeyHint = t?.incrementKey || ''
}

async function loadCatalog() {
  try {
    const c = await lakeSyncCatalog()
    tables.value = (c?.tables || []).map(t => ({ name: t.name, cols: t.cols || [], rows: t.rows, incrementKey: t.incrementKey }))
  } catch (e) { tables.value = [] }
}

async function loadTask() {
  if (!editId.value) return
  const t = await getLakeSyncTask(editId.value)
  form.logicalTable = t.logical_table
  form.lakeTable = t.lake_table || t.logical_table
  form.schedule = t.schedule || 'interval'
  form.intervalMin = t.interval_min || 15
  form.cronExpr = t.cron_expr || ''
  form.enabled = !!t.enabled
  columns.value = []
}

async function submit() {
  saving.value = true
  try {
    const payload = {
      // 后端当前已落库的字段
      logicalTable: form.logicalTable,
      schedule: form.schedule,
      intervalMin: form.intervalMin,
      cronExpr: form.cronExpr,
      enabled: form.enabled ? 1 : 0,
      status: form.schedule === 'manual' ? 'manual' : 'running',
      // TODO(接入消费侧): 以下字段待后端 lake_sync_task 加列后持久化(现由后端忽略)
      entityKeyColumns: form.entityKeyColumns,
      syncColumns: form.syncColumns,
      rowFilter: form.rowFilter || null,
      tombstone: { enabled: form.tombstoneEnabled, column: form.tombstoneColumn },
      lake: { catalog: form.lakeCatalog, namespace: form.lakeNamespace, table: form.lakeTable },
      writeMode: form.writeMode,
      executor: form.executor,
      firstLoadPolicy: form.firstLoadPolicy,
      boundaryTso: form.boundaryTso || null,
      consumerId: consumerId.value,
      streamId: form.logicalTable
    }
    if (editId.value) {
      await updateLakeSyncTask(editId.value, payload)
      ElMessage.success('已保存')
      router.push(`/lakesync/${editId.value}`)
    } else {
      const r = await createLakeSyncTask(payload)
      ElMessage.success('已创建')
      router.push(`/lakesync/${r?.id || ''}`)
    }
  } finally {
    saving.value = false
  }
}

onMounted(async () => {
  await Promise.all([loadCatalog(), loadTask()])
})
</script>

<style scoped>
.foot { margin-top: 18px; display: flex; gap: 8px; }
.readpath { background: #fafafa; border-left: 3px solid var(--el-color-info); }
.rp-title { font-weight: 600; font-size: 13px; margin-bottom: 8px; }
.rp-row { display: flex; gap: 10px; font-size: 12.5px; color: #606266; padding: 3px 0; }
.rp-row span { min-width: 72px; color: #909399; }
code { font-family: monospace; font-size: 12px; background: #f2f3f5; padding: 1px 5px; border-radius: 4px; word-break: break-all; }
.confirm { max-width: 900px; }
</style>
