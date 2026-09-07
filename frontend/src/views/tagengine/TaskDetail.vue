<template>
  <div class="page-card">
    <div v-if="task" class="head">
      <div>
        <el-button size="small" @click="router.push('/tagengine/task')">← 返回</el-button>
        <span class="title">{{ task.name }}</span>
        <el-tag size="small" effect="plain">v{{ task.version }}</el-tag>
        <el-tag size="small" :type="statusMap[task.status].type">{{ statusMap[task.status].label }}</el-tag>
        <el-tag size="small" type="info" effect="plain" style="margin-left:6px">{{ task.engine }}</el-tag>
        <el-tag v-if="task.guard" size="small" type="warning" effect="plain">护栏开启</el-tag>
      </div>
      <div>
        <el-button size="small" @click="toast('刷新(mock)')">刷新</el-button>
        <el-button type="success" size="small" @click="toast('试跑 1% 采样(mock)')">试跑</el-button>
        <el-button type="primary" size="small" @click="toast('立即执行(mock)')">立即执行</el-button>
        <el-button type="danger" size="small" plain @click="toast('停用任务(mock)')">停用</el-button>
      </div>
    </div>

    <div v-if="task" style="margin-top:12px">
      <el-row :gutter="12">
        <el-col :span="6"><el-card shadow="never" class="stat"><div class="stat-num">{{ stat.hits }}</div><div class="stat-label">当前命中实体</div></el-card></el-col>
        <el-col :span="6"><el-card shadow="never" class="stat"><div class="stat-num">{{ stat.coverage }}%</div><div class="stat-label">覆盖率</div></el-card></el-col>
        <el-col :span="6"><el-card shadow="never" class="stat"><div class="stat-num">{{ stat.runs }}</div><div class="stat-label">运行轮次</div></el-card></el-col>
        <el-col :span="6"><el-card shadow="never" class="stat"><div class="stat-num bad">{{ stat.failedRuns }}</div><div class="stat-label">失败/告警轮次</div></el-card></el-col>
      </el-row>

      <el-card shadow="never" style="margin-top:12px">
        <el-tabs v-model="tab">
          <!-- 规则 -->
          <el-tab-pane label="规则与 IR" name="rule">
            <el-descriptions :column="2" border size="small" style="max-width:860px">
              <el-descriptions-item label="数据集">{{ task.models.join(' + ') }}<el-text type="info" style="font-size:11px"> 实体: {{ task.entityKey }}</el-text></el-descriptions-item>
              <el-descriptions-item label="时间窗">近 90 天(order_date)</el-descriptions-item>
              <el-descriptions-item label="产出标签">{{ task.tags.join(', ') }}</el-descriptions-item>
              <el-descriptions-item label="调度">{{ task.schedule }} · 冲突: 后发布覆盖 · ttl 1d</el-descriptions-item>
            </el-descriptions>
            <div style="display:flex;gap:12px;margin-top:12px">
              <!-- 可视化任务: 算子链 + 血缘 -->
              <template v-if="task.mode !== 'script' && task.mode !== 'sql'">
                <div style="flex:1">
                  <div class="sub-t">算子链</div>
                  <div v-for="(s, i) in ruleSteps" :key="i" class="chain">
                    <span class="chain-no">{{ i + 1 }}</span>
                    <el-tag size="small" :type="s.t" effect="plain">{{ s.label }}</el-tag>
                    <el-text size="small" type="info">{{ s.desc }}</el-text>
                  </div>
                </div>
                <div style="flex:1">
                  <div class="sub-t">血缘</div>
                  <el-steps direction="vertical" :active="3" finish-status="success" style="max-width:380px">
                    <el-step title="源数据集" description="customer_wide / order_wide / tag_fact(黑名单)" />
                    <el-step title="规则(RFM 分层 v3)" description="聚合近 90 天消费与频次 → 条件 → 排除黑名单" />
                    <el-step title="产出标签" description="客户分层/高价值 · 客户分层/中价值" />
                    <el-step title="下游消费" description="订阅中心 · 资产详情 · 运营圈选" />
                  </el-steps>
                </div>
              </template>

              <!-- SQL 任务: 规则文本 + 契约 -->
              <div v-else-if="task.mode === 'sql'" style="flex:1;min-width:0">
                <div class="sub-t">规则(SQL 声明 · 值型映射)</div>
                <pre class="code-pre">{{ sqlText }}</pre>
                <el-text size="small" type="info">产物: 单个值型标签「数据密级」, value ∈ 公开/内部/机密/绝密 —— 一个任务打完整个枚举域, 不拆 N 个任务</el-text>
              </div>

              <!-- 脚本任务: 代码 + 契约 -->
              <div v-else style="flex:1;min-width:0">
                <div class="sub-t">脚本(PySpark · 在线编辑器 · v2)</div>
                <pre class="code-pre">{{ scriptText }}</pre>
                <el-descriptions :column="1" border size="small" style="max-width:640px;margin-top:10px">
                  <el-descriptions-item label="输出契约">ctx.emit(entity_key, tag_value) → 平台校验 schema 后落 tag_fact</el-descriptions-item>
                  <el-descriptions-item label="读数据集(血缘)">ds.customer_wide · ds.churn_model_output</el-descriptions-item>
                  <el-descriptions-item label="资源/护栏">driver 2C/4G · 超时 30min · 突变护栏开启</el-descriptions-item>
                </el-descriptions>
              </div>
            </div>
          </el-tab-pane>

          <!-- 运行历史 -->
          <el-tab-pane label="运行历史" name="runs">
            <el-table :data="runs" size="small" border stripe>
              <el-table-column prop="id" label="run" width="70" />
              <el-table-column prop="time" label="开始时间" width="170" />
              <el-table-column prop="trigger" label="触发" width="90" />
              <el-table-column label="版本" width="70"><template #default="{ row }">v{{ row.ver }}</template></el-table-column>
              <el-table-column label="状态" width="110">
                <template #default="{ row }">
                  <el-tag size="small" :type="runStatusType(row.status)" effect="plain">{{ runStatusName(row.status) }}</el-tag>
                </template>
              </el-table-column>
              <el-table-column prop="hits" label="命中实体" width="120" />
              <el-table-column label="变化率" width="100">
                <template #default="{ row }">
                  <el-text :type="row.warn ? 'danger' : 'info'">{{ row.delta }}</el-text>
                </template>
              </el-table-column>
              <el-table-column prop="duration" label="耗时" width="90" />
              <el-table-column prop="engine" label="引擎" width="80" />
              <el-table-column prop="note" label="说明" min-width="160" show-overflow-tooltip />
            </el-table>
          </el-tab-pane>

          <!-- 标签产出分布 -->
          <el-tab-pane label="标签产出" name="tags">
            <el-table :data="tagDist" size="small" border stripe style="max-width:640px">
              <el-table-column prop="tag" label="标签" min-width="160" />
              <el-table-column label="命中实体" width="200">
                <template #default="{ row }">
                  <el-progress :percentage="pct(row)" :stroke-width="10" style="width:160px" />
                </template>
              </el-table-column>
              <el-table-column prop="hits" label="数量" width="110" />
            </el-table>
            <div class="sub-t" style="margin-top:14px">圈选加速(生产: Bitmap)</div>
            <el-text size="small" type="info">标签已物化为 roaring bitmap(每标签一个位图), 圈选/交集/并集毫秒级; 事实层与位图层由服务同步</el-text>
          </el-tab-pane>
        </el-tabs>
      </el-card>
    </div>
  </div>
</template>

<script setup>
import { ref, onMounted } from 'vue'
import { useRouter } from 'vue-router'
import { ElMessage } from 'element-plus'

const router = useRouter()
const toast = (m) => ElMessage.info(m)
const tab = ref('rule')
const id = Number(router.currentRoute.value.params.id)
const statusMap = {
  DRAFT: { label: '草稿', type: 'info' }, REVIEW: { label: '待审批', type: 'warning' },
  APPROVED: { label: '已审批', type: 'primary' }, RUNNING: { label: '计算中', type: 'warning' },
  ONLINE: { label: '已上线', type: 'success' }, FAILED: { label: '失败', type: 'danger' },
  OFFLINE: { label: '已停用', type: 'info' }
}
const task = ref(null)
const stat = ref({})
const ruleSteps = [
  { t: 'success', label: '聚合加工', desc: 'group by customer_id → SUM(amount) / COUNT(DISTINCT order_no) (近 90 天)' },
  { t: 'warning', label: '条件过滤', desc: 'amount_90d ≥ 5000 OR orders_90d ≥ 3' },
  { t: 'primary', label: '标签运算', desc: '排除 风控/黑名单(读 tag_fact, DAG 依赖)' },
  { t: 'success', label: '输出打标', desc: '命中实体写 tag_fact: v3 新 valid 区间, 双版本并行' }
]
const runs = []
const tagDist = ref([])
function barPct(row) {
  const mx = tagDist.value.reduce((a, b) => Math.max(a, b.hits || 0), 1)
  return Math.round((row.hits || 0) / mx * 100)
}
const runStatusName = s => ({ SUCCESS: '成功', FAILED: '失败', WARN: '告警', RUNNING: '运行中' }[s])
const runStatusType = s => ({ SUCCESS: 'success', FAILED: 'danger', WARN: 'warning', RUNNING: 'warning' }[s])
const sqlText = `SELECT customer_id AS entity_key,
       CASE data_secret WHEN 4 THEN '绝密' WHEN 3 THEN '机密'
                        WHEN 2 THEN '内部' ELSE '公开' END AS tag_value
FROM customer_wide WHERE data_secret IS NOT NULL`
const scriptText = `def run(ctx):
    cust = ctx.table("ds.customer_wide")
    churn = ctx.table("ds.churn_model_output")
    df = cust.join(churn, "customer_id")
             .select("customer_id", F.col("churn_prob").cast("double").alias("tag_value"))
    return ctx.emit(df)`

onMounted(() => {
  if (id === 9) {
    task.value = {
      id, name: '流失概率打分(模型分→值型标签)', version: 2, status: 'ONLINE', engine: 'Spark', mode: 'script',
      models: ['customer_wide', 'churn_model_output'], entityKey: 'customer_id',
      tags: ['模型/流失概率(值: 0~1)'], schedule: '每日 04:00', guard: true
    }
    stat.value = { hits: '382,101', coverage: '96.4', runs: 14, failedRuns: 0 }
    runs.push(
      { id: 14, time: '2026-09-07 04:00:22', trigger: '定时', ver: 2, status: 'SUCCESS', hits: '382,101', delta: '+0.1%', duration: '11m40s', engine: 'Spark', note: '模型版本 v7 输出' },
      { id: 13, time: '2026-09-06 04:00:10', trigger: '定时', ver: 1, status: 'SUCCESS', hits: '381,900', delta: '—', duration: '12m02s', engine: 'Spark', note: '首次上线' }
    )
    tagDist.value.push({ tag: '流失概率 ≥0.8', hits: 28341 }, { tag: '0.5~0.8', hits: 57220 }, { tag: '0.2~0.5', hits: 103880 }, { tag: '<0.2', hits: 192660 })
    return
  }
  if (id === 10) {
    task.value = {
      id, name: '数据密级映射(枚举值型)', version: 1, status: 'APPROVED', engine: 'SQL-on-TiDB', mode: 'sql',
      models: ['customer_wide'], entityKey: 'customer_id',
      tags: ['密级/数据密级(公开|内部|机密|绝密)'], schedule: '每日 05:00', guard: true
    }
    stat.value = { hits: '—', coverage: '—', runs: 0, failedRuns: 0 }
    tagDist.value.push({ tag: '公开', hits: 41020 }, { tag: '内部', hits: 152300 }, { tag: '机密', hits: 120400 }, { tag: '绝密', hits: 18300 })
    return
  }
  task.value = {
    id, name: 'RFM-高价值客户分层', version: 3, status: 'ONLINE', engine: 'Spark',
    models: ['customer_wide', 'order_wide'], entityKey: 'customer_id',
    tags: ['客户分层/高价值', '客户分层/中价值'], schedule: '每日 02:00', guard: true
  }
  stat.value = { hits: '128,540', coverage: '34.2', runs: 26, failedRuns: 1 }
  runs.push(
    { id: 26, time: '2026-09-07 02:00:31', trigger: '定时', ver: 3, status: 'SUCCESS', hits: '128,540', delta: '+0.4%', duration: '6m12s', engine: 'Spark', note: '' },
    { id: 25, time: '2026-09-06 02:00:12', trigger: '定时', ver: 3, status: 'SUCCESS', hits: '128,010', delta: '+0.1%', duration: '6m05s', engine: 'Spark', note: '' },
    { id: 24, time: '2026-09-05 02:00:45', trigger: '定时', ver: 3, status: 'WARN', hits: '127,902', delta: '+212%', duration: '6m01s', engine: 'Spark', note: '命中突增告警(规则 v3 首次全量), 人工确认口径无误' },
    { id: 23, time: '2026-09-05 01:30:00', trigger: '发布', ver: 2, status: 'SUCCESS', hits: '41,203', delta: '—', duration: '4m52s', engine: 'Spark', note: 'v2 → v3 切换, 双版本窗口并行' },
    { id: 22, time: '2026-09-01 02:00:20', trigger: '定时', ver: 2, status: 'FAILED', hits: '—', delta: '—', duration: '1m02s', engine: 'Spark', note: 'TiDB 连接超时, 自动重试 2 次后成功' }
  )
  tagDist.value.push(
    { tag: '客户分层/高价值', hits: 128540 }, { tag: '客户分层/中价值', hits: 82310 },
    { tag: '客户分层/一般', hits: 142110 }, { tag: '客户分层/沉睡', hits: 18903 }
  )
})
</script>

<style scoped>
.head { display: flex; justify-content: space-between; align-items: center; }
.title { font-size: 16px; font-weight: 600; margin: 0 10px; }
.stat { text-align: center; }
.stat-num { font-size: 22px; font-weight: 700; color: #409eff; }
.stat-num.bad { color: #f56c6c; }
.stat-label { font-size: 12px; color: #8f959e; margin-top: 4px; }
.sub-t { font-weight: 600; margin: 10px 0 8px; font-size: 13px; }
.chain { display: flex; align-items: center; gap: 8px; margin-bottom: 8px; }
.chain-no { width: 18px; height: 18px; border-radius: 50%; background: #409eff; color: #fff; font-size: 11px; display: inline-flex; align-items: center; justify-content: center; }
</style>
