<template>
  <div class="page-card">
    <el-alert type="info" :closable="false" show-icon style="margin-bottom:14px"
      title="打标引擎 V1 · 设计稿(mock 数据): 真后端请用菜单「标签注册表 V2 / 规则库 V2 / 打标任务 V2」(已接 /api/tagengine/*); 本页仅供 V1 形态参考" />

    <el-card shadow="never">
      <div class="toolbar">
        <div style="display:flex;gap:8px;flex-wrap:wrap">
          <el-input v-model="q.keyword" placeholder="任务名/标签名" clearable style="width:200px" />
          <el-select v-model="q.status" placeholder="状态" clearable style="width:130px">
            <el-option v-for="(v,k) in statusMap" :key="k" :value="k" :label="v.label" />
          </el-select>
          <el-select v-model="q.engine" placeholder="执行引擎" clearable style="width:130px">
            <el-option value="spark" label="Spark" />
            <el-option value="sql" label="SQL-on-TiDB" />
          </el-select>
          <el-button type="primary" @click="toast('筛选(mock)')">查询</el-button>
        </div>
        <div>
          <el-button @click="toast('规则模板库(mock)')">规则模板</el-button>
          <el-button type="warning" @click="router.push('/tagengine2/task/create')">＋ 新建打标任务(去 V2)</el-button>
        </div>
      </div>

      <el-table :data="tasks" stripe v-loading="loading">
        <el-table-column label="任务 / 版本" min-width="200">
          <template #default="{ row }">
            <el-link type="primary" @click="router.push(`/tagengine/task/${row.id}`)">{{ row.name }}</el-link>
            <el-tag size="small" effect="plain" style="margin-left:6px">v{{ row.version }}</el-tag>
            <el-tag size="small" :type="modeMap[row.mode || 'visual'].type" effect="light" style="margin-left:4px">{{ modeMap[row.mode || 'visual'].label }}</el-tag>
            <el-text size="small" type="info" style="display:block;font-size:11px">{{ row.owner }}</el-text>
          </template>
        </el-table-column>
        <el-table-column label="状态" width="110">
          <template #default="{ row }">
            <el-tag size="small" :type="statusMap[row.status].type" effect="light">{{ statusMap[row.status].label }}</el-tag>
          </template>
        </el-table-column>
        <el-table-column label="数据集(实体)" min-width="170">
          <template #default="{ row }">
            <el-tag v-for="m in row.models" :key="m" size="small" type="info" effect="plain" style="margin-right:4px">{{ m }}</el-tag>
            <el-text size="small" type="info" style="display:block;font-size:11px">实体: {{ row.entityKey }}</el-text>
          </template>
        </el-table-column>
        <el-table-column label="产出标签" min-width="170">
          <template #default="{ row }">
            <el-tag v-for="t in row.tags" :key="t" size="small" effect="plain" style="margin-right:4px">{{ t }}</el-tag>
          </template>
        </el-table-column>
        <el-table-column label="调度" width="130">
          <template #default="{ row }">
            <div>{{ row.schedule.text }}</div>
            <el-text size="small" type="info" style="font-size:11px">引擎 {{ row.engine }}</el-text>
          </template>
        </el-table-column>
        <el-table-column label="最近一轮" min-width="150">
          <template #default="{ row }">
            <template v-if="row.lastRun">
              <div>{{ row.lastRun.time }}</div>
              <el-text size="small" type="success">命中 {{ row.lastRun.hits }}</el-text>
              <el-text v-if="row.lastRun.warn" size="small" type="danger" style="margin-left:6px">⚠ 突变告警</el-text>
            </template>
            <el-text v-else size="small" type="info">未运行</el-text>
          </template>
        </el-table-column>
        <el-table-column label="操作" width="250">
          <template #default="{ row }">
            <el-link type="primary" underline="never" @click="router.push(`/tagengine/task/${row.id}`)">详情</el-link>
            <el-link v-if="row.status === 'DRAFT'" type="warning" underline="never" style="margin-left:8px" @click="toast('提交审批(mock)')">提交审批</el-link>
            <el-link v-if="row.status === 'REVIEW'" type="warning" underline="never" style="margin-left:8px" @click="toast('审批通过(mock)')">审批</el-link>
            <el-link v-if="['APPROVED','OFFLINE'].includes(row.status)" type="success" underline="never" style="margin-left:8px" @click="toast('试跑预览(mock)')">试跑</el-link>
            <el-link v-if="row.status === 'APPROVED'" type="primary" underline="never" style="margin-left:8px" @click="toast('发布上线(mock)')">发布</el-link>
            <el-link v-if="['ONLINE','FAILED'].includes(row.status)" type="success" underline="never" style="margin-left:8px" @click="toast('重跑(mock)')">重跑</el-link>
            <el-link v-if="row.status === 'ONLINE'" type="danger" underline="never" style="margin-left:8px" @click="toast('停用(mock)')">停用</el-link>
          </template>
        </el-table-column>
      </el-table>
    </el-card>

    <el-card shadow="never" style="margin-top:12px">
      <template #header><b>规则模板库(示例)</b></template>
      <div class="tpl-grid">
        <div v-for="t in templates" :key="t.name" class="tpl" @click="toast(`使用模板: ${t.name}(mock)`)">
          <b>{{ t.name }}</b>
          <div class="tpl-desc">{{ t.desc }}</div>
          <div><el-tag size="small" effect="plain">{{ t.complexity }}</el-tag></div>
        </div>
      </div>
    </el-card>
  </div>
</template>

<script setup>
import { ref, onMounted } from 'vue'
import { useRouter } from 'vue-router'
import { ElMessage } from 'element-plus'

const router = useRouter()
const loading = ref(true)
const q = ref({ keyword: '', status: '', engine: '' })
const toast = (m) => ElMessage.info(m)

const statusMap = {
  DRAFT: { label: '草稿', type: 'info' },
  REVIEW: { label: '待审批', type: 'warning' },
  APPROVED: { label: '已审批', type: 'primary' },
  RUNNING: { label: '计算中', type: 'warning' },
  ONLINE: { label: '已上线', type: 'success' },
  FAILED: { label: '失败', type: 'danger' },
  OFFLINE: { label: '已停用', type: 'info' }
}

const tasks = ref([])
onMounted(() => {
  tasks.value = [
    { id: 1, name: 'RFM-高价值客户分层', version: 3, owner: '运营组·张琪', status: 'ONLINE', engine: 'spark',
      models: ['customer_wide', 'order_wide'], entityKey: 'customer_id',
      tags: ['客户分层/高价值', '客户分层/一般'], schedule: { text: '每日 02:00' },
      lastRun: { time: '2026-09-07 02:00:31', hits: 128_540 } },
    { id: 2, name: '华东区域活跃客户圈选', version: 5, owner: '运营组·李然', status: 'ONLINE', engine: 'spark',
      models: ['customer_wide'], entityKey: 'customer_id',
      tags: ['区域/华东'], schedule: { text: '每日 02:15' },
      lastRun: { time: '2026-09-07 02:15:12', hits: 43_212 } },
    { id: 3, name: '黑名单自动标记(风控)', version: 8, owner: '风控组·王硕', status: 'ONLINE', engine: 'spark',
      models: ['customer_wide', 'risk_event_wide'], entityKey: 'customer_id',
      tags: ['风控/黑名单'], schedule: { text: '每 30 分钟' },
      lastRun: { time: '2026-09-07 15:30:02', hits: 1_872, warn: true } },
    { id: 4, name: '近90天复购≥3次客户', version: 1, owner: '运营组·张琪', status: 'RUNNING', engine: 'sql',
      models: ['order_wide'], entityKey: 'customer_id',
      tags: ['行为/高频复购'], schedule: { text: '每日 03:00' },
      lastRun: { time: '2026-09-07 15:58:11', hits: 0 } },
    { id: 5, name: '沉睡客户唤醒名单(180天)', version: 2, owner: '运营组·陈默', status: 'APPROVED', engine: 'spark',
      models: ['customer_wide', 'behavior_log_wide'], entityKey: 'customer_id',
      tags: ['生命周期/沉睡'], schedule: { text: '每周一 03:30' },
      lastRun: { time: '2026-09-06 03:30:44', hits: 21_093 } },
    { id: 6, name: '高价值且非黑名单(组合)', version: 1, owner: '运营组·张琪', status: 'REVIEW', engine: 'spark',
      models: ['customer_wide'], entityKey: 'customer_id',
      tags: ['组合/高价值非黑'], schedule: { text: '随上游标签完成后触发' }, lastRun: null },
    { id: 7, name: '新客首月行为评分', version: 1, owner: '数科组·刘洋', status: 'DRAFT', engine: 'spark',
      models: ['customer_wide'], entityKey: 'customer_id',
      tags: ['行为/首月评分'], schedule: { text: '手动' }, lastRun: null },
    { id: 8, name: '旧版-区域标签(已废弃)', version: 12, owner: '运营组·李然', status: 'OFFLINE', engine: 'sql',
      models: ['customer_wide'], entityKey: 'customer_id',
      tags: ['区域/华东', '区域/华南'], schedule: { text: '—' },
      lastRun: { time: '2026-08-20 02:00:01', hits: 102_331 } },
    { id: 9, name: '流失概率打分(模型分→值型标签)', version: 2, owner: '数科组·刘洋', status: 'ONLINE', engine: 'spark', mode: 'script',
      models: ['customer_wide', 'churn_model_output'], entityKey: 'customer_id',
      tags: ['模型/流失概率'], schedule: { text: '每日 04:00' },
      lastRun: { time: '2026-09-07 04:00:22', hits: 382_101 } },
    { id: 10, name: '数据密级映射(枚举值型, 一个任务打完)', version: 1, owner: '管控组·周原', status: 'APPROVED', engine: 'sql', mode: 'sql',
      models: ['customer_wide'], entityKey: 'customer_id',
      tags: ['密级/数据密级(值: 公开|内部|机密|绝密)'], schedule: { text: '每日 05:00' }, lastRun: null }
  ]
  loading.value = false
})

const modeMap = {
  visual: { label: '模板作业', type: 'success' },
  sql: { label: 'SQL', type: 'warning' },
  pyspark: { label: 'PySpark', type: 'danger' },
  jar: { label: 'Spark-jar', type: 'danger' },
  script: { label: 'Spark 脚本', type: 'danger' }
}
const templates = [
  { name: 'RFM 客户分层', desc: '近 90 天消费额×频次×最近购买 3 维分箱 → 一个值型标签, 值=高/中/低(分支一次打完)', complexity: '聚合+分箱+分支' },
  { name: '密级/分层枚举映射', desc: '业务字段 CASE/码表 → 单个值型标签(如 数据密级=公开|内部|机密|绝密), 不拆 N 个任务', complexity: '映射/枚举值' },
  { name: '标签组合(与/或/排除)', desc: '对已有标签做布尔与集合运算(黑名单排除等), 产出组合标签', complexity: '集合运算+级联' },
  { name: '时间窗行为序列', desc: '近 N 天事件序列命中(领券后未下单) → 打标', complexity: '时间窗+序列' },
  { name: 'ML 打分(值型)', desc: '模型评分列原样落 value(可再叠分箱), 供阈值圈选/排序', complexity: '打分/连续值' },
  { name: '自定义 Spark 脚本(L4)', desc: 'PySpark/Scala jar 直提: 自研逻辑/复杂模型/图计算, 输出契约固定 (entity_key, value)', complexity: '自由代码+契约' }
]
</script>

<style scoped>
.toolbar { display: flex; justify-content: space-between; margin-bottom: 14px; align-items: center; flex-wrap: wrap; gap: 8px; }
.tpl-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(260px, 1fr)); gap: 12px; }
.tpl { border: 1px solid #e4e7ed; border-radius: 8px; padding: 12px 14px; cursor: pointer; }
.tpl:hover { border-color: #409eff; background: #f5f9ff; }
.tpl-desc { font-size: 12px; color: #6b7785; margin: 6px 0 8px; line-height: 1.6; }
</style>
