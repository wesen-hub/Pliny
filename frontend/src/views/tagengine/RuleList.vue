<template>
  <div class="page-card">
    <el-alert type="info" :closable="false" show-icon style="margin-bottom:14px"
      title="规则打标(快捷型): 条件/聚合规则 → 圈选实体打标; 规则内部编译为一次作业, 与「打标引擎(作业型)」共用执行/存储/治理底座 —— 复杂计算请用作业型" />
    <el-card shadow="never">
      <div class="toolbar">
        <el-radio-group v-model="q.status" size="small">
          <el-radio-button value="">全部</el-radio-button>
          <el-radio-button value="ONLINE">已上线</el-radio-button>
          <el-radio-button value="REVIEW">待审批</el-radio-button>
        </el-radio-group>
        <div>
          <el-button type="primary" plain @click="go('/tagengine/task')">去打标引擎(作业型)</el-button>
          <el-button type="warning" @click="go('/tagengine/rule/create')">＋ 新建规则打标</el-button>
        </div>
      </div>
      <el-table :data="tasks" stripe>
        <el-table-column label="规则 / 类型" min-width="200">
          <template #default="{ row }">
            <el-link type="primary" @click="go(`/tagengine/task/${row.id}`)">{{ row.name }}</el-link>
            <el-tag size="small" effect="plain" style="margin-left:6px">{{ row.kind === 'visual' ? '可视化' : 'SQL' }}</el-tag>
            <el-text size="small" type="info" style="display:block;font-size:11px">v{{ row.version }} · {{ row.owner }}</el-text>
          </template>
        </el-table-column>
        <el-table-column label="状态" width="110">
          <template #default="{ row }"><el-tag size="small" :type="row.status === 'ONLINE' ? 'success' : row.status === 'REVIEW' ? 'warning' : 'info'" effect="light">{{ row.status === 'ONLINE' ? '已上线' : row.status === 'REVIEW' ? '待审批' : '草稿' }}</el-tag></template>
        </el-table-column>
        <el-table-column label="数据集" min-width="120">
          <template #default="{ row }"><el-tag size="small" type="info" effect="plain">{{ row.dataset }}</el-tag></template>
        </el-table-column>
        <el-table-column label="条件摘要" min-width="240">
          <template #default="{ row }"><el-text size="small">{{ row.cond }}</el-text></template>
        </el-table-column>
        <el-table-column label="产出标签" min-width="150">
          <template #default="{ row }"><el-tag v-for="t in row.tags" :key="t" size="small" effect="plain" style="margin-right:4px">{{ t }}</el-tag></template>
        </el-table-column>
        <el-table-column label="最近命中" width="130">
          <template #default="{ row }">{{ row.lastHits || '—' }}</template>
        </el-table-column>
        <el-table-column label="操作" width="130">
          <template #default="{ row }">
            <el-link type="primary" underline="never" @click="go(`/tagengine/task/${row.id}`)">详情</el-link>
            <el-link v-if="row.status !== 'ONLINE'" type="success" underline="never" style="margin-left:8px" @click="toast('试跑(mock)')">试跑</el-link>
            <el-link v-else type="warning" underline="never" style="margin-left:8px" @click="toast('停用(mock)')">停用</el-link>
          </template>
        </el-table-column>
      </el-table>
    </el-card>
  </div>
</template>

<script setup>
import { ref, onMounted } from 'vue'
import { useRouter } from 'vue-router'
import { ElMessage } from 'element-plus'

const router = useRouter()
const go = (p) => router.push(p)
const toast = (m) => ElMessage.info(m)
const q = ref({ status: '' })
const tasks = ref([])
onMounted(() => {
  tasks.value = [
    { id: 2, name: '华东区域活跃客户圈选', kind: 'visual', version: 5, owner: '运营组·李然', status: 'ONLINE', dataset: 'customer_wide', cond: 'region = 华东 且 last_active_at 近 30 天', tags: ['区域/华东'], lastHits: '43,212' },
    { id: 10, name: '数据密级映射(枚举值型)', kind: 'sql', version: 1, owner: '管控组·周原', status: 'REVIEW', dataset: 'customer_wide', cond: 'CASE data_secret → 绝密/机密/内部/公开', tags: ['密级/数据密级'], lastHits: '—' },
    { id: 7, name: '新客首月行为评分', kind: 'visual', version: 1, owner: '数科组·刘洋', status: 'DRAFT', dataset: 'customer_wide', cond: '首月下单≥1 且 金额≥500', tags: ['行为/首月评分'], lastHits: '—' },
    { id: 6, name: '沉睡客户唤醒名单(180天)', kind: 'sql', version: 2, owner: '运营组·陈默', status: 'ONLINE', dataset: 'behavior_log_wide', cond: 'last_active_at < 180 天前 且 非黑名单', tags: ['生命周期/沉睡'], lastHits: '21,093' }
  ]
})
</script>

<style scoped>
.toolbar { display: flex; justify-content: space-between; margin-bottom: 14px; align-items: center; flex-wrap: wrap; gap: 8px; }
</style>
