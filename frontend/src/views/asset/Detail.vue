<template>
  <div class="page-card">
    <el-card shadow="never" v-loading="loading">
      <div class="flex-between">
        <div>
          <div class="title">{{ detail.table || '…' }}
            <el-tag type="success" effect="plain" style="vertical-align: middle">{{ detail.status }}</el-tag>
            <el-tag v-if="detail.sensitive" type="warning" style="vertical-align: middle">含敏感字段(脱敏)</el-tag>
          </div>
          <div class="meta">{{ detail.topic }} · 表 {{ detail.tableCount }}(TiDB 单表) · 桶 {{ detail.bucketCount }}</div>
          <div class="tags">
            <el-tag v-for="s in detail.sources || []" :key="s.table" size="small" type="info">{{ s.dsName }} · {{ s.db }}.{{ s.table }}</el-tag>
            <el-tag v-if="!detail.sources || !detail.sources.length" size="small" type="info">批任务(无 CDC 来源)</el-tag>
            <el-tag size="small" type="success">表数 {{ detail.tableCount ?? '—' }}</el-tag>
          </div>
        </div>
        <div>
          <el-button @click="$router.push('/asset/catalog')">返回目录</el-button>
          <el-button type="primary" @click="$router.push('/subscribe/create')">＋ 订阅此资产</el-button>
        </div>
      </div>
    </el-card>

    <el-card shadow="never" style="margin-top: 16px" v-loading="loading">
      <el-tabs v-model="tab">
        <el-tab-pane label="字段 Schema" name="schema">
          <el-table :data="detail.fields || []" stripe>
            <el-table-column prop="name" label="字段" width="150" />
            <el-table-column prop="type" label="类型" width="150" />
            <el-table-column prop="nullable" label="可空" width="70" />
            <el-table-column prop="source" label="来源字段(血缘)" min-width="220" />
            <el-table-column prop="transform" label="转换" width="110" />
            <el-table-column label="敏感" width="90">
              <template #default="{ row }">
                <el-tag v-if="row.sensitive" size="small" type="warning">脱敏</el-tag>
                <span v-else>—</span>
              </template>
            </el-table-column>
          </el-table>
          <div v-if="!(detail.fields || []).length" class="tip">该资产暂无字段映射配置(逻辑表已注册, 未配置 CDC 任务)</div>
        </el-tab-pane>
        <el-tab-pane label="数据预览" name="preview">
          <el-table :data="preview.rows || []" stripe size="small">
            <el-table-column v-for="c in preview.columns || []" :key="c" :prop="c" :label="c" min-width="110" />
          </el-table>
          <div class="tip">真实数据预览(目标分表 {{ preview.shard || '—' }});脱敏在采集链路已生效(phone 掩码)</div>
        </el-tab-pane>
        <el-tab-pane label="字段血缘" name="lineage">
          <el-table :data="lineage" stripe>
            <el-table-column prop="modelField" label="模型字段" width="150" />
            <el-table-column label="来源" min-width="200">
              <template #default="{ row }"><el-tag size="small" type="info">{{ row.srcTable }}</el-tag> <span style="margin:0 4px">←</span> {{ row.srcField }}</template>
            </el-table-column>
            <el-table-column prop="transform" label="转换" width="110" />
            <el-table-column label="敏感" width="90">
              <template #default="{ row }">
                <el-tag v-if="row.sensitive" size="small" type="warning">脱敏</el-tag>
                <span v-else>—</span>
              </template>
            </el-table-column>
          </el-table>
          <div class="tip">字段级血缘由 CDC 任务 field_mapping 推导(源表.字段 → 模型字段, 含转换步骤)</div>
        </el-tab-pane>
        <el-tab-pane label="订阅关系" name="subs">
          <el-empty description="暂无订阅(订阅中心为独立模块)" />
        </el-tab-pane>
      </el-tabs>
    </el-card>

    <el-row :gutter="16" style="margin-top: 16px">
      <el-col :span="12">
        <el-card shadow="never">
          <template #header>基本信息</template>
          <el-descriptions :column="1">
            <el-descriptions-item label="逻辑表">{{ detail.table }}</el-descriptions-item>
            <el-descriptions-item label="Kafka Topic">{{ detail.topic }}</el-descriptions-item>
            <el-descriptions-item label="物理表数">{{ detail.tableCount ?? '—' }}</el-descriptions-item>
            <el-descriptions-item label="桶数(路由)">{{ detail.bucketCount }}</el-descriptions-item>
            <el-descriptions-item label="来源数量">{{ (detail.sources || []).length }} {{ detail.multiSource ? '(多表合并)' : '' }}</el-descriptions-item>
          </el-descriptions>
        </el-card>
      </el-col>
      <el-col :span="12">
        <el-card shadow="never">
          <template #header>来源明细</template>
          <el-descriptions :column="1" v-for="s in detail.sources || []" :key="s.table" style="margin-bottom: 8px">
            <el-descriptions-item label="来源系统">{{ s.dsName }} ({{ s.dsType }})</el-descriptions-item>
            <el-descriptions-item label="源表">{{ s.db }}.{{ s.table }}</el-descriptions-item>
          </el-descriptions>
          <div v-if="!(detail.sources || []).length" class="tip">无 CDC 采集任务(逻辑表由批任务写入)</div>
        </el-card>
      </el-col>
    </el-row>
  </div>
</template>

<script setup>
import { ref, onMounted } from 'vue'
import { useRoute } from 'vue-router'
import { getAssetDetail, getAssetPreview, getAssetLineage } from '../../api'

const route = useRoute()
const tab = ref('schema')
const detail = ref({})
const preview = ref({})
const lineage = ref([])
const loading = ref(false)

async function load() {
  loading.value = true
  try {
    const table = route.params.id
    const [d, p, l] = await Promise.all([
      getAssetDetail(table), getAssetPreview(table, 10), getAssetLineage(table)
    ])
    detail.value = d || {}
    preview.value = p || {}
    lineage.value = l || []
  } finally {
    loading.value = false
  }
}

onMounted(load)
</script>

<style scoped>
.flex-between { display: flex; justify-content: space-between; align-items: flex-start; }
.title { font-size: 18px; font-weight: 700; display: flex; align-items: center; gap: 8px; }
.meta { color: #8f959e; font-size: 12px; margin-top: 4px; }
.tags { margin-top: 10px; display: flex; gap: 6px; flex-wrap: wrap; }
.tip { margin-top: 10px; font-size: 12px; color: #8f959e; }
</style>
