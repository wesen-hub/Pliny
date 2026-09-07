<template>
  <div class="page-card">
    <el-card shadow="never">
      <template #header>血缘检索<el-link type="primary" style="float: right" @click="goDetail">资产详情 →</el-link></template>
      <div class="toolbar">
        <el-select v-model="keyword" filterable allow-create default-first-option placeholder="选择/输入逻辑表名, 查看字段级血缘" style="width: 420px">
          <el-option v-for="t in tables" :key="t" :label="t" :value="t" />
        </el-select>
        <el-button type="primary" :loading="loading" @click="search">查询</el-button>
      </div>
    </el-card>

    <el-card shadow="never" style="margin-top: 16px" v-loading="loading">
      <template #header>{{ keyword }} · 字段级血缘(由 CDC field_mapping 推导, {{ lineage.length }} 条)</template>
      <el-table :data="lineage" stripe>
        <el-table-column label="模型字段" width="160">
          <template #default="{ row }"><span class="fld">{{ row.modelField }}</span></template>
        </el-table-column>
        <el-table-column label="来源" min-width="260">
          <template #default="{ row }">
            <span class="src">{{ row.srcTable }}.{{ row.srcField }}</span>
            <span class="arr">→</span>
            <span class="fld">{{ row.modelField }}</span>
          </template>
        </el-table-column>
        <el-table-column prop="transform" label="转换" width="120" />
        <el-table-column label="敏感" width="90">
          <template #default="{ row }">
            <el-tag v-if="row.sensitive" size="small" type="warning">脱敏</el-tag>
            <span v-else>—</span>
          </template>
        </el-table-column>
      </el-table>
      <el-empty v-if="!lineage.length" description="该资产无字段映射血缘(未配置 CDC 任务)" />
      <el-descriptions :column="2" border style="margin-top: 14px">
        <el-descriptions-item label="溯源分析(上游)">字段级映射 {{ lineage.length }} 条 · 源表 {{ srcTables.length }} 张</el-descriptions-item>
        <el-descriptions-item label="影响分析(下游)">目标分表 {{ detail.shardCount }} 片 · 订阅/报表模块见订阅中心</el-descriptions-item>
      </el-descriptions>
    </el-card>
  </div>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue'
import { useRouter } from 'vue-router'
import { listAssets, getAssetLineage, getAssetDetail } from '../../api'

const router = useRouter()
const keyword = ref('')
const tables = ref([])
const lineage = ref([])
const detail = ref({})
const loading = ref(false)

const srcTables = computed(() => [...new Set(lineage.value.map(e => e.srcTable))])

async function search() {
  if (!keyword.value) return
  loading.value = true
  try {
    const [l, d] = await Promise.all([
      getAssetLineage(keyword.value), getAssetDetail(keyword.value)
    ])
    lineage.value = l || []
    detail.value = d || {}
  } finally {
    loading.value = false
  }
}

function goDetail() {
  if (keyword.value) router.push(`/asset/${keyword.value}`)
}

onMounted(async () => {
  const res = await listAssets()
  tables.value = (res || []).map(a => a.table)
  if (tables.value.length) {
    keyword.value = tables.value[0]
    search()
  }
})
</script>

<style scoped>
.fld { font-weight: 600; color: #1677ff; }
.src { color: #d46b08; font-weight: 500; }
.arr { color: #bbb; margin: 0 6px; }
</style>
