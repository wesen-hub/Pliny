<template>
  <div class="page-card">
    <el-card shadow="never" v-loading="loading">
      <div class="flex-between">
        <div>
          <div class="title">{{ detail.name || '…' }}
            <el-tag size="small" :type="detail.status === '运行中' ? 'success' : 'warning'" effect="plain">{{ detail.status }}</el-tag>
            <el-tag v-if="detail.mode === 'push'" size="small" type="primary" effect="plain">Push</el-tag>
          </div>
          <div class="meta">订阅 #{{ detail.id }} · {{ detail.target || '—' }} · {{ detail.create_time }}</div>
          <div class="tags">
            <el-tag v-for="t in detail.expr?.tags || []" :key="t" size="small" type="primary">{{ t }}</el-tag>
            <el-tag v-if="(detail.expr?.tags || []).length > 1" size="small" type="warning" effect="plain">{{ detail.expr?.op }}</el-tag>
            <el-tag v-if="detail.expr?.maxSecret" size="small">密级≤{{ secretName(detail.expr.maxSecret) }}</el-tag>
          </div>
        </div>
        <div>
          <el-button @click="$router.push('/subscribe')">返回列表</el-button>
          <el-button v-if="detail.status === '运行中'" type="warning" @click="toggle('暂停')">暂停</el-button>
          <el-button v-else type="success" @click="toggle('运行中')">恢复</el-button>
        </div>
      </div>
    </el-card>

    <el-card shadow="never" style="margin-top: 16px">
      <template #header>命中资产(标签动态子集 · {{ (detail.assets || []).length }} 个)</template>
      <el-table :data="detail.assets || []" stripe size="small" v-loading="loading">
        <el-table-column label="资产" min-width="160">
          <template #default="{ row }">
            <el-link type="primary" @click="$router.push(`/asset/${row.table}`)">{{ row.displayName || row.name }}</el-link>
          </template>
        </el-table-column>
        <el-table-column label="来源" min-width="140">
          <template #default="{ row }">
            <el-tag v-for="s in row.sources" :key="s.table" size="small">{{ s.dsName }}</el-tag>
          </template>
        </el-table-column>
        <el-table-column label="密级" width="70">
          <template #default="{ row }"><span :class="`secret sec-${row.secretLevel}`">{{ secretName(row.secretLevel) }}</span></template>
        </el-table-column>
        <el-table-column label="表数" width="90">
          <template #default="{ row }"><span>{{ row.shardCount ?? '—' }}</span></template>
        </el-table-column>
        <el-table-column label="状态" width="80">
          <template #default="{ row }">
            <el-tag size="small" :type="row.status==='就绪'?'success':'info'" effect="plain">{{ row.status }}</el-tag>
          </template>
        </el-table-column>
      </el-table>
      <div class="tip">命中规则 = 标签条件实时求值(与资产目录子集同源); 资产打标变化自动影响命中集合</div>
    </el-card>
  </div>
</template>

<script setup>
import { ref, onMounted } from 'vue'
import { useRoute } from 'vue-router'
import { ElMessage } from 'element-plus'
import { getSubscription, setSubscriptionStatus } from '../../api'

const route = useRoute()
const detail = ref({})
const loading = ref(false)

function secretName(level) { return ['', '公开', '内部', '机密', '绝密'][level] }

async function load() {
  loading.value = true
  try {
    detail.value = (await getSubscription(route.params.id)) || {}
  } finally {
    loading.value = false
  }
}
async function toggle(status) {
  await setSubscriptionStatus(route.params.id, status)
  ElMessage.success(`已${status}`)
  load()
}

onMounted(load)
</script>

<style scoped>
.flex-between { display: flex; justify-content: space-between; align-items: flex-start; }
.title { font-size: 18px; font-weight: 700; display: flex; align-items: center; gap: 8px; }
.meta { color: #8f959e; font-size: 12px; margin-top: 4px; }
.tags { margin-top: 10px; display: flex; gap: 6px; flex-wrap: wrap; }
.tip { margin-top: 10px; font-size: 12px; color: #8f959e; }
.secret { display: inline-flex; align-items: center; padding: 1px 8px; border-radius: 4px; font-size: 12px; line-height: 18px; font-weight: 600; }
.sec-1 { background: #f6ffed; color: #389e0d; } .sec-2 { background: #e6f4ff; color: #0958d9; }
.sec-3 { background: #fff7e6; color: #d46b08; } .sec-4 { background: #fff1f0; color: #cf1322; }
</style>
