<template>
  <div class="page-card">
    <el-card shadow="never">
      <div class="toolbar">
        <div style="font-weight: 600">订阅中心<el-text type="info" style="margin-left: 8px; font-weight: 400">订阅 = 标签条件 · 命中资产 = 标签动态子集(与资产目录同源)</el-text></div>
        <el-button type="primary" @click="$router.push('/subscribe/create')">＋ 新建订阅</el-button>
      </div>
      <div class="toolbar">
        <el-input v-model="keyword" placeholder="搜索订阅名称…" clearable style="width: 300px">
          <template #prefix><el-icon><Search /></el-icon></template>
        </el-input>
        <el-button :loading="loading" @click="load">刷新</el-button>
      </div>
      <el-table :data="filtered" stripe v-loading="loading">
        <el-table-column label="订阅名称" min-width="150">
          <template #default="{ row }"><el-link type="primary" @click="$router.push(`/subscribe/${row.id}`)">{{ row.name }}</el-link></template>
        </el-table-column>
        <el-table-column label="标签条件" min-width="300">
          <template #default="{ row }">
            <el-tag v-for="t in row.expr.tags || []" :key="t" size="small" type="primary" style="margin-right:4px">{{ t }}</el-tag>
            <el-tag v-if="(row.expr.tags||[]).length > 1" size="small" type="warning" effect="plain" style="margin-right:4px">{{ row.expr.op }}</el-tag>
            <span v-if="row.expr.maxSecret" class="sub">密级≤{{ secretName(row.expr.maxSecret) }}</span>
          </template>
        </el-table-column>
        <el-table-column prop="mode" label="模式" width="80">
          <template #default="{ row }">
            <el-tag size="small" :type="row.mode === 'push' ? 'primary' : 'info'" effect="plain">{{ row.mode }}</el-tag>
          </template>
        </el-table-column>
        <el-table-column prop="target" label="接收方" width="130" show-overflow-tooltip />
        <el-table-column label="状态" width="90">
          <template #default="{ row }">
            <el-tag size="small" :type="row.status === '运行中' ? 'success' : 'warning'" effect="plain">{{ row.status }}</el-tag>
          </template>
        </el-table-column>
        <el-table-column prop="create_time" label="创建时间" width="170" />
        <el-table-column label="操作" width="180">
          <template #default="{ row }">
            <el-link type="primary" underline="never" @click="$router.push(`/subscribe/${row.id}`)">详情</el-link>
            <el-link v-if="row.status === '运行中'" type="warning" underline="never" style="margin-left:8px" @click="toggle(row, '暂停')">暂停</el-link>
            <el-link v-else type="success" underline="never" style="margin-left:8px" @click="toggle(row, '运行中')">恢复</el-link>
            <el-link type="danger" underline="never" style="margin-left:8px" @click="remove(row)">删除</el-link>
          </template>
        </el-table-column>
      </el-table>
    </el-card>
  </div>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import { listSubscriptions, setSubscriptionStatus, deleteSubscription } from '../../api'

const subscriptions = ref([])
const keyword = ref('')
const loading = ref(false)

const filtered = computed(() => subscriptions.value.filter(s =>
  !keyword.value || s.name.includes(keyword.value)))

function secretName(level) { return ['', '公开', '内部', '机密', '绝密'][level] }

async function load() {
  loading.value = true
  try {
    subscriptions.value = await listSubscriptions() || []
  } finally {
    loading.value = false
  }
}
async function toggle(row, status) {
  await setSubscriptionStatus(row.id, status)
  ElMessage.success(`已${status}`)
  load()
}
async function remove(row) {
  await ElMessageBox.confirm(`删除订阅「${row.name}」?`, '确认', { type: 'warning' })
  await deleteSubscription(row.id)
  ElMessage.success('已删除')
  load()
}

onMounted(load)
</script>

<style scoped>
.sub { color: #8f959e; font-size: 11px; }
</style>
