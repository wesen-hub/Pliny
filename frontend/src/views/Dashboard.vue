<template>
  <div class="page-card">
    <el-row :gutter="16">
      <el-col :span="6" v-for="card in cards" :key="card.label">
        <el-card shadow="hover">
          <div class="stat-card">
            <el-icon :size="32" :color="card.color"><component :is="card.icon" /></el-icon>
            <div>
              <div class="stat-value">{{ card.value }}</div>
              <div class="stat-label">{{ card.label }}</div>
            </div>
          </div>
        </el-card>
      </el-col>
    </el-row>

    <el-card shadow="never" style="margin-top: 16px">
      <template #header>快速开始</template>
      <el-steps :active="3" align-center>
        <el-step title="注册数据源" description="数据源管理 → 新建 (MySQL/PostgreSQL/Oracle/SQLServer/MongoDB)" />
        <el-step title="创建同步任务" description="任务管理 → 新建任务, 4 步向导配置抽取/映射/调度" />
        <el-step title="执行与监控" description="立即执行, 在「执行历史」查看结果, 统计报表看趋势" />
      </el-steps>
    </el-card>

    <el-card shadow="never" style="margin-top: 16px">
      <template #header>平台能力</template>
      <el-descriptions :column="3" border>
        <el-descriptions-item label="同步模式">全量 / 增量(两段式位点) </el-descriptions-item>
        <el-descriptions-item label="转换链">直接映射 → 字典 → Aviator 表达式</el-descriptions-item>
        <el-descriptions-item label="幂等写入">REPLACE INTO 批量</el-descriptions-item>
        <el-descriptions-item label="调度">Cron 定时 + 手动触发</el-descriptions-item>
        <el-descriptions-item label="可靠性">失败重试(指数退避) + 超时控制</el-descriptions-item>
        <el-descriptions-item label="安全">JWT + RBAC + 密码 AES-256-GCM + 审计</el-descriptions-item>
      </el-descriptions>
    </el-card>
  </div>
</template>

<script setup>
import { onMounted, ref } from 'vue'
import { statsOverview } from '../api'

const cards = ref([
  { label: '今日执行次数', value: '-', icon: 'Timer', color: '#409EFF' },
  { label: '24h 成功率', value: '-', icon: 'CircleCheck', color: '#67C23A' },
  { label: '运行中任务', value: '-', icon: 'Loading', color: '#E6A23C' },
  { label: '任务总数', value: '-', icon: 'SetUp', color: '#909399' }
])

onMounted(async () => {
  try {
    const data = await statsOverview()
    cards.value[0].value = data.todayExecutions
    cards.value[1].value = (data.successRate24h ?? 0) + '%'
    cards.value[2].value = data.runningCount
    cards.value[3].value = data.jobCount
  } catch (e) { /* ignore */ }
})
</script>

<style scoped>
.stat-card { display: flex; align-items: center; gap: 14px; }
.stat-value { font-size: 26px; font-weight: 700; color: #303133; }
.stat-label { font-size: 13px; color: #909399; }
</style>
