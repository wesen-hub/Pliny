<template>
  <div class="page-card">
    <el-row :gutter="16">
      <el-col :span="12">
        <el-card shadow="never">
          <template #header>
            <div style="display: flex; justify-content: space-between; align-items: center">
              <span>执行趋势 ({{ days }} 天)</span>
              <el-radio-group v-model="days" size="small" @change="loadTrend">
                <el-radio-button :value="7">7天</el-radio-button>
                <el-radio-button :value="30">30天</el-radio-button>
              </el-radio-group>
            </div>
          </template>
          <div ref="trendEl" style="height: 320px"></div>
        </el-card>
      </el-col>
      <el-col :span="12">
        <el-card shadow="never">
          <template #header>任务排行 (Top {{ rankLimit }})</template>
          <div ref="rankEl" style="height: 320px"></div>
        </el-card>
      </el-col>
    </el-row>
    <el-card shadow="never" style="margin-top: 16px">
      <template #header>成功率</template>
      <div ref="rateEl" style="height: 280px"></div>
    </el-card>
  </div>
</template>

<script setup>
import { onMounted, onUnmounted, ref } from 'vue'
import * as echarts from 'echarts'
import { statsTrend, statsTaskRank } from '../../api'

const trendEl = ref(null)
const rankEl = ref(null)
const rateEl = ref(null)
const days = ref(7)
const rankLimit = ref(10)
let charts = []

async function loadTrend() {
  const data = await statsTrend(days.value)
  const chart = echarts.init(trendEl.value)
  chart.setOption({
    tooltip: { trigger: 'axis' },
    legend: { data: ['执行次数', '成功次数'] },
    grid: { left: 50, right: 20, top: 40, bottom: 30 },
    xAxis: { type: 'category', data: data.map(d => d.day) },
    yAxis: { type: 'value' },
    series: [
      { name: '执行次数', type: 'line', smooth: true, areaStyle: { opacity: 0.15 }, data: data.map(d => Number(d.total)) },
      { name: '成功次数', type: 'line', smooth: true, data: data.map(d => Number(d.success || 0)) }
    ]
  })
  charts.push(chart)
}

async function loadRank() {
  const data = await statsTaskRank(rankLimit.value)
  const chart = echarts.init(rankEl.value)
  chart.setOption({
    tooltip: { trigger: 'axis' },
    grid: { left: 120, right: 40, top: 20, bottom: 30 },
    xAxis: { type: 'value' },
    yAxis: { type: 'category', data: data.map(d => d.jobName).reverse() },
    series: [{
      name: '执行次数', type: 'bar', barMaxWidth: 22,
      data: data.map(d => Number(d.total)).reverse(),
      itemStyle: { color: (p) => ['#409EFF', '#67C23A', '#E6A23C', '#F56C6C'][p.dataIndex % 4] }
    }]
  })
  charts.push(chart)
}

async function loadRate() {
  const data = await statsTrend(days.value)
  const chart = echarts.init(rateEl.value)
  chart.setOption({
    tooltip: { trigger: 'axis', formatter: p => `${p[0].name}<br/>成功率: ${p[0].value}%` },
    grid: { left: 50, right: 20, top: 30, bottom: 30 },
    xAxis: { type: 'category', data: data.map(d => d.day) },
    yAxis: { type: 'value', max: 100, axisLabel: { formatter: '{value}%' } },
    series: [{
      name: '成功率', type: 'bar', barMaxWidth: 30,
      data: data.map(d => {
        const total = Number(d.total)
        return total === 0 ? 0 : Math.round(Number(d.success || 0) * 10000 / total) / 100
      }),
      itemStyle: { color: '#67C23A' }
    }]
  })
  charts.push(chart)
}

function resize() { charts.forEach(c => c.resize()) }

onMounted(async () => {
  await Promise.all([loadTrend(), loadRank(), loadRate()])
  window.addEventListener('resize', resize)
})
onUnmounted(() => {
  window.removeEventListener('resize', resize)
  charts.forEach(c => c.dispose())
})
</script>
