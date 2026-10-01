<script setup>
import { ref, computed } from 'vue'
import { store, nsColor } from '../store'

const nsFilter = ref('sync')

const series = {
  sync: [42, 45, 44, 47, 46, 48, 47, 49, 48, 50, 49, 46],
  indexer: [88, 92, 95, 94, 97, 99, 96, 98, 97, 96, 94, 96],
  outlet: [18, 20, 19, 22, 21, 23, 22, 21, 22, 21, 20, 21],
}
const cur = computed(() => series[nsFilter.value] || series.sync)

// 折线 SVG 坐标
const W = 560, H = 160, PAD = 24
const pts = computed(() => {
  const data = cur.value
  const max = Math.max(...data) * 1.15
  return data.map((v, i) => {
    const x = PAD + (i * (W - PAD * 2)) / (data.length - 1)
    const y = H - PAD - (v / max) * (H - PAD * 2)
    return [x, y]
  })
})
const path = computed(() => pts.value.map(([x, y], i) => `${i === 0 ? 'M' : 'L'}${x},${y}`).join(' '))
const area = computed(() => `${path.value} L${pts.value.at(-1)[0]},${H - PAD} L${PAD},${H - PAD} Z`)

const kpi = computed(() => {
  const ns = store.namespaces.find(n => n.name === nsFilter.value)
  return { rps: ns.rps, lag: ns.lag, latency: ns.name === 'indexer' ? 3.2 : ns.name === 'sync' ? 18.5 : 42.0, error: ns.name === 'indexer' ? 0.01 : 0.08 }
})

const tableRanks = computed(() => {
  const tasks = store.tasks.filter(t => t.ns === nsFilter.value)
  return [...tasks].sort((a, b) => b.currentRps - a.currentRps).map(t => ({
    topic: t.topic + (t.unit ? ` · ${t.unit}` : ''),
    rps: t.currentRps,
    parts: t.partitions.length,
    cap: t.partitions.length * t.targetRps,
    ratio: Math.round((t.currentRps / (t.partitions.length * t.targetRps)) * 100),
  }))
})
</script>

<template>
  <div>
    <div class="head">
      <div>
        <h1 class="page-title">监控</h1>
        <p class="page-desc">业务只报 4 个原始计数器(处理条数/提交位点/错误数/累计耗时);框架查 Kafka 水位做差值计算,派生 rps · lag · latency · errorRate。快照上报行为本身就是活性心跳。</p>
      </div>
      <el-select v-model="nsFilter" style="width:180px">
        <el-option v-for="ns in store.namespaces" :key="ns.name" :label="`${ns.label} (${ns.name})`" :value="ns.name" />
      </el-select>
    </div>

    <div class="kpi-grid">
      <div class="kpi-card"><div class="kpi-label">总流量 rps</div><div class="kpi-val">{{ (kpi.rps / 1000).toFixed(1) }}k</div><div class="kpi-sub">Σ 分区级 rps(框架聚合)</div></div>
      <div class="kpi-card"><div class="kpi-label">总积压 lag</div><div class="kpi-val" :style="{ color: kpi.lag > 3000 ? '#ef4444' : '#0f172a' }">{{ kpi.lag.toLocaleString() }}</div><div class="kpi-sub">endOffset − 已提交位点(框架查 Kafka)</div></div>
      <div class="kpi-card"><div class="kpi-label">处理延迟 avg</div><div class="kpi-val">{{ kpi.latency }} ms</div><div class="kpi-sub">Δ累计耗时 / Δ处理条数</div></div>
      <div class="kpi-card"><div class="kpi-label">错误率</div><div class="kpi-val" :style="{ color: kpi.error > 0.05 ? '#ef4444' : '#0f172a' }">{{ kpi.error }}%</div><div class="kpi-sub">Δ错误数 / Δ处理条数</div></div>
    </div>

    <div class="two-col">
      <div class="card">
        <h3 class="card-title">rps 趋势(每周期 10s)</h3>
        <svg :viewBox="`0 0 ${W} ${H}`" width="100%">
          <line v-for="i in 4" :key="i" :x1="PAD" :x2="W - PAD" :y1="H - PAD - (i * (H - PAD * 2)) / 4" :y2="H - PAD - (i * (H - PAD * 2)) / 4" stroke="#e2e8f0" stroke-width="1" />
          <path :d="area" :fill="nsColor[nsFilter] + '18'" />
          <path :d="path" fill="none" :stroke="nsColor[nsFilter]" stroke-width="2.5" />
          <circle v-for="(p, i) in pts" :key="i" :cx="p[0]" :cy="p[1]" r="3" :fill="nsColor[nsFilter]" />
        </svg>
        <div class="chart-x">-120s ← → 当前</div>
      </div>

      <div class="card">
        <h3 class="card-title">表级流量与容量水位</h3>
        <div v-for="r in tableRanks" :key="r.topic" class="rank-row">
          <div class="rank-head">
            <span class="rank-topic">{{ r.topic }}</span>
            <span class="rank-rps">{{ (r.rps / 1000).toFixed(1) }}k rps</span>
          </div>
          <el-progress
            :percentage="Math.min(r.ratio, 100)"
            :color="r.ratio > 80 ? '#ef4444' : r.ratio > 50 ? '#f59e0b' : '#10b981'"
            :format="() => `水位 ${r.ratio}% · ${r.parts} 分区`"
          />
        </div>
        <div class="watermark-tip">水位 = 表流量 / (分区数 × targetRps):&gt;80% 连续 3 周期触发扩容,40% 连续 6 周期触发缩容。</div>
      </div>
    </div>

    <div class="tip">
      <b>指标来源分工</b>:业务侧只"顺手记"——处理条数 +1、提交位点赋值、错误 +1、耗时累加,连指标概念都不需要知道;<b>框架侧</b>查 Kafka Admin API 拿 endOffset 算 lag、做快照差值算 rps/latency/errorRate、按分区→表→实例三级聚合;<b>活性检测</b>靠快照上报行为(收不到 = 执行器卡死)与 etcd lease(实例离线)。
    </div>
  </div>
</template>

<style scoped>
.page-title { font-size: 22px; font-weight: 700; color: #0f172a; }
.page-desc { color: #64748b; font-size: 13px; margin: 8px 0 18px; max-width: 860px; }
.head { display: flex; justify-content: space-between; align-items: flex-start; }
.kpi-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 16px; margin-bottom: 16px; }
.kpi-card { background: #fff; border-radius: 12px; padding: 16px 18px; box-shadow: 0 1px 3px rgba(15,23,42,.06); }
.kpi-label { font-size: 12px; color: #64748b; }
.kpi-val { font-size: 26px; font-weight: 700; color: #0f172a; margin: 6px 0 4px; }
.kpi-sub { font-size: 11px; color: #94a3b8; }
.two-col { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }
.card { background: #fff; border-radius: 12px; padding: 18px 20px; box-shadow: 0 1px 3px rgba(15,23,42,.06); }
.card-title { font-size: 15px; color: #0f172a; margin-bottom: 12px; }
.chart-x { text-align: center; font-size: 11px; color: #94a3b8; margin-top: 6px; }
.rank-row { margin-bottom: 14px; }
.rank-head { display: flex; justify-content: space-between; margin-bottom: 4px; }
.rank-topic { font-size: 12.5px; font-weight: 600; color: #334155; }
.rank-rps { font-size: 12.5px; color: #64748b; }
.watermark-tip { font-size: 11.5px; color: #94a3b8; margin-top: 10px; line-height: 1.7; }
.tip { margin-top: 16px; background: #eff6ff; border: 1px solid #bfdbfe; border-radius: 10px; padding: 12px 16px; font-size: 12.5px; color: #1e3a8a; line-height: 1.8; }
</style>
