<template>
  <div class="page-card">
    <el-row :gutter="16">
      <el-col :span="6" v-for="c in cards" :key="c.label">
        <el-card shadow="hover">
          <div class="stat-card">
            <div>
              <div class="stat-value" :style="{ color: c.color }">{{ c.value }}<small v-if="c.unit">{{ c.unit }}</small></div>
              <div class="stat-label">{{ c.label }}</div>
            </div>
          </div>
        </el-card>
      </el-col>
    </el-row>

    <!-- 链式缓冲链路 -->
    <el-card shadow="never" style="margin-top: 16px">
      <template #header>端到端链路(链式缓冲: 源 → MySQL 缓冲 → Iceberg 湖仓)<el-text type="info" style="float: right; font-weight: 400; font-size: 12px">中间层不查询,只做传输/削峰/解耦</el-text></template>
      <div class="pipeline">
        <div class="pl-node">
          <div class="n">源系统</div>
          <div class="s">ERP · CRM · SCM · HRM</div>
          <div class="d">异构库表</div>
        </div>
        <div class="arr">→</div>
        <div class="pl-node run">
          <div class="n">SeaTunnel CDC</div>
          <div class="s">全量快照 + 增量 binlog</div>
          <div class="d delay"><span class="dot ok"></span>秒级 · 1.8s</div>
        </div>
        <div class="arr">→</div>
        <div class="pl-node run">
          <div class="n">MySQL 缓冲</div>
          <div class="s">Kafka 分区按桶路由 · 写 TiDB 单表</div>
          <div class="d">系统内递增序号 · Binlog 位点溯源</div>
        </div>
        <div class="arr">→</div>
        <div class="pl-node run">
          <div class="n">批量同步</div>
          <div class="s">Spark/Flink Batch · MERGE INTO</div>
          <div class="d delay"><span class="dot ok"></span>5–15 min</div>
        </div>
        <div class="arr">→</div>
        <div class="pl-node">
          <div class="n">Iceberg 湖仓</div>
          <div class="s">MinIO · ACID · 时间旅行</div>
          <div class="d">ODS / DWD / DWS / ADS</div>
        </div>
        <div class="arr">→</div>
        <div class="pl-node">
          <div class="n">资产与订阅</div>
          <div class="s">Trino 查询 · 标签打标 · 订阅分发</div>
          <div class="d">资产目录占位 → 就绪</div>
        </div>
      </div>
    </el-card>

    <el-row :gutter="16" style="margin-top: 16px">
      <el-col :span="16">
        <el-card shadow="never">
          <template #header>同步任务状态<el-button link type="primary" style="float: right" @click="$router.push('/sync/task')">全部任务 →</el-button></template>
          <el-table :data="tasks" stripe size="small">
            <el-table-column label="任务" min-width="150">
              <template #default="{ row }"><el-link type="primary" @click="$router.push(`/sync/task/${row.id}`)">{{ row.name }}</el-link></template>
            </el-table-column>
            <el-table-column prop="model" label="模型" width="140" />
            <el-table-column prop="source" label="源表" width="120" />
            <el-table-column label="采集延迟" width="100">
              <template #default="{ row }"><el-tag size="small" type="success">{{ row.cdcLag }}</el-tag></template>
            </el-table-column>
            <el-table-column label="湖可见延迟" width="110">
              <template #default="{ row }"><el-tag size="small" type="primary">{{ row.lakeLag }}</el-tag></template>
            </el-table-column>
            <el-table-column label="状态" width="90">
              <template #default="{ row }"><el-tag size="small" :type="row.status === '运行中' ? 'success' : 'warning'" effect="plain">{{ row.status }}</el-tag></template>
            </el-table-column>
          </el-table>
        </el-card>
      </el-col>
      <el-col :span="8">
        <el-card shadow="never">
          <template #header>待办与告警</template>
          <div class="notice"><el-tag size="small" type="primary">审批</el-tag> 映射配置待审批: SCM.t_material → 物料主数据(2 字段)</div>
          <div class="notice"><el-tag size="small" type="warning">待办</el-tag> 模型「物料主数据」已发布,等待源表映射</div>
          <div class="notice"><el-tag size="small" type="success">完成</el-tag> 模型「订单明细」发布 → 资产目录占位已创建</div>
          <div class="notice"><el-tag size="small" type="danger">告警</el-tag> 攒批写链路延迟 2.4s(阈值 5s)</div>
        </el-card>
      </el-col>
    </el-row>
  </div>
</template>

<script setup>
import { ref } from 'vue'
// TODO: 接入后端 API(/sync/overview,链路两段延迟 /stats)
const cards = ref([
  { label: '采集延迟(源→缓冲)', value: '1.8', unit: 's', color: '#52c41a' },
  { label: '湖仓可见延迟', value: '7.2', unit: 'min', color: '#1677ff' },
  { label: '运行中同步任务', value: '14', unit: '', color: '#303133' },
  { label: '今日同步数据量', value: '186', unit: 'GB', color: '#722ed1' }
])
const tasks = ref([
  { id: 't-od', name: '订单明细-同步', model: '订单明细 DWD', source: 'ERP.t_order', cdcLag: '1.8s', lakeLag: '7.2min', status: '运行中' },
  { id: 't-cm', name: '客户主数据-同步', model: '客户主数据 DWD', source: 'CRM.t_customer', cdcLag: '2.2s', lakeLag: '6.8min', status: '运行中' },
  { id: 't-os', name: '订单汇总-同步', model: '订单汇总 DWS', source: 'ERP+CRM 合并', cdcLag: '2.0s', lakeLag: '8.1min', status: '运行中' },
  { id: 't-mm', name: '物料主数据-同步', model: '物料主数据 DWD', source: 'SCM.t_material', cdcLag: '—', lakeLag: '—', status: '待映射' }
])
</script>

<style scoped>
.stat-value { font-size: 26px; font-weight: 700; }
.stat-value small { font-size: 13px; font-weight: 400; color: #8f959e; }
.stat-label { font-size: 12px; color: #646a73; margin-top: 4px; }
.pipeline { display: flex; align-items: stretch; gap: 0; overflow-x: auto; padding: 8px 0; }
.pl-node { flex-shrink: 0; min-width: 150px; background: #fff; border: 1px solid #d9d9d9; border-radius: 10px; padding: 12px 14px; box-shadow: 0 1px 4px rgba(0,0,0,.04); }
.pl-node.run { border-color: #1677ff; }
.pl-node .n { font-weight: 600; font-size: 13px; }
.pl-node .s { font-size: 11px; color: #646a73; margin-top: 4px; line-height: 1.5; }
.pl-node .d { font-size: 11px; color: #8f959e; margin-top: 6px; }
.delay { display: flex; align-items: center; gap: 4px; }
.dot { width: 8px; height: 8px; border-radius: 50%; display: inline-block; }
.dot.ok { background: #52c41a; }
.arr { display: flex; align-items: center; color: #bbb; font-size: 18px; padding: 0 6px; flex-shrink: 0; }
.notice { display: flex; gap: 8px; font-size: 12.5px; padding: 8px 0; border-bottom: 1px solid #f5f5f5; align-items: center; line-height: 1.6; }
.notice:last-child { border-bottom: none; }
</style>
