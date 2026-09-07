<template>
  <div class="page-card">
    <el-card shadow="never">
      <div class="flex-between">
        <div>
          <div class="title">订单明细 <el-tag type="primary" style="vertical-align: middle">DWD</el-tag>
            <el-tag type="success" effect="plain" style="vertical-align: middle">已发布</el-tag></div>
          <div class="meta">m-dwd-order · v3.2 · 业务域=交易 / 主题=订单 · Owner: 数据治理委员会-交易组</div>
        </div>
        <div>
          <el-button @click="$router.push('/model')">返回</el-button>
          <el-button type="primary" @click="$router.push('/sync/task/create')">＋ 新建同步任务</el-button>
        </div>
      </div>
    </el-card>

    <el-card shadow="never" style="margin-top: 16px">
      <el-tabs v-model="tab">
        <!-- Schema -->
        <el-tab-pane label="模型 Schema" name="schema">
          <el-table :data="schema" stripe>
            <el-table-column prop="name" label="字段" width="160" />
            <el-table-column prop="type" label="类型" width="140" />
            <el-table-column prop="nullable" label="可空" width="70" />
            <el-table-column prop="defaultVal" label="默认值" width="110" />
            <el-table-column prop="desc" label="字段描述" min-width="200" />
          </el-table>
          <div class="tip">模型字段由数据治理委员会定义;源表选择、字段映射、转换规则均在「同步任务」中配置</div>
        </el-tab-pane>

        <!-- 映射概览(只读,配置入口在同步任务) -->
        <el-tab-pane label="映射概览" name="overview">
          <el-alert type="info" :closable="false" show-icon style="margin-bottom: 14px"
            title="字段映射与转换规则在「同步任务」中配置(新建/编辑任务 → 字段映射、转换规则步骤);此处汇总各源表配置状态" />
          <el-table :data="mappings" stripe>
            <el-table-column label="源表" width="150">
              <template #default="{ row }"><span class="mono">{{ row.table }}</span></template>
            </el-table-column>
            <el-table-column label="数据源" width="150">
              <template #default="{ row }">{{ row.datasource }}<div class="sub">{{ row.dbType }}</div></template>
            </el-table-column>
            <el-table-column prop="mode" label="采集方式" width="110" />
            <el-table-column prop="mapCount" label="字段映射" width="90" />
            <el-table-column prop="ruleCount" label="表级规则" width="90" />
            <el-table-column label="状态" width="100">
              <template #default="{ row }">
                <el-tag size="small" :type="row.status === '已生效' ? 'success' : row.status === '待审批' ? 'warning' : 'info'" effect="plain">{{ row.status }}</el-tag>
              </template>
            </el-table-column>
            <el-table-column label="配置入口" width="130">
              <template #default="{ row }">
                <el-link type="primary" underline="never" @click="$router.push('/sync/task/create')">同步任务配置</el-link>
              </template>
            </el-table-column>
          </el-table>
        </el-tab-pane>

        <!-- 关联的同步任务 -->
        <el-tab-pane label="同步任务" name="tasks">
          <el-table :data="tasks" stripe>
            <el-table-column label="任务" min-width="160">
              <template #default="{ row }"><el-link type="primary" @click="$router.push(`/sync/task/${row.id}`)">{{ row.name }}</el-link></template>
            </el-table-column>
            <el-table-column prop="source" label="源表" width="140" />
            <el-table-column prop="segment" label="链路段" width="160" />
            <el-table-column label="延迟" width="100">
              <template #default="{ row }"><el-tag size="small" :type="row.lagType">{{ row.lag }}</el-tag></template>
            </el-table-column>
            <el-table-column label="状态" width="90">
              <template #default="{ row }"><el-tag size="small" type="success" effect="plain">{{ row.status }}</el-tag></template>
            </el-table-column>
            <el-table-column label="说明" min-width="200">
              <template #default="{ row }"><span class="sub">{{ row.note }}</span></template>
            </el-table-column>
          </el-table>
        </el-tab-pane>
      </el-tabs>
    </el-card>
  </div>
</template>

<script setup>
import { ref } from 'vue'
// TODO: 接入后端 API(/models/:id,映射概览 /models/:id/mappings)
const tab = ref('overview')
const schema = ref([
  { name: 'order_id', type: 'BIGINT', nullable: '否', defaultVal: '—', desc: '订单ID(主键)' },
  { name: 'order_no', type: 'VARCHAR(32)', nullable: '否', defaultVal: '—', desc: '订单编号' },
  { name: 'customer_id', type: 'BIGINT', nullable: '否', defaultVal: '—', desc: '客户ID' },
  { name: 'customer_mobile', type: 'VARCHAR(16)', nullable: '是', defaultVal: '—', desc: '客户手机号(敏感字段)' },
  { name: 'amount', type: 'DECIMAL(18,2)', nullable: '否', defaultVal: '0', desc: '订单金额' },
  { name: 'status', type: 'INT', nullable: '否', defaultVal: '1', desc: '订单状态' },
  { name: 'order_ts', type: 'TIMESTAMP', nullable: '否', defaultVal: '—', desc: '下单时间' },
  { name: 'seq', type: 'BIGINT', nullable: '否', defaultVal: '—', desc: '全局序号(幂等/水位)' }
])
const mappings = ref([
  { table: 't_order', datasource: 'ERP 生产库', dbType: 'MySQL', mode: '全量+CDC', mapCount: 7, ruleCount: 3, status: '已生效' },
  { table: 't_payment', datasource: 'ERP 生产库', dbType: 'MySQL', mode: '全量+CDC', mapCount: 5, ruleCount: 1, status: '待审批' }
])
const tasks = ref([
  { id: 't-od', name: '订单明细-同步', source: 'ERP.t_order', segment: '采集(CDC) + 批量转换', lag: '1.8s / 7.2min', lagType: 'success', status: '运行中', note: '由同步任务向导配置生成' },
  { id: 't-od-pay', name: '订单明细-同步(支付)', source: 'ERP.t_payment', segment: '采集(CDC) + 批量转换', lag: '—', lagType: 'info', status: '待审批', note: '配置提交审批中' }
])
</script>

<style scoped>
.flex-between { display: flex; justify-content: space-between; align-items: flex-start; }
.title { font-size: 18px; font-weight: 700; display: flex; align-items: center; gap: 8px; }
.meta { color: #8f959e; font-size: 12px; margin-top: 4px; }
.sub { font-size: 11px; color: #8f959e; }
.mono { font-family: ui-monospace, Menlo, Consolas, monospace; }
.tip { margin-top: 10px; font-size: 12px; color: #8f959e; }
</style>
