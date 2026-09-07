<template>
  <div class="page-card">
    <el-card shadow="never">
      <div class="flex" style="justify-content:space-between;align-items:center;margin-bottom:4px">
        <div>
          <div class="pg-title">打标任务(V2)</div>
          <el-text size="small" type="info">任务 = 执行形态(批/流/在线) + 事实装配(数据集 + 派生字段) + 绑定的规则资产; 规则在「规则库」统一维护(单条/分档/编排流三种形态), 任务引用或内联新建落库。</el-text>
        </div>
        <el-button type="primary" @click="$router.push('/tagengine2/task/create')">＋ 新建打标任务</el-button>
      </div>

      <el-table :data="tasks" size="small" border stripe style="margin-top:10px">
        <el-table-column label="任务名称" min-width="150">
          <template #default="{ row }"><el-text size="small">{{ row.name }}</el-text></template>
        </el-table-column>
        <el-table-column label="执行形态" width="90">
          <template #default="{ row }">
            <el-tag :type="row.mode === 'batch' ? 'primary' : row.mode === 'stream' ? 'warning' : 'success'" size="small" effect="plain">
              {{ { batch: '批', stream: '流', online: '在线' }[row.mode] }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column label="数据集 / 事实" min-width="150"><template #default="{ row }"><el-text size="small">{{ row.fact }}</el-text></template></el-table-column>
        <el-table-column label="绑定规则(按执行序)" min-width="300">
          <template #default="{ row }">
            <div style="display:flex;flex-wrap:wrap;gap:4px;align-items:center">
              <el-tag v-for="(b, i) in row.bound" :key="i" size="small" :type="b.kind === 'flow' ? 'warning' : 'primary'" effect="plain">
                {{ b.kind === 'flow' ? '编排' : '基础·' + b.style }}:{{ b.name }} v{{ b.ver }}
              </el-tag>
              <el-text size="small" type="info" style="margin-left:2px">(先 {{ row.bound.filter(b => b.kind !== 'flow').length }} 基础 → 后 {{ row.bound.filter(b => b.kind === 'flow').length }} 编排)</el-text>
            </div>
          </template>
        </el-table-column>
        <el-table-column label="调度" width="110"><template #default="{ row }"><el-text size="small">{{ row.sched }}</el-text></template></el-table-column>
        <el-table-column label="状态" width="80">
          <template #default="{ row }">
            <el-tag size="small" :type="row.status === '运行中' ? 'success' : 'info'" effect="plain">{{ row.status }}</el-tag>
          </template>
        </el-table-column>
        <el-table-column label="操作" width="150" fixed="right">
          <template #default="{ row }">
            <el-button size="small" text type="primary" @click="edit(row)">编辑</el-button>
            <el-button size="small" text type="danger" @click="del(row)">删除</el-button>
          </template>
        </el-table-column>
      </el-table>
      <el-text size="small" type="info" style="margin-top:8px;display:block">mock 示例: 规则改动在规则库统一升版本, 任务记录引用版本; 执行引擎按发布快照跑。</el-text>
    </el-card>
  </div>
</template>

<script setup>
import { ref } from 'vue'
import { useRouter } from 'vue-router'
import { ElMessage, ElMessageBox } from 'element-plus'

const router = useRouter()
const toast = (m) => ElMessage.info(m)
const tasks = ref([
  {
    id: 't1', name: '客户价值分层批任务', mode: 'batch', fact: 'customer_wide(36.2 万) · 派生 2', sched: '每日 02:00',
    status: '运行中',
    bound: [
      { kind: 'simple', style: 'cond', name: '黑名单命中', ver: 1 },
      { kind: 'flow', name: '风控优先决策链', ver: 1 },
      { kind: 'simple', style: 'bucket', name: '金额档', ver: 1 }
    ]
  },
  {
    id: 't2', name: '支付事件实时打标', mode: 'stream', fact: 'pay-events · 5 分钟滑动窗 · 派生 1', sched: '常驻',
    status: '运行中',
    bound: [{ kind: 'simple', style: 'bucket', name: '金额档', ver: 1 }]
  },
  {
    id: 't3', name: '在线风控瞬时置位', mode: 'online', fact: 'Kafka 事件 → Redis TTL', sched: '常驻',
    status: '已停',
    bound: [{ kind: 'simple', style: 'cond', name: '黑名单命中', ver: 1 }]
  }
])
const edit = (row) => { toast('演示: 任务编辑向导中规则为「绑定/新增」, 规则本体在规则库维护'); router.push('/tagengine2/task/create') }
const del = (row) => {
  ElMessageBox.confirm('删除任务「' + row.name + '」? 仅解除任务, 不影响规则库资产。', '删除任务', { type: 'warning' }).then(() => {
    tasks.value = tasks.value.filter(t => t.id !== row.id)
    toast('已删除')
  }).catch(() => {})
}
</script>

<style scoped>
.page-card { max-width: 1320px; }
.pg-title { font-size: 17px; font-weight: 700; margin-bottom: 4px; }
.flex { display: flex; }
</style>
