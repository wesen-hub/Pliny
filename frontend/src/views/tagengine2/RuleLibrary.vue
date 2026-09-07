<template>
  <div class="page-card">
    <el-card shadow="never">
      <div class="flex" style="justify-content:space-between;align-items:center;margin-bottom:4px">
        <div>
          <div class="pg-title">规则库</div>
          <el-text size="small" type="info">规则 = 可复用的打标资产, 统一在此维护(增删改/版本/启停)。定义形态两种: <b>基础规则</b>(单条 WHEN→THEN, 或「分档规则」一条表达多档位)与 <b>编排规则</b>(规则流: 把规则库里的规则/规则流编排成新的规则流, 可嵌套)。字段依赖自动收集, 由任务装配的 数据集字段∪派生字段 满足(发布校验)。</el-text>
        </div>
      </div>

      <div class="flex" style="gap:8px;margin:12px 0;flex-wrap:wrap;align-items:center">
        <el-radio-group v-model="filter" size="small">
          <el-radio-button value="all">全部 ({{ ruleState.rules.length }})</el-radio-button>
          <el-radio-button value="simple">基础规则 ({{ baseCount }})</el-radio-button>
          <el-radio-button value="flow">编排规则 ({{ flowCount }})</el-radio-button>
        </el-radio-group>
        <div style="flex:1"></div>
        <el-button type="primary" size="small" @click="$router.push('/tagengine2/rule/simple')">＋ 基础规则</el-button>
        <el-button type="primary" size="small" plain @click="$router.push('/tagengine2/rule/simple?style=bucket')">＋ 分档规则(金额档式)</el-button>
        <el-button type="warning" size="small" plain @click="$router.push('/tagengine2/rule/flow')">＋ 编排规则(规则流)</el-button>
      </div>

      <el-table :data="shown" size="small" border stripe>
        <el-table-column label="名称" min-width="160">
          <template #default="{ row }">
            <el-text size="small" style="font-weight:600">{{ row.name }}</el-text>
            <el-tag size="small" :type="kindTagType(row)" effect="plain" style="margin-left:6px">{{ kindLabel(row) }}</el-tag>
          </template>
        </el-table-column>
        <el-table-column label="版本" width="56"><template #default="{ row }"><el-tag size="small" effect="plain">v{{ row.ver }}</el-tag></template></el-table-column>
        <el-table-column label="规则定义摘要" min-width="300">
          <template #default="{ row }"><el-text size="small" type="info">{{ sumRule(row) }}</el-text></template>
        </el-table-column>
        <el-table-column label="被引用(编排)" min-width="100">
          <template #default="{ row }">
            <el-tag v-if="refCount(row.id)" size="small" type="warning" effect="plain">{{ refCount(row.id) }} 处</el-tag>
            <el-text v-else size="small" type="info">—</el-text>
          </template>
        </el-table-column>
        <el-table-column label="状态" width="70">
          <template #default="{ row }"><el-switch v-model="row.status" :active-value="1" :inactive-value="0" size="small" /></template>
        </el-table-column>
        <el-table-column label="操作" width="170" fixed="right">
          <template #default="{ row }">
            <el-button size="small" text type="primary" @click="goEdit(row)">编辑</el-button>
            <el-button size="small" text @click="copyRule(row)">复制</el-button>
            <el-button size="small" text type="danger" @click="delRule(row)">删除</el-button>
          </template>
        </el-table-column>
      </el-table>
      <el-text size="small" type="info" style="margin-top:8px;display:block">提示: 金额档 = 一条「分档规则」; 想让黑名单先判、金额档后判 → 新建「编排规则」引用它们(可引用别的编排规则, 层层组合; 引用会被环检测拦下)。</el-text>
    </el-card>
  </div>
</template>

<script setup>
import { ref, computed } from 'vue'
import { useRouter } from 'vue-router'
import { ElMessage, ElMessageBox } from 'element-plus'
import { ruleState, kindLabel, kindTagType, sumRule, upsertRule, removeRule } from './mockLib.js'

const router = useRouter()
const toast = (m, t) => (t ? ElMessage[t](m) : ElMessage.info(m))
const filter = ref('all')
const baseCount = computed(() => ruleState.rules.filter(r => r.kind === 'simple').length)
const flowCount = computed(() => ruleState.rules.filter(r => r.kind === 'flow').length)
const shown = computed(() => (filter.value === 'all' ? ruleState.rules : ruleState.rules.filter(r => r.kind === filter.value)))
const refCount = (id) => ruleState.rules.filter(x => x.kind === 'flow' && (x.steps || []).some(s => s.ruleId === id)).length

const goEdit = (row) => router.push(row.kind === 'flow' ? '/tagengine2/rule/flow?id=' + row.id : '/tagengine2/rule/simple?id=' + row.id)
function copyRule(row) {
  const c = JSON.parse(JSON.stringify(row))
  delete c.id
  c.name = row.name + '(副本)'
  c.desc = (c.desc ? c.desc + ' ' : '') + '副本'
  upsertRule(c)
  toast('已复制: ' + c.name + ' v' + c.ver)
}
function delRule(row) {
  ElMessageBox.confirm('删除规则「' + row.name + '」?', '删除', { type: 'warning' }).then(() => {
    const r = removeRule(row.id)
    if (!r.ok) { toast('被编排规则引用, 请先在引用处移除: ' + r.usedBy.join('、'), 'warning'); return }
    toast('已删除 ' + row.name)
  }).catch(() => {})
}
</script>

<style scoped>
.page-card { max-width: 1320px; }
.pg-title { font-size: 17px; font-weight: 700; margin-bottom: 4px; }
.flex { display: flex; }
</style>
