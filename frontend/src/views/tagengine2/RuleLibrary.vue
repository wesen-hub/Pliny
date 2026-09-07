<template>
  <div class="page-card">
    <el-card shadow="never" v-loading="loading">
      <div class="flex" style="justify-content:space-between;align-items:center;margin-bottom:4px">
        <div>
          <div class="pg-title">规则库 = 规则管理 + 规则流管理</div>
          <el-text size="small" type="info">「规则管理」维护完整规则(可视化/决策表/DRL 三种定义任选),「规则流管理」把规则(及别的规则流)编排成更复杂的规则流;已接后端 /api/tagengine/*(数据实时)。字段依赖由任务装配满足, 发布时后端校验。</el-text>
        </div>
      </div>

      <el-tabs v-model="tab" style="margin-top:8px">
        <!-- ═══ 规则管理 ═══ -->
        <el-tab-pane :label="'规则管理 (' + rules.length + ')'" name="rule">
          <div class="flex" style="gap:8px;margin-bottom:10px;flex-wrap:wrap">
            <el-button type="primary" @click="$router.push('/tagengine2/rule/edit')">＋ 新建规则</el-button>
            <el-button plain @click="$router.push('/tagengine2/rule/edit?mode=table')">＋ 决策表规则(金额档式)</el-button>
            <el-button plain @click="$router.push('/tagengine2/rule/edit?mode=drl')">＋ DRL 规则(高级)</el-button>
            <el-button text type="primary" @click="load">⟳ 刷新</el-button>
          </div>
          <el-table :data="rules" size="small" border stripe>
            <el-table-column label="规则名称" min-width="180">
              <template #default="{ row }">
                <el-text size="small" style="font-weight:600">{{ row.name }}</el-text>
                <el-tag size="small" :type="defTagType(row)" effect="plain" style="margin-left:6px">{{ defLabel(row) }}</el-tag>
              </template>
            </el-table-column>
            <el-table-column label="版本" width="60"><template #default="{ row }"><el-tag size="small" effect="plain">v{{ row.ver }}</el-tag></template></el-table-column>
            <el-table-column label="规则定义摘要" min-width="300"><template #default="{ row }"><el-text size="small" type="info">{{ sumRule(row) }}</el-text></template></el-table-column>
            <el-table-column label="字段依赖" min-width="120">
              <template #default="{ row }">
                <el-tag v-for="d in (row.deps || [])" :key="d" size="small" type="info" effect="plain" style="margin-right:4px">{{ d }}</el-tag>
                <el-text v-if="!row.deps || !row.deps.length" size="small" type="info">—</el-text>
              </template>
            </el-table-column>
            <el-table-column label="被规则流引用" width="100">
              <template #default="{ row }">
                <el-tag v-if="(row.usedBy || []).length" size="small" type="warning" effect="plain">{{ row.usedBy.length }} 处</el-tag>
                <el-text v-else size="small" type="info">—</el-text>
              </template>
            </el-table-column>
            <el-table-column label="状态" width="70">
              <template #default="{ row }"><el-switch v-model="row.status" :active-value="1" :inactive-value="0" size="small" @change="toggle(row)" /></template>
            </el-table-column>
            <el-table-column label="操作" width="170" fixed="right">
              <template #default="{ row }">
                <el-button size="small" text type="primary" @click="$router.push('/tagengine2/rule/edit?id=' + row.id)">编辑</el-button>
                <el-button size="small" text @click="copyRule(row)">复制</el-button>
                <el-button size="small" text type="danger" @click="delRule(row)">删除</el-button>
              </template>
            </el-table-column>
          </el-table>
        </el-tab-pane>

        <!-- ═══ 规则流管理 ═══ -->
        <el-tab-pane :label="'规则流管理 (' + flows.length + ')'" name="flow">
          <div class="flex" style="gap:8px;margin-bottom:10px;flex-wrap:wrap">
            <el-button type="primary" @click="$router.push('/tagengine2/rule/flow')">＋ 新建规则流</el-button>
            <el-button text type="primary" @click="load">⟳ 刷新</el-button>
          </div>
          <el-table :data="flows" size="small" border stripe>
            <el-table-column label="规则流名称" min-width="180">
              <template #default="{ row }">
                <el-text size="small" style="font-weight:600">{{ row.name }}</el-text>
                <el-tag size="small" type="warning" effect="plain" style="margin-left:6px">规则流</el-tag>
              </template>
            </el-table-column>
            <el-table-column label="版本" width="60"><template #default="{ row }"><el-tag size="small" effect="plain">v{{ row.ver }}</el-tag></template></el-table-column>
            <el-table-column label="步骤数" width="70" align="center"><template #default="{ row }">{{ (row.def && row.def.steps || []).length }}</template></el-table-column>
            <el-table-column label="编排结构" min-width="360"><template #default="{ row }"><el-text size="small" type="info">{{ sumRule(row) }}</el-text></template></el-table-column>
            <el-table-column label="状态" width="70">
              <template #default="{ row }"><el-switch v-model="row.status" :active-value="1" :inactive-value="0" size="small" @change="toggle(row)" /></template>
            </el-table-column>
            <el-table-column label="操作" width="170" fixed="right">
              <template #default="{ row }">
                <el-button size="small" text type="primary" @click="$router.push('/tagengine2/rule/flow?id=' + row.id)">编辑</el-button>
                <el-button size="small" text @click="copyRule(row)">复制</el-button>
                <el-button size="small" text type="danger" @click="delRule(row)">删除</el-button>
              </template>
            </el-table-column>
          </el-table>
        </el-tab-pane>
      </el-tabs>
    </el-card>
  </div>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue'
import { useRouter } from 'vue-router'
import { ElMessage, ElMessageBox } from 'element-plus'
import { rulesApi } from '../../api/tagengine.js'
import { sumRule, defLabel, defTagType } from './mockLib.js'

const router = useRouter()
const toast = (m, t) => (t ? ElMessage[t](m) : ElMessage.info(m))
const tab = ref('rule')
const loading = ref(false)
const all = ref([])
const rules = computed(() => all.value.filter(r => r.kind === 'rule'))
const flows = computed(() => all.value.filter(r => r.kind === 'flow'))

async function load() {
  loading.value = true
  try {
    all.value = await rulesApi.list()
  } catch (e) { /* request 层已提示 */ } finally { loading.value = false }
}
onMounted(load)

async function toggle(row) {
  try {
    const body = { name: row.name, descr: row.descr || '', status: row.status, def: row.def }
    await rulesApi.update(row.id, body)
    toast(`已${row.status === 1 ? '启用' : '停用'} ${row.name}(升版 v${(row.ver || 0) + 1})`)
    load()
  } catch (e) {
    row.status = row.status === 1 ? 0 : 1
  }
}
async function copyRule(row) {
  try {
    const r = await rulesApi.copy(row.id)
    toast('已复制: ' + r.name + ' v' + r.ver)
    load()
  } catch (e) { /* 已提示 */ }
}
function delRule(row) {
  ElMessageBox.confirm('删除「' + row.name + '」?', '删除', { type: 'warning' }).then(async () => {
    try {
      await rulesApi.remove(row.id)
      toast('已删除')
      load()
    } catch (e) { /* 已提示(被引用等) */ }
  }).catch(() => {})
}
</script>

<style scoped>
.page-card { max-width: 1320px; }
.pg-title { font-size: 17px; font-weight: 700; margin-bottom: 4px; }
.flex { display: flex; }
</style>
