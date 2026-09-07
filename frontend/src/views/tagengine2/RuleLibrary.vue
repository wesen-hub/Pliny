<template>
  <div class="page-card">
    <el-card shadow="never">
      <div class="flex" style="justify-content:space-between;align-items:center;margin-bottom:4px">
        <div>
          <div class="pg-title">规则库 = 规则管理 + 规则流管理</div>
          <el-text size="small" type="info">「规则管理」维护<b>完整规则</b>(可视化 / 决策表 / DRL 三种定义任选),「规则流管理」把规则(及别的规则流)编排成更复杂的规则流。两者独立维护、统一版本化; 任务绑定 = 规则先跑 → 规则流后跑。字段依赖由任务装配的 数据集字段∪派生字段 满足。</el-text>
        </div>
      </div>

      <el-tabs v-model="tab" style="margin-top:8px">
        <!-- ═══ 规则管理 ═══ -->
        <el-tab-pane :label="'规则管理 (' + rulesCnt + ')'" name="rule">
          <div class="flex" style="gap:8px;margin-bottom:10px;flex-wrap:wrap">
            <el-button type="primary" @click="$router.push('/tagengine2/rule/edit')">＋ 新建规则</el-button>
            <el-button plain @click="$router.push('/tagengine2/rule/edit?mode=table')">＋ 决策表规则(金额档式)</el-button>
            <el-button plain @click="$router.push('/tagengine2/rule/edit?mode=drl')">＋ DRL 规则(高级)</el-button>
            <el-text size="small" type="info">规则支持三种定义方式: 可视化条件组 / 决策表档位 / DRL 源码</el-text>
          </div>
          <el-table :data="rules" size="small" border stripe>
            <el-table-column label="规则名称" min-width="170">
              <template #default="{ row }">
                <el-text size="small" style="font-weight:600">{{ row.name }}</el-text>
                <el-tag size="small" :type="defTagType(row)" effect="plain" style="margin-left:6px">{{ defLabel(row) }}</el-tag>
              </template>
            </el-table-column>
            <el-table-column label="版本" width="56"><template #default="{ row }"><el-tag size="small" effect="plain">v{{ row.ver }}</el-tag></template></el-table-column>
            <el-table-column label="规则定义摘要" min-width="300"><template #default="{ row }"><el-text size="small" type="info">{{ sumRule(row) }}</el-text></template></el-table-column>
            <el-table-column label="被规则流引用" width="100">
              <template #default="{ row }">
                <el-tag v-if="usedBy(row.id)" size="small" type="warning" effect="plain">{{ usedBy(row.id) }} 处</el-tag>
                <el-text v-else size="small" type="info">—</el-text>
              </template>
            </el-table-column>
            <el-table-column label="状态" width="70">
              <template #default="{ row }"><el-switch v-model="row.status" :active-value="1" :inactive-value="0" size="small" /></template>
            </el-table-column>
            <el-table-column label="操作" width="170" fixed="right">
              <template #default="{ row }">
                <el-button size="small" text type="primary" @click="$router.push('/tagengine2/rule/edit?id=' + row.id)">编辑</el-button>
                <el-button size="small" text @click="copyAsset(row)">复制</el-button>
                <el-button size="small" text type="danger" @click="delAsset(row)">删除</el-button>
              </template>
            </el-table-column>
          </el-table>
        </el-tab-pane>

        <!-- ═══ 规则流管理 ═══ -->
        <el-tab-pane :label="'规则流管理 (' + flowsCnt + ')'" name="flow">
          <div class="flex" style="gap:8px;margin-bottom:10px;flex-wrap:wrap">
            <el-button type="primary" @click="$router.push('/tagengine2/rule/flow')">＋ 新建规则流</el-button>
            <el-text size="small" type="info">决策链: 每步引用规则管理的规则或别的规则流(可嵌套), 命中/未命中分支(下一步/终止/跳转); 画布版 P1</el-text>
          </div>
          <el-table :data="flows" size="small" border stripe>
            <el-table-column label="规则流名称" min-width="170">
              <template #default="{ row }">
                <el-text size="small" style="font-weight:600">{{ row.name }}</el-text>
                <el-tag size="small" type="warning" effect="plain" style="margin-left:6px">规则流</el-tag>
              </template>
            </el-table-column>
            <el-table-column label="版本" width="56"><template #default="{ row }"><el-tag size="small" effect="plain">v{{ row.ver }}</el-tag></template></el-table-column>
            <el-table-column label="步骤数" width="70" align="center"><template #default="{ row }">{{ row.steps.length }}</template></el-table-column>
            <el-table-column label="编排结构" min-width="360"><template #default="{ row }"><el-text size="small" type="info">{{ sumRule(row) }}</el-text></template></el-table-column>
            <el-table-column label="状态" width="70">
              <template #default="{ row }"><el-switch v-model="row.status" :active-value="1" :inactive-value="0" size="small" /></template>
            </el-table-column>
            <el-table-column label="操作" width="170" fixed="right">
              <template #default="{ row }">
                <el-button size="small" text type="primary" @click="$router.push('/tagengine2/rule/flow?id=' + row.id)">编辑</el-button>
                <el-button size="small" text @click="copyAsset(row)">复制</el-button>
                <el-button size="small" text type="danger" @click="delAsset(row)">删除</el-button>
              </template>
            </el-table-column>
          </el-table>
        </el-tab-pane>
      </el-tabs>
    </el-card>
  </div>
</template>

<script setup>
import { ref, computed } from 'vue'
import { useRouter } from 'vue-router'
import { ElMessage, ElMessageBox } from 'element-plus'
import { ruleState, defLabel, defTagType, sumRule, upsertAsset, removeAsset } from './mockLib.js'

const router = useRouter()
const toast = (m, t) => (t ? ElMessage[t](m) : ElMessage.info(m))
const tab = ref('rule')
const rules = computed(() => ruleState.rules.filter(r => r.kind === 'rule'))
const flows = computed(() => ruleState.rules.filter(r => r.kind === 'flow'))
const rulesCnt = computed(() => rules.value.length)
const flowsCnt = computed(() => flows.value.length)
const usedBy = (id) => ruleState.rules.filter(x => x.kind === 'flow' && (x.steps || []).some(s => s.refId === id)).length

function copyAsset(row) {
  const c = JSON.parse(JSON.stringify(row))
  delete c.id
  c.name = row.name + '(副本)'
  upsertAsset(c)
  toast('已复制: ' + c.name + ' v' + c.ver)
}
function delAsset(row) {
  ElMessageBox.confirm('删除「' + row.name + '」?', '删除', { type: 'warning' }).then(() => {
    const r = removeAsset(row.id)
    if (!r.ok) { toast('正被规则流引用, 请先在引用处移除: ' + r.usedBy.join('、'), 'warning'); return }
    toast('已删除 ' + row.name)
  }).catch(() => {})
}
</script>

<style scoped>
.page-card { max-width: 1320px; }
.pg-title { font-size: 17px; font-weight: 700; margin-bottom: 4px; }
.flex { display: flex; }
</style>
