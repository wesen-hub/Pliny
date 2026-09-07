<template>
  <div class="page-card">
    <el-card shadow="never">
      <div class="pg-title">{{ isEdit ? '编辑规则流(决策链)' : '新建规则流(决策链)' }}</div>
      <el-text size="small" type="info">规则流 = 规则单元的按序编排(决策链): 每步引用一个单元(可「连同互斥组」整组分档), 命中/未命中各自分支 —— 下一步 / 终止整链 / 跳转第 N 步。任务绑定时平铺单元先跑、再按序跑各流。画布版 P1。</el-text>

      <el-form label-width="86px" style="max-width:720px;margin-top:12px">
        <el-form-item label="流名称">
          <el-input v-model="f.name" style="width:320px" placeholder="如: 风控优先决策链" />
        </el-form-item>
        <el-form-item label="说明">
          <el-input v-model="f.desc" style="width:520px" placeholder="一句话说明(可选)" />
        </el-form-item>
      </el-form>

      <div class="sec-title">决策链步骤(自上而下执行; 每步是「一个单元或整个互斥组」的求值点)</div>
      <el-table :data="f.steps" size="small" border style="margin-bottom:8px">
        <el-table-column label="步" type="index" width="46" />
        <el-table-column label="节点(单元)" min-width="240">
          <template #default="{ row, $index }">
            <div class="flex" style="gap:6px;align-items:center;flex-wrap:wrap">
              <el-select v-model="row.unitId" size="small" style="width:190px" placeholder="选单元" @change="onUnit($index)">
                <el-option v-for="u in ruleState.units" :key="u.id" :value="u.id" :label="(u.status === 0 ? '[停] ' : '') + u.name + ' · v' + u.ver" :disabled="u.status === 0" />
              </el-select>
              <el-checkbox v-if="unitOf(row.unitId)?.activationGroup" v-model="row.withGroup" size="small">连同互斥组整组分档</el-checkbox>
            </div>
            <el-text v-if="unitOf(row.unitId)" size="small" type="info">{{ whenSum(unitOf(row.unitId)) }} → {{ thenSum(unitOf(row.unitId)) }}</el-text>
          </template>
        </el-table-column>
        <el-table-column label="命中 →" width="170">
          <template #default="{ row }">
            <div class="flex" style="gap:4px;align-items:center">
              <el-select v-model="row.hit" size="small" style="width:92px">
                <el-option value="next" label="下一步" /><el-option value="stop" label="终止" /><el-option value="jump" label="跳转" />
              </el-select>
              <el-input-number v-if="row.hit === 'jump'" v-model="row.hitJump" size="small" style="width:60px" :min="1" :max="f.steps.length" />
            </div>
          </template>
        </el-table-column>
        <el-table-column label="未命中 →" width="170">
          <template #default="{ row }">
            <div class="flex" style="gap:4px;align-items:center">
              <el-select v-model="row.miss" size="small" style="width:92px">
                <el-option value="next" label="下一步" /><el-option value="end" label="结束" /><el-option value="jump" label="跳转" />
              </el-select>
              <el-input-number v-if="row.miss === 'jump'" v-model="row.missJump" size="small" style="width:60px" :min="1" :max="f.steps.length" />
            </div>
          </template>
        </el-table-column>
        <el-table-column label="节点字段依赖" min-width="150">
          <template #default="{ row }">
            <el-tag v-for="d in nodeDeps(row)" :key="d" size="small" effect="plain" type="info" style="margin-right:4px">{{ d }}</el-tag>
            <el-text v-if="!nodeDeps(row).length" size="small" type="info">—</el-text>
          </template>
        </el-table-column>
        <el-table-column label="操作" width="128" fixed="right">
          <template #default="{ $index }">
            <el-button size="small" text :disabled="$index === 0" @click="mv($index, -1)">↑</el-button>
            <el-button size="small" text :disabled="$index === f.steps.length - 1" @click="mv($index, 1)">↓</el-button>
            <el-button size="small" text type="danger" @click="f.steps.splice($index, 1)">删</el-button>
          </template>
        </el-table-column>
      </el-table>
      <div class="flex" style="gap:10px;align-items:center;flex-wrap:wrap">
        <el-button size="small" text type="primary" @click="f.steps.push(newStep())">＋ 步骤</el-button>
        <el-button size="small" plain @click="fillDemo">填入示例(风控优先 + 金额档)</el-button>
        <el-text size="small" type="warning">停止 = 终止整个流(后续节点不再执行); 跳转目标为步序号(1 起)</el-text>
      </div>

      <div class="chain-preview">
        <el-text size="small"><b>流结构预览</b></el-text>
        <div style="margin-top:4px"><el-text size="small" type="info">{{ preview }}</el-text></div>
      </div>

      <div class="dep-bar">
        <el-text size="small"><b>流字段依赖(自动收集)</b>: 共 {{ flowDeps(f).length }} 个 —— 绑定时由任务 数据集字段 ∪ 派生字段 满足, 缺则发布拦截:</el-text>
        <div style="margin-top:4px">
          <el-tag v-for="d in flowDeps(f)" :key="d" size="small" effect="plain" style="margin-right:6px">{{ d }}</el-tag>
        </div>
      </div>

      <div class="nav-bar">
        <el-button @click="$router.back()">取消</el-button>
        <el-button @click="doTest">▶ 干跑(样本模拟)</el-button>
        <el-button type="primary" :disabled="!f.name.trim() || !f.steps.length" @click="save">保存流</el-button>
        <el-text v-if="isEdit" size="small" type="info" style="margin-left:auto">保存将升版本 v{{ f.ver }} → v{{ f.ver + 1 }}</el-text>
      </div>
    </el-card>
  </div>
</template>

<script setup>
import { ref, computed } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { ElMessage } from 'element-plus'
import { ruleState, unitOf, upsertFlow, flowOf, whenSum, thenSum, flowDeps, nodeUnits, depsOf, stepSum } from './mockLib.js'

const route = useRoute()
const router = useRouter()
const toast = (m) => ElMessage.info(m)
const isEdit = computed(() => !!route.query.id)
const src = isEdit.value ? flowOf(route.query.id) : null
const f = ref(src ? JSON.parse(JSON.stringify(src)) : { id: undefined, name: '', desc: '', status: 1, steps: [] })

const newStep = () => ({ unitId: '', withGroup: false, hit: 'next', hitJump: 1, miss: 'next', missJump: 1 })
const fillDemo = () => {
  f.value.steps = [
    { unitId: 'u4', withGroup: false, hit: 'stop', hitJump: 0, miss: 'next', missJump: 0 },
    { unitId: 'u1', withGroup: true, hit: 'next', hitJump: 0, miss: 'end', missJump: 0 }
  ]
}
function onUnit(i) {
  const st = f.value.steps[i]
  st.withGroup = false
}
function mv(i, d) {
  const arr = f.value.steps
  const j = i + d
  ;[arr[i], arr[j]] = [arr[j], arr[i]]
}
const nodeDeps = (st) => {
  const s = new Set()
  nodeUnits(st).forEach(u => depsOf(u).forEach(x => s.add(x)))
  return [...s]
}
const preview = computed(() => (f.value.steps || []).map((s, i) => (i + 1) + '. ' + stepSum(s, i)).join('  →  ') || '(空链)')

function doTest() {
  if (!f.value.name.trim()) { toast('先填流名称'); return }
  ElMessage.success('干跑(mock): 样本 5,000 行 · 黑名单命中 63 行(链终止) · 金额档分档 4,938 行 · 流无异常')
}
function save() {
  const fv = upsertFlow(JSON.parse(JSON.stringify(f.value)))
  toast('流已保存: ' + fv.name + ' v' + fv.ver + ' · ' + fv.steps.length + ' 步' + (isEdit.value ? '(升版)' : ''))
  router.back()
}
</script>

<style scoped>
.page-card { max-width: 1280px; }
.pg-title { font-size: 17px; font-weight: 700; margin-bottom: 4px; }
.sec-title { font-weight: 600; font-size: 14px; color: #2c3e50; margin: 12px 0 8px; }
.flex { display: flex; }
.chain-preview { background: #0d1b2a; color: #9fdcb0; border-radius: 6px; padding: 10px 12px; margin-top: 14px; }
.chain-preview .el-text { color: #9fdcb0 !important; }
.dep-bar { background: #f4f8ff; border: 1px solid #d6e4ff; border-radius: 6px; padding: 8px 10px; margin-top: 10px; }
.nav-bar { display: flex; gap: 10px; margin-top: 16px; align-items: center; }
</style>
