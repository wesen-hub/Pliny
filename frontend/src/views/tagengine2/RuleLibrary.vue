<template>
  <div class="page-card">
    <el-card shadow="never">
      <div class="flex" style="justify-content:space-between;align-items:center;margin-bottom:4px">
        <div>
          <div class="pg-title">规则库(V2)</div>
          <el-text size="small" type="info">规则资产独立维护: 原子「规则单元」可被任务平铺引用或被「规则流」编排; 决策表 = 批量生产互斥单元的工具; DRL = 源码视图(高级)。字段依赖只声明字段名, 由任务的 数据集字段 ∪ 派生字段 满足(发布时校验)。</el-text>
        </div>
      </div>

      <el-tabs v-model="tab" style="margin-top:6px">
        <!-- ═══ 规则单元 ═══ -->
        <el-tab-pane :label="'规则单元 (' + ruleState.units.length + ')'" name="unit">
          <div class="flex" style="gap:8px;margin-bottom:10px;flex-wrap:wrap">
            <el-button type="primary" @click="goCreateUnit">＋ 新建单元</el-button>
            <el-button @click="dtOpen = true">⚙ 决策表生成互斥单元(分档)</el-button>
            <el-text size="small" type="info">示例: 金额档 = 3 个同互斥组单元(大额/中额/小额), 只命中最高档</el-text>
          </div>
          <el-table :data="ruleState.units" size="small" border stripe>
            <el-table-column label="名称" min-width="170">
              <template #default="{ row }"><el-text size="small">{{ row.name }}</el-text></template>
            </el-table-column>
            <el-table-column label="版本" width="60"><template #default="{ row }"><el-tag size="small" effect="plain">v{{ row.ver }}</el-tag></template></el-table-column>
            <el-table-column label="WHEN(条件)" min-width="200"><template #default="{ row }"><el-text size="small" type="info">{{ whenSum(row) }}</el-text></template></el-table-column>
            <el-table-column label="THEN(动作)" min-width="170"><template #default="{ row }"><el-text size="small">{{ thenSum(row) }}</el-text></template></el-table-column>
            <el-table-column label="优先级" width="70" align="center"><template #default="{ row }">{{ row.salience }}</template></el-table-column>
            <el-table-column label="互斥组" width="130">
              <template #default="{ row }"><el-tag v-if="row.activationGroup" size="small" type="warning" effect="plain">{{ row.activationGroup }}</el-tag><el-text v-else size="small" type="info">—</el-text></template>
            </el-table-column>
            <el-table-column label="字段依赖" min-width="140">
              <template #default="{ row }">
                <el-tag v-for="d in depsOf(row)" :key="d" size="small" type="info" effect="plain" style="margin-right:4px">{{ d }}</el-tag>
                <el-text v-if="!depsOf(row).length" size="small" type="info">—</el-text>
              </template>
            </el-table-column>
            <el-table-column label="状态" width="70">
              <template #default="{ row }"><el-switch v-model="row.status" :active-value="1" :inactive-value="0" size="small" /></template>
            </el-table-column>
            <el-table-column label="操作" width="150" fixed="right">
              <template #default="{ row }">
                <el-button size="small" text type="primary" @click="goEditUnit(row.id)">编辑</el-button>
                <el-button size="small" text @click="copyUnit(row)">复制</el-button>
                <el-button size="small" text type="danger" @click="delUnit(row)">删除</el-button>
              </template>
            </el-table-column>
          </el-table>
        </el-tab-pane>

        <!-- ═══ 规则流 ═══ -->
        <el-tab-pane :label="'规则流 (' + ruleState.flows.length + ')'" name="flow">
          <div class="flex" style="gap:8px;margin-bottom:10px;flex-wrap:wrap">
            <el-button type="primary" @click="goCreateFlow">＋ 新建流</el-button>
            <el-text size="small" type="info">决策链: 每步引用一个单元(可连同互斥组), 按 命中/未命中 分支(下一步 / 终止 / 跳转); 画布版 P1。</el-text>
          </div>
          <el-table :data="ruleState.flows" size="small" border stripe>
            <el-table-column label="名称" min-width="150"><template #default="{ row }"><el-text size="small">{{ row.name }}</el-text></template></el-table-column>
            <el-table-column label="版本" width="60"><template #default="{ row }"><el-tag size="small" effect="plain">v{{ row.ver }}</el-tag></template></el-table-column>
            <el-table-column label="步骤数" width="70" align="center"><template #default="{ row }">{{ row.steps.length }}</template></el-table-column>
            <el-table-column label="决策链结构" min-width="360">
              <template #default="{ row }">
                <el-text size="small" type="info">{{ flowBrief(row) }}</el-text>
              </template>
            </el-table-column>
            <el-table-column label="状态" width="70">
              <template #default="{ row }"><el-switch v-model="row.status" :active-value="1" :inactive-value="0" size="small" /></template>
            </el-table-column>
            <el-table-column label="操作" width="150" fixed="right">
              <template #default="{ row }">
                <el-button size="small" text type="primary" @click="goEditFlow(row.id)">编辑</el-button>
                <el-button size="small" text @click="copyFlow(row)">复制</el-button>
                <el-button size="small" text type="danger" @click="delFlow(row)">删除</el-button>
              </template>
            </el-table-column>
          </el-table>
        </el-tab-pane>
      </el-tabs>
    </el-card>

    <!-- ═══ 决策表: 批量生成互斥单元 ═══ -->
    <el-dialog v-model="dtOpen" title="决策表 → 生成互斥单元" width="640">
      <el-alert type="info" :closable="false" style="margin-bottom:10px"
        title="每行 = 一个档位; 生成的单元自动同互斥组(只命中最高档), 落规则库「单元」, 可被流以「连同互斥组」整组编排" />
      <div class="flex" style="gap:8px;align-items:center;margin-bottom:8px;flex-wrap:wrap">
        <el-text size="small" type="info">按字段分档 → 打标签</el-text>
        <el-select v-model="dt.field" size="small" style="width:160px"><el-option v-for="f in fieldOpts" :key="f" :value="f" :label="f" /></el-select>
        <el-select v-model="dt.tag" size="small" style="width:180px"><el-option v-for="t in tagOptions" :key="t" :value="t" :label="t" /></el-select>
        <el-text size="small" type="info">(组 = 该标签)</el-text>
      </div>
      <el-table :data="dt.rows" size="small" border>
        <el-table-column label="档" type="index" width="46" />
        <el-table-column label="条件">
          <template #default="{ row }">
            <div class="flex" style="gap:6px;align-items:center">
              <el-select v-model="row.op" size="small" style="width:64px"><el-option value=">=" label="≥" /><el-option value=">" label=">" /><el-option value="<" label="<" /><el-option value="==" label="=" /></el-select>
              <el-input v-model="row.th" size="small" style="width:110px" placeholder="阈值" />
            </div>
          </template>
        </el-table-column>
        <el-table-column label="命中 → 标签值">
          <template #default="{ row }"><el-input v-model="row.val" size="small" placeholder="如 大额" style="width:150px" /></template>
        </el-table-column>
        <el-table-column width="56"><template #default="{ $index }"><el-button size="small" text type="danger" @click="dt.rows.splice($index, 1)">✕</el-button></template></el-table-column>
      </el-table>
      <div class="flex" style="gap:10px;margin-top:8px;align-items:center;flex-wrap:wrap">
        <el-button size="small" text type="primary" @click="dt.rows.push({ op: '>=', th: '', val: '' })">＋ 档位行</el-button>
        <el-text size="small" type="info">兜底档值</el-text>
        <el-input v-model="dt.elseVal" size="small" style="width:100px" placeholder="如 小额" />
        <el-button size="small" plain @click="fillDemo">填入示例(金额档)</el-button>
        <el-button type="success" size="small" @click="gen">生成互斥单元</el-button>
      </div>
    </el-dialog>
  </div>
</template>

<script setup>
import { ref } from 'vue'
import { useRouter } from 'vue-router'
import { ElMessage, ElMessageBox } from 'element-plus'
import { ruleState, tagOptions, depsOf, whenSum, thenSum, flowBrief, unitOf, addUnit, removeUnit, removeFlow, upsertFlow, metaBool } from './mockLib.js'

const router = useRouter()
const toast = (m, t) => (t ? ElMessage[t](m) : ElMessage.info(m))
const tab = ref('unit')
const fieldOpts = ['amount', 'pay_amount', 'quantity', 'order_date']

const goCreateUnit = () => router.push('/tagengine2/rule/unit')
const goEditUnit = (id) => router.push('/tagengine2/rule/unit?id=' + id)
const goCreateFlow = () => router.push('/tagengine2/rule/flow')
const goEditFlow = (id) => router.push('/tagengine2/rule/flow?id=' + id)

function copyUnit(u) {
  addUnit(JSON.parse(JSON.stringify({ ...u, id: undefined, name: u.name + '(副本)', activationGroup: '', status: 1 })))
  toast('已复制为副本单元(副本不带互斥组)')
}
function copyFlow(f) {
  const c = JSON.parse(JSON.stringify(f))
  upsertFlow({ ...c, id: undefined, name: f.name + '(副本)' })
  toast('已复制流(节点仍引用原单元)')
}
async function delUnit(u) {
  const r = removeUnit(u.id)
  if (!r.ok) { toast('该单元正被流引用: ' + r.usedBy.join('、') + ', 请先在流中移除节点', 'warning'); return }
  toast('已删除单元 ' + u.name)
}
function delFlow(f) {
  ElMessageBox.confirm('删除规则流「' + f.name + '」? 不影响其引用的单元。', '删除流', { type: 'warning' }).then(() => {
    removeFlow(f.id)
    toast('已删除流')
  }).catch(() => {})
}

/* 决策表 */
const dtOpen = ref(false)
const dt = ref({ field: 'amount', tag: '订单/金额档', elseVal: '小额', rows: [{ op: '>=', th: '10000', val: '大额' }, { op: '>=', th: '1000', val: '中额' }] })
function fillDemo() {
  dt.value = { field: 'amount', tag: '订单/金额档', elseVal: '小额', rows: [{ op: '>=', th: '10000', val: '大额' }, { op: '>=', th: '1000', val: '中额' }] }
}
function gen() {
  const tag = dt.value.tag
  const n0 = ruleState.units.filter(u => u.activationGroup === tag).length
  let made = 0
  dt.value.rows.forEach((row, i) => {
    if (!row.val || row.th === '' || row.th === undefined) return
    addUnit({
      name: '档-' + tag.split('/').pop() + '-' + row.val, salience: 100 - i * 10, activationGroup: tag, status: 1,
      when: [{ logic: 'AND', conds: [{ field: dt.value.field, op: row.op, val: String(row.th) }] }],
      actions: [{ type: 'tag', tag, val: row.val }],
      desc: '决策表生成: ' + dt.value.field + ' ' + row.op + ' ' + row.th + ' → ' + tag + '=' + row.val
    })
    made++
  })
  if (dt.value.elseVal) {
    addUnit({
      name: '档-' + tag.split('/').pop() + '-兜底', salience: 5, activationGroup: tag, status: 1,
      when: [], actions: [{ type: 'tag', tag, val: dt.value.elseVal }],
      desc: '决策表兜底档: 恒真 → ' + tag + '=' + dt.value.elseVal
    })
    made++
  }
  const existed = ruleState.units.filter(u => u.activationGroup === tag).length > n0
  toast('已生成 ' + made + ' 个互斥单元(组=' + tag + (existed ? ', 与原组单元同组)' : ')') + '; 需布尔标签请直接建单元')
}
</script>

<style scoped>
.page-card { max-width: 1320px; }
.pg-title { font-size: 17px; font-weight: 700; margin-bottom: 4px; }
.flex { display: flex; }
</style>
