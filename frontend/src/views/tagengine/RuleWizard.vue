<template>
  <div class="page-card">
    <el-card shadow="never">
      <el-steps :active="step" align-center finish-status="success" style="margin-bottom:22px">
        <el-step title="数据集" />
        <el-step title="打标规则" />
        <el-step title="目标标签与调度" />
        <el-step title="试跑与发布" />
      </el-steps>
      <el-text size="small" type="info" style="display:block;text-align:center;margin-bottom:14px">
        规则打标(快捷型): 规则内部被编译为一次数据作业提交执行 —— 与「打标引擎(作业型)」同一执行/存储/治理底座; 复杂计算请用作业型</el-text>

      <!-- ① 数据集 -->
      <div v-if="step === 0" class="sec">
        <div class="sec-title">① 选择数据集与实体</div>
        <div class="ds-cards">
          <div v-for="d in ds" :key="d.name" class="ds-card" :class="{ sel: form.dataset === d.name }" @click="form.dataset = d.name">
            <b>{{ d.name }}</b>
            <div class="muted">{{ d.rows }} 行 · 主键 {{ d.pk }}</div>
          </div>
        </div>
        <el-form label-width="90px" style="max-width:560px;margin-top:12px">
          <el-form-item label="实体键"><el-select v-model="form.entityKey" style="width:200px"><el-option value="customer_id" label="customer_id" /></el-select></el-form-item>
        </el-form>
      </div>

      <!-- ② 规则 -->
      <div v-if="step === 1" class="sec">
        <div class="sec-title">② 打标规则</div>
        <el-tabs v-model="ruleTab">
          <el-tab-pane label="可视化规则" name="visual">
            <div class="flex" style="align-items:flex-start;gap:14px">
              <div style="flex:1.2;min-width:0">
                <el-text size="small" type="info" style="display:block;margin-bottom:6px">条件组(组内 且/或, 组间 {{ groupLogic }}); 命中的实体将被打上所选标签</el-text>
                <div v-for="(g, gi) in groups" :key="gi" class="grp">
                  <div class="flex" style="gap:8px;align-items:center;margin-bottom:4px">
                    <el-tag size="small" type="info" effect="plain">条件组 {{ gi + 1 }}</el-tag>
                    <el-select v-model="g.logic" size="small" style="width:80px"><el-option value="AND" label="且" /><el-option value="OR" label="或" /></el-select>
                    <el-button size="small" text type="danger" @click="groups.splice(gi, 1)">删组</el-button>
                  </div>
                  <div v-for="(c, ci) in g.conds" :key="ci" class="flex" style="gap:6px;margin:4px 0;flex-wrap:wrap">
                    <el-select v-model="c.field" size="small" style="width:150px"><el-option v-for="f in fields" :key="f" :value="f" :label="f" /></el-select>
                    <el-select v-model="c.op" size="small" style="width:100px"><el-option v-for="o in ops" :key="o.v" :value="o.v" :label="o.l" /></el-select>
                    <el-input v-model="c.value" size="small" style="width:110px" placeholder="值" />
                    <el-button size="small" text type="danger" @click="g.conds.splice(ci, 1)">✕</el-button>
                  </div>
                  <el-button size="small" text type="primary" @click="g.conds.push({ field: '', op: '==', value: '' })">＋ 条件</el-button>
                </div>
                <el-button size="small" text type="success" @click="groups.push({ logic: 'AND', conds: [{ field: '', op: '==', value: '' }] })">＋ 条件组(复杂布尔)</el-button>
                <div class="flex" style="gap:8px;margin-top:10px;align-items:center">
                  <el-text size="small" type="info">聚合: </el-text>
                  <el-select v-model="aggFn" size="small" style="width:220px">
                    <el-option value="none" label="不聚合(行级圈选)" />
                    <el-option value="sum" label="SUM(amount) 近 N 天 按实体" />
                    <el-option value="count" label="COUNT(DISTINCT order_no) 近 N 天" />
                  </el-select>
                </div>
              </div>
              <div class="ir-panel" style="flex:1;min-width:0">
                <div class="ir-head"><b>规则 IR(内部)</b><el-tag size="small" effect="dark" style="background:#22344a;border:none;color:#7ee787">编译为作业</el-tag></div>
                <pre class="ir-pre">{{ irText }}</pre>
              </div>
            </div>
          </el-tab-pane>

          <el-tab-pane label="SQL 规则" name="sql">
            <div class="flex" style="align-items:flex-start;gap:14px">
              <el-input v-model="sqlRule" type="textarea" :rows="10" style="flex:1.2;font-family:ui-monospace,Menlo,monospace;font-size:12px" />
              <div class="ir-panel" style="flex:1;min-width:0">
                <div class="ir-head"><b>产物</b></div>
                <pre class="ir-pre">-- 同一执行底座: 提交 Spark/TiDB 执行
-- 校验: 必须输出 entity_key; 禁止无界全表(护栏)</pre>
              </div>
            </div>
          </el-tab-pane>
        </el-tabs>
        <el-alert type="info" :closable="false" style="margin-top:10px"
          title="需要复杂计算(模型推理/图/任意代码)?→ 请用「打标引擎(作业型)」, 支持 PySpark / Spark jar, 此处为快捷规则通道" />
      </div>

      <!-- ③ 目标标签与调度 -->
      <div v-if="step === 2" class="sec">
        <div class="sec-title">③ 目标标签与调度</div>
        <div class="flex" style="gap:24px">
          <div style="flex:1">
            <el-text size="small" type="info" style="display:block;margin-bottom:8px">命中实体打哪些标签(布尔); 若规则带值列(SQL 输出 tag_value), 可声明为枚举/数值产出</el-text>
            <el-tree :data="tagTree" show-checkbox node-key="id" default-expand-all :props="{ label: 'name', children: 'children' }" style="border:1px solid #e4e7ed;border-radius:8px;padding:10px;max-height:260px;overflow:auto" />
          </div>
          <div style="flex:1">
            <el-form label-width="100px" style="max-width:420px">
              <el-form-item label="规则含值列">
                <el-switch v-model="hasValue" />
                <el-text v-if="hasValue" size="small" type="info" style="display:block">产出类型: <el-select v-model="valueType" size="small" style="width:130px;margin-top:4px"><el-option value="enum1" label="枚举·单值" /><el-option value="number" label="数值" /></el-select></el-text>
              </el-form-item>
              <el-form-item label="有效期(ttl)"><el-select v-model="ttl" style="width:140px"><el-option value="1d" label="1 天" /><el-option value="7d" label="7 天" /><el-option value="永久" label="永久" /></el-select></el-form-item>
              <el-form-item label="调度">
                <el-radio-group v-model="sched"><el-radio-button value="manual">手动</el-radio-button><el-radio-button value="interval">定时</el-radio-button></el-radio-group>
                <el-input v-if="sched === 'interval'" v-model="cron" size="small" style="width:240px;margin-top:6px" placeholder="0 0 2 * * ?" />
              </el-form-item>
            </el-form>
          </div>
        </div>
      </div>

      <!-- ④ 试跑/发布 -->
      <div v-if="step === 3" class="sec">
        <div class="sec-title">④ 试跑与发布</div>
        <el-descriptions :column="2" border size="small" style="max-width:760px">
          <el-descriptions-item label="数据集">{{ form.dataset }} / {{ form.entityKey }}</el-descriptions-item>
          <el-descriptions-item label="规则">{{ ruleTab === 'visual' ? '可视化(→编译 SQL 作业)' : 'SQL' }}</el-descriptions-item>
          <el-descriptions-item label="产出">布尔打标 {{ chosenCount }} 个标签<template v-if="hasValue"> + 值型</template></el-descriptions-item>
          <el-descriptions-item label="调度">{{ sched === 'interval' ? cron : '手动' }}</el-descriptions-item>
        </el-descriptions>
        <div class="flex" style="gap:10px;margin-top:14px">
          <el-button type="primary" :loading="previewing" @click="doPreview">▶ 试跑(1% 采样)</el-button>
          <el-button @click="toast('存草稿(mock)')">存草稿</el-button>
        </div>
        <template v-if="previewDone">
          <el-divider />
          <div class="flex" style="gap:14px;align-items:center">
            <el-tag size="large" effect="plain">预估命中 43,212 实体 · 覆盖率 11.9%</el-tag>
            <el-tag size="large" type="success" effect="plain">较上版 +2.1%(护栏内)</el-tag>
            <el-button type="success" @click="toast('提交审批(mock)')">提交审批发布</el-button>
          </div>
        </template>
      </div>

      <div class="nav-bar">
        <el-button :disabled="step === 0" @click="step--">上一步</el-button>
        <el-button v-if="step < 3" type="primary" @click="step++">下一步</el-button>
        <div style="flex:1"></div>
        <el-text size="small" type="info">第 {{ step + 1 }} / 4 步 · 页面设计稿(mock)</el-text>
      </div>
    </el-card>
  </div>
</template>

<script setup>
import { ref, computed } from 'vue'
import { ElMessage } from 'element-plus'

const toast = (m) => ElMessage.info(m)
const step = ref(0)
const ruleTab = ref('visual')
const ds = [
  { name: 'customer_wide', rows: '36.2 万', pk: 'customer_id' },
  { name: 'order_wide', rows: '412 万', pk: 'order_id' },
  { name: 'behavior_log_wide', rows: '3,800 万', pk: 'event_id' }
]
const form = ref({ dataset: 'customer_wide', entityKey: 'customer_id' })
const fields = ['region', 'amount_90d', 'data_secret', 'status', 'last_active_at']
const ops = [{ v: '==', l: '=' }, { v: '>=', l: '≥' }, { v: '<', l: '<' }, { v: 'in', l: '∈' }, { v: 'like', l: '含' }]
const groupLogic = '全部满足'
const groups = ref([{ logic: 'OR', conds: [{ field: 'region', op: '==', value: '华东' }, { field: 'amount_90d', op: '>=', value: '5000' }] }])
const aggFn = ref('none')

const irText = computed(() => `{
  "dataset": "${form.value.dataset}", "entityKey": "${form.value.entityKey}",
  "when": [
    { "cond": "region = '华东'", "logic": "OR" },
    { "cond": "amount_90d >= 5000" }
  ],
  "aggregate": ${JSON.stringify(aggFn.value)},
  "emit": { "tags": [${chosenTags()}], "type": "boolean" }
}`)
function chosenTags() { return JSON.stringify(['客户分层/高价值']) }
const chosenCount = ref(2)
const sqlRule = ref(`SELECT customer_id AS entity_key\nFROM ds.customer_wide\nWHERE region = '华东'\n   OR amount_90d >= 5000`)

const tagTree = ref([
  { id: 1, name: '区域', children: [{ id: 11, name: '华东' }, { id: 12, name: '华南' }] },
  { id: 2, name: '客户分层', children: [{ id: 21, name: '高价值' }, { id: 22, name: '沉睡' }] },
  { id: 3, name: '行为', children: [{ id: 31, name: '高频复购' }] },
  { id: 4, name: '风控', children: [{ id: 41, name: '黑名单' }] }
])
const hasValue = ref(false)
const valueType = ref('enum1')
const ttl = ref('1d')
const sched = ref('interval')
const cron = ref('0 0 2 * * ?')
const previewing = ref(false)
const previewDone = ref(false)
function doPreview() { previewing.value = true; setTimeout(() => { previewing.value = false; previewDone.value = true }, 900) }
</script>

<style scoped>
.sec { min-height: 300px; }
.sec-title { font-weight: 600; font-size: 15px; margin-bottom: 12px; }
.flex { display: flex; }
.muted { font-size: 12px; color: #8f959e; margin-top: 4px; }
.ds-cards { display: flex; gap: 10px; flex-wrap: wrap; }
.ds-card { border: 1px solid #e4e7ed; border-radius: 10px; padding: 12px 14px; width: 250px; cursor: pointer; }
.ds-card.sel { border-color: #409eff; background: #ecf5ff; }
.grp { border-left: 3px solid #409eff; background: #f7faff; padding: 8px 10px; border-radius: 4px; margin-bottom: 8px; }
.ir-panel { border: 1px solid #22344a; border-radius: 8px; background: #0d1b2a; }
.ir-head { display: flex; justify-content: space-between; align-items: center; padding: 8px 12px; color: #d7e3f4; border-bottom: 1px solid #22344a; }
.ir-pre { margin: 0; padding: 12px; font-size: 12px; line-height: 1.7; color: #9fdcb0; overflow: auto; max-height: 340px; white-space: pre-wrap; word-break: break-all; }
.nav-bar { display: flex; gap: 10px; margin-top: 18px; align-items: center; }
</style>
