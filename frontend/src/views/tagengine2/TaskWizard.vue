<template>
  <div class="page-card">
    <el-card shadow="never">
      <el-steps :active="step" align-center finish-status="success" style="margin-bottom:16px">
        <el-step title="数据集与事实" />
        <el-step title="规则集(WHEN→THEN)" />
        <el-step title="试跑与命中分析" />
        <el-step title="调度与发布" />
      </el-steps>

      <!-- ═══ ① 数据集与事实 ═══ -->
      <div v-if="step === 0" class="sec">
        <div class="sec-title">① 执行形态 + 数据集与事实(Fact)</div>
        <div class="flex" style="gap:10px;align-items:center;margin-bottom:12px;flex-wrap:wrap">
          <span style="font-weight:600">执行形态</span>
          <el-radio-group v-model="mode">
            <el-radio-button value="batch">批(离线/定时)</el-radio-button>
            <el-radio-button value="stream">流(近实时/窗口)</el-radio-button>
            <el-radio-button value="online">在线(瞬时置位)</el-radio-button>
          </el-radio-group>
          <el-text size="small" type="info">{{ modeHint }}</el-text>
        </div>
        <div class="flex" style="gap:10px;flex-wrap:wrap;align-items:center;margin-bottom:10px">
          <template v-if="mode === 'batch'">
            <el-select v-model="form.dataset" style="width:230px" @change="onDs">
              <el-option v-for="d in dsList" :key="d.name" :value="d.name" :label="d.name + ' · ' + d.rows" />
            </el-select>
            <el-text size="small" type="info">实体键</el-text>
            <el-select v-model="form.entityKey" style="width:150px"><el-option v-for="k in pksOf" :key="k" :value="k" :label="k" /></el-select>
            <el-text size="small" type="info">时间窗</el-text>
            <el-select v-model="form.win" style="width:110px"><el-option value="none" label="全量" /><el-option value="rel" label="近 N 天" /></el-select>
            <template v-if="form.win === 'rel'">
              <el-input-number v-model="form.winNum" size="small" style="width:90px" :min="1" />
              <el-select v-model="form.winOn" size="small" style="width:140px"><el-option v-for="f in dateFields" :key="f" :value="f" :label="f" /></el-select>
            </template>
            <el-text size="small" type="success">装配: 每行 = 一个 Fact, 规则逐行求值</el-text>
          </template>
          <template v-else-if="mode === 'stream'">
            <el-select v-model="st.topic" style="width:240px">
              <el-option value="pay-events" label="pay-events(支付事件)" />
              <el-option value="cdc-binlog-events" label="CDC binlog" />
              <el-option value="click-stream" label="click-stream" />
            </el-select>
            <el-text size="small" type="info">实体键(事件字段)</el-text>
            <el-select v-model="form.entityKey" style="width:140px"><el-option value="customer_id" label="customer_id" /></el-select>
            <el-text size="small" type="info">事件时间</el-text>
            <el-select v-model="st.eventTime" size="small" style="width:130px"><el-option value="event_time" label="event_time" /></el-select>
            <el-text size="small" type="info">窗口</el-text>
            <el-radio-group v-model="st.winType" size="small"><el-radio-button value="sliding">滑动</el-radio-button><el-radio-button value="tumbling">滚动</el-radio-button></el-radio-group>
            <el-input-number v-model="st.winLen" size="small" style="width:80px" :min="1" />
            <el-select v-model="st.winUnit" size="small" style="width:70px"><el-option value="m" label="分钟" /><el-option value="h" label="小时" /><el-option value="d" label="天" /></el-select>
            <el-text size="small" type="success">事实 = 事件(窗口聚合见派生字段)</el-text>
          </template>
          <template v-else>
            <el-radio-group v-model="st.srcType" size="small"><el-radio-button value="topic">Kafka 事件</el-radio-button><el-radio-button value="api">平台 API</el-radio-button></el-radio-group>
            <el-select v-if="st.srcType === 'topic'" v-model="st.topic" size="small" style="width:220px"><el-option value="app-heartbeat" label="app-heartbeat" /></el-select>
            <el-text v-else size="small" type="info">POST /api/tags/ingest { entity, tag, value, ts }</el-text>
            <el-text size="small" type="success">事实 = 单事件; 只支持布尔/瞬时标签(注册表 timing=instant)</el-text>
          </template>
        </div>

        <div class="cfg-title">派生字段(事实预处理: 字段变换 / 聚合统计; 规则 WHEN 可直接引用)
          <el-text v-if="mode === 'stream'" size="small" type="warning" style="margin-left:8px">流模式: 聚合将用窗口实现(近 N {{ st.winUnit === 'm' ? '分钟' : '小时' }})</el-text>
          <el-text v-else-if="mode === 'online'" size="small" type="warning" style="margin-left:8px">在线模式: 仅事件字段直取, 不支持聚合派生</el-text>
        </div>
        <div v-for="(d, di) in derived" :key="'d' + di" class="metric-row">
          <el-input v-model="d.name" size="small" style="width:140px" placeholder="字段名, 如 pay7d" />
          <el-text size="small" type="info">=</el-text>
          <el-input v-model="d.expr" size="small" style="flex:1;min-width:260px" spellcheck="false"
            placeholder="表达式: concat(region,'-',city) / upper(status) / 或聚合 SUM(amount) 按实体(近90天, 引擎自动 GROUP BY 实体)" />
          <el-select v-model="d.ins" size="small" style="width:150px" placeholder="插入字段" @change="insD(d)">
            <el-option v-for="f in allFields" :key="'i' + f" :value="f" :label="f" />
          </el-select>
          <el-button size="small" text type="danger" @click="derived.splice(di, 1)">✕</el-button>
        </div>
        <div class="flex" style="gap:8px;align-items:center">
          <el-button size="small" type="primary" plain @click="derived.push({ name: 'f' + (derived.length + 1), expr: '', ins: '' })">＋ 派生字段</el-button>
          <el-text size="small" type="info">例: pay7d = SUM(amount)(按实体近7天) · net = pay_amount - coupon_amount · 大区 = concat(region,'大区')</el-text>
        </div>
      </div>

      <!-- ═══ ② 规则集 ═══ -->
      <div v-if="step === 1" class="sec">
        <div class="sec-title">② 规则集(WHEN 条件 → THEN 打标, 参考 Drools)</div>
        <el-tabs v-model="ruleView">
          <el-tab-pane label="规则视图" name="list">
            <div v-for="(r, ri) in rules" :key="'r' + ri" class="rule-card">
              <div class="flex" style="gap:8px;flex-wrap:wrap;align-items:center">
                <el-tag size="small" type="warning" effect="plain">规则 {{ ri + 1 }}</el-tag>
                <el-input v-model="r.name" size="small" style="width:200px" placeholder="规则名, 如 金额-大额" />
                <el-text size="small" type="info">优先级</el-text>
                <el-input-number v-model="r.salience" size="small" style="width:80px" :min="0" />
                <el-checkbox v-model="r.exclusive" size="small">互斥(激活组: 命中本组最高优先级后不再匹配同组)</el-checkbox>
                <el-button size="small" text type="danger" style="margin-left:auto" @click="rules.splice(ri, 1)">删除规则</el-button>
              </div>

              <div class="when-box">
                <div class="flex" style="align-items:center;gap:6px;margin-bottom:4px">
                  <el-tag size="small" type="success" effect="plain">WHEN</el-tag>
                  <el-text size="small" type="info">当(条件组; 引用字段/派生字段)</el-text>
                </div>
                <div v-for="(g, gi) in r.groups" :key="gi" class="grp">
                  <div class="flex" style="gap:6px;align-items:center;margin-bottom:4px">
                    <el-tag size="small" type="info" effect="plain">组 {{ gi + 1 }}</el-tag>
                    <el-select v-model="g.logic" size="small" style="width:70px"><el-option value="AND" label="且" /><el-option value="OR" label="或" /></el-select>
                    <el-text size="small" type="info">组间: 全部满足</el-text>
                    <el-button size="small" text type="danger" @click="r.groups.splice(gi, 1)">删组</el-button>
                  </div>
                  <div v-for="(c, ci) in g.conds" :key="ci" class="flex" style="gap:6px;margin:4px 0;flex-wrap:wrap;align-items:center">
                    <el-select v-model="c.field" size="small" style="width:160px"><el-option v-for="f in allFields" :key="f" :value="f" :label="f" /></el-select>
                    <el-select v-model="c.op" size="small" style="width:90px"><el-option v-for="o in opers" :key="o.v" :value="o.v" :label="o.l" /></el-select>
                    <el-input v-model="c.val" size="small" style="width:130px" placeholder="值" />
                    <el-button size="small" text type="danger" @click="g.conds.splice(ci, 1)">✕</el-button>
                  </div>
                  <el-button size="small" text type="primary" @click="g.conds.push({ field: '', op: '==', val: '' })">＋ 条件</el-button>
                </div>
                <el-button size="small" text type="success" @click="r.groups.push({ logic: 'AND', conds: [{ field: '', op: '==', val: '' }] })">＋ 条件组</el-button>
              </div>

              <div class="then-box">
                <div class="flex" style="align-items:center;gap:6px;margin-bottom:4px">
                  <el-tag size="small" type="danger" effect="plain">THEN</el-tag>
                  <el-text size="small" type="info">动作(打标签 / 停止)</el-text>
                </div>
                <div v-for="(a, ai) in r.actions" :key="ai" class="flex" style="gap:6px;margin:4px 0;flex-wrap:wrap;align-items:center">
                  <el-select v-model="a.type" size="small" style="width:110px">
                    <el-option value="tag" label="打标签" />
                    <el-option value="stop" label="停止(不再匹配)" />
                  </el-select>
                  <template v-if="a.type === 'tag'">
                    <el-select v-model="a.tag" size="small" filterable style="width:200px" placeholder="标签(注册表)">
                      <el-option v-for="t in tagOptions" :key="t" :value="t" :label="t" />
                    </el-select>
                    <template v-if="!metaBool(a.tag)">
                      <el-text size="small" type="info">值</el-text>
                      <el-input v-model="a.val" size="small" style="width:120px" placeholder="如 大额/7.5" />
                    </template>
                    <el-text v-else size="small" type="info">(布尔标签: 命中即打)</el-text>
                    <el-text v-if="badTagVal(a)" size="small" type="danger">⚠ 不在注册域: {{ domainOf(a.tag) }}</el-text>
                  </template>
                  <el-button size="small" text type="danger" @click="r.actions.splice(ai, 1)">✕</el-button>
                </div>
                <el-button size="small" text type="primary" @click="r.actions.push({ type: 'tag', tag: '', val: '' })">＋ 动作</el-button>
              </div>
            </div>
            <div class="flex" style="gap:8px;margin-top:8px;flex-wrap:wrap">
              <el-button size="small" type="primary" plain @click="addRule">＋ 规则</el-button>
              <el-button size="small" @click="openDecision">⚙ 用决策表生成互斥分档规则(金额档示例)</el-button>
              <el-text size="small" type="info">互斥组 = 同一标签多档位时只命中优先级最高的一条</el-text>
            </div>
          </el-tab-pane>

          <el-tab-pane label="决策表视图(分档/映射)" name="dt">
            <el-alert type="info" :closable="false" style="margin-bottom:10px"
              title="决策表 = 批量生成互斥规则(同 Drools 决策表): 每一行 = 一个档位/条件组合; 生成后可在「规则视图」编辑" />
            <div class="flex" style="gap:8px;align-items:center;margin-bottom:8px;flex-wrap:wrap">
              <el-text size="small" type="info">按字段/派生字段分档 → 打标签</el-text>
              <el-select v-model="dt.field" size="small" style="width:160px"><el-option v-for="f in allFields" :key="f" :value="f" :label="f" /></el-select>
              <el-select v-model="dt.tag" size="small" style="width:200px"><el-option v-for="t in tagOptions" :key="t" :value="t" :label="t" /></el-select>
              <el-text size="small" type="info">(优先级: 行序越高越优先, 同组互斥)</el-text>
            </div>
            <el-table :data="dt.rows" size="small" border>
              <el-table-column label="序号" type="index" width="50" />
              <el-table-column label="条件">
                <template #default="{ row }">
                  <div class="flex" style="gap:6px;align-items:center">
                    <el-select v-model="row.op" size="small" style="width:70px"><el-option value=">=" label="≥" /><el-option value=">" label=">" /><el-option value="<" label="<" /><el-option value="==" label="=" /></el-select>
                    <el-input v-model="row.th" size="small" style="width:100px" placeholder="阈值/值" />
                  </div>
                </template>
              </el-table-column>
              <el-table-column label="命中 → 打标签值">
                <template #default="{ row }">
                  <el-input v-model="row.val" size="small" style="width:140px" placeholder="如 大额" />
                </template>
              </el-table-column>
              <el-table-column width="60">
                <template #default="{ $index }"><el-button size="small" text type="danger" @click="dt.rows.splice($index, 1)">✕</el-button></template>
              </el-table-column>
            </el-table>
            <div class="flex" style="gap:10px;margin-top:8px;align-items:center;flex-wrap:wrap">
              <el-button size="small" text type="primary" @click="dt.rows.push({ op: '>=', th: '', val: '' })">＋ 档位行</el-button>
              <el-text size="small" type="info">未命中默认值(该标签)</el-text>
              <el-input v-model="dt.elseVal" size="small" style="width:110px" placeholder="如 小额" />
              <el-button size="small" type="success" @click="genFromDt">生成规则(追加到规则视图)</el-button>
              <el-button size="small" plain @click="fillDtDemo">填入示例(金额档)</el-button>
            </div>
          </el-tab-pane>

          <el-tab-pane label="DRL 源码" name="drl">
            <el-alert type="info" :closable="false" style="margin-bottom:8px"
              title="平台将规则集编译为 DRL(Drools 规则语言); 高级用户可直接编辑(切手写后与可视化互斥)" />
            <div class="flex" style="gap:8px;margin-bottom:6px">
              <el-radio-group v-model="drlMode" size="small">
                <el-radio-button value="gen">生成预览</el-radio-button>
                <el-radio-button value="raw">手写 DRL</el-radio-button>
              </el-radio-group>
            </div>
            <pre class="sql-pre">{{ drlMode === 'gen' ? genDrl : rawDrl }}</pre>
          </el-tab-pane>
        </el-tabs>
      </div>

      <!-- ═══ ③ 试跑与命中分析 ═══ -->
      <div v-if="step === 2" class="sec">
        <div class="sec-title">③ 试跑与命中分析(引擎在样本上逐 Fact 跑规则集)</div>
        <div class="flex" style="gap:10px;align-items:center">
          <el-button type="primary" :loading="testing" @click="doTest">▶ {{ testLabel }}</el-button>
          <el-text size="small" type="info">{{ testHint }}</el-text>
        </div>
        <template v-if="tested">
          <el-table :data="hitRows" size="small" border stripe style="margin-top:10px">
            <el-table-column type="index" label="#" width="46" />
            <el-table-column label="规则" min-width="150">
              <template #default="{ row }"><el-text size="small">{{ row.rule }}</el-text></template>
            </el-table-column>
            <el-table-column label="条件摘要" min-width="220"><template #default="{ row }"><el-text size="small" type="info">{{ row.when }}</el-text></template></el-table-column>
            <el-table-column label="动作" min-width="200"><template #default="{ row }"><el-text size="small">{{ row.then }}</el-text></template></el-table-column>
            <el-table-column label="命中数" width="100"><template #default="{ row }"><el-text size="small" type="success">{{ row.hits }}</el-text></template></el-table-column>
            <el-table-column label="互斥截断" width="100"><template #default="{ row }"><el-text size="small" :type="row.blocked ? 'danger' : 'info'">{{ row.blocked ? '被截断' : '—' }}</el-text></template></el-table-column>
          </el-table>
          <div class="flex" style="gap:10px;margin-top:10px;align-items:center">
            <el-tag type="success" effect="plain">总计命中 12,540 实体 · 互斥生效 2 组</el-tag>
            <el-button type="success" @click="step = 3">继续: 调度与发布</el-button>
          </div>
        </template>
      </div>

      <!-- ═══ ④ 调度与发布 ═══ -->
      <div v-if="step === 3" class="sec">
        <div class="sec-title">④ 调度与发布</div>
        <el-form label-width="110px" style="max-width:640px">
          <el-form-item label="任务名称">
            <el-input v-model="taskName" style="width:340px" placeholder="任务名称(必填)" />
          </el-form-item>
          <el-form-item label="调度">
            <el-radio-group v-model="sched">
              <el-radio-button v-if="mode === 'batch'" value="cron">定时</el-radio-button>
              <el-radio-button value="manual">手动</el-radio-button>
              <el-radio-button v-if="mode !== 'batch'" value="resident">常驻(流/在线)</el-radio-button>
            </el-radio-group>
            <el-input v-if="sched === 'cron'" v-model="cron" size="small" style="width:220px;margin-left:10px" placeholder="0 0 2 * * ?" />
          </el-form-item>
          <el-form-item label="护栏">
            <el-checkbox v-model="guard">命中变化率 &gt; 30% 拦截</el-checkbox>
            <el-checkbox v-model="schemaCheck" style="margin-left:14px">标签类型/取值域校验失败即失败</el-checkbox>
          </el-form-item>
        </el-form>
        <div v-if="badChecks.length" class="check-bar">
          <el-text size="small" type="danger">校验 {{ badChecks.length }} 项:</el-text>
          <el-tag v-for="c in badChecks" :key="c" size="small" type="danger" effect="plain" style="margin-left:6px">{{ c }}</el-tag>
        </div>
        <el-button type="success" :disabled="!!badChecks.length" @click="toast('提交审批发布(mock)')">提交审批发布</el-button>
        <el-text size="small" type="info" style="margin-left:10px">发布后: 引擎装配 ds → 派生字段 → 规则集(每行 Fact 跑 Drools/Spark) → 命中打标 → 校验落 tag_fact</el-text>
      </div>

      <div class="nav-bar">
        <el-button :disabled="step === 0" @click="step--">上一步</el-button>
        <el-button v-if="step < 3" type="primary" @click="step++">下一步</el-button>
        <div style="flex:1"></div>
        <el-text size="small" type="info">第 {{ step + 1 }} / 4 步 · 打标规则引擎(参考 Drools, mock)</el-text>
      </div>
    </el-card>
  </div>
</template>

<script setup>
import { ref, computed } from 'vue'
import { ElMessage } from 'element-plus'

const toast = (m) => ElMessage.info(m)
const step = ref(0)
const taskName = ref('')
const mode = ref('batch')
const modeHint = computed(() => ({
  batch: '表快照事实 · 定时/手动 · 支持聚合派生 · 落事实层',
  stream: '事件流事实 + 窗口 · 常驻 · 结果近实时双写 事实+在线',
  online: '单事件事实 · 常驻 · 布尔/瞬时标签置位在线层(Redis TTL)'
}[mode.value]))
const st = ref({ topic: 'pay-events', eventTime: 'event_time', winType: 'sliding', winLen: 5, winUnit: 'm', srcType: 'topic' })
const testLabel = computed(() => (mode.value === 'batch' ? '▶ 试跑(1% 采样)' : mode.value === 'stream' ? '▶ 干跑(事件回放 10 分钟)' : '▶ 回放(最近 1 万事件)'))
const testHint = computed(() => (mode.value === 'batch'
  ? '事实 = 数据行(含派生字段); 规则按优先级/互斥逐条求值'
  : mode.value === 'stream'
    ? '事实 = 事件/窗口聚合结果; 同一规则集在 Flink 算子内逐条求值'
    : '事实 = 单事件; 规则即时求值置位, TTL 自动过期'))
const sched = ref('cron')
const cron = ref('0 0 2 * * ?')
const guard = ref(true)
const schemaCheck = ref(true)

const dsList = [
  { name: 'order_wide', rows: '412 万' },
  { name: 'customer_wide', rows: '36.2 万' }
]
const dsFields = {
  order_wide: ['order_id', 'customer_id', 'amount', 'coupon_amount', 'quantity', 'pay_amount', 'region', 'status', 'order_date'],
  customer_wide: ['customer_id', 'name', 'region', 'data_secret', 'status', 'last_active_at']
}
const pks = { order_wide: ['order_id'], customer_wide: ['customer_id'] }
const dateFields = computed(() => (dsFields[form.dataset] || []).filter(f => /date|time|_at/.test(f)))
const form = ref({ dataset: 'order_wide', entityKey: 'order_id', win: 'rel', winNum: 90, winOn: 'order_date' })
const pksOf = computed(() => pks[form.dataset] || ['id'])
function onDs() {
  form.entityKey = pks[form.dataset][0]
}

const derived = ref([
  { name: 'pay7d', expr: 'SUM(amount)  (近7天, 按实体聚合)', ins: '' },
  { name: 'net_amt', expr: 'pay_amount - coupon_amount', ins: '' },
  { name: 'region_lv', expr: "concat(region, '大区')", ins: '' }
])
function insD(d) {
  if (!d.ins) return
  d.expr = (d.expr ? d.expr + ' ' : '') + d.ins
  d.ins = ''
}
const allFields = computed(() => {
  const base = [...(dsFields[form.dataset] || [])]
  derived.value.forEach(d => { if (d.name && !base.includes(d.name)) base.push(d.name) })
  return base
})

const opers = [
  { v: '==', l: '=' }, { v: '!=', l: '≠' }, { v: '>=', l: '≥' }, { v: '<=', l: '≤' },
  { v: '>', l: '>' }, { v: '<', l: '<' }, { v: 'in', l: '∈' }, { v: 'like', l: '含' }, { v: 'null', l: '为空' }
]
const tagOptions = ['订单/金额档', '订单/归属地区', '客户分层/高价值', '风控/黑名单', '区域/华东', '行为/高频复购', '订单/净支付额']
const tagMeta = {
  '订单/金额档': { bool: false, domain: ['大额', '中额', '小额'] },
  '订单/归属地区': { bool: false, domain: ['华北', '华东', '华南', '西南', '华中', '东北', '西北'] },
  '订单/净支付额': { bool: false },
  '客户分层/高价值': { bool: true },
  '区域/华东': { bool: true },
  '风控/黑名单': { bool: true },
  '行为/高频复购': { bool: true }
}
const metaBool = (t) => !!(tagMeta[t] || {}).bool
const domainOf = (t) => (tagMeta[t] || {}).domain || []
const badTagVal = (a) => {
  const m = tagMeta[a.tag]
  return !!(m && m.domain && a.val && !m.domain.includes(a.val))
}

const emptyRule = () => ({
  name: '', salience: 10, exclusive: false,
  groups: [{ logic: 'AND', conds: [{ field: '', op: '==', val: '' }] }],
  actions: [{ type: 'tag', tag: '', val: '' }]
})
const rules = ref([
  {
    name: '金额-大额', salience: 30, exclusive: true,
    groups: [{ logic: 'AND', conds: [{ field: 'amount', op: '>=', val: '10000' }] }],
    actions: [{ type: 'tag', tag: '订单/金额档', val: '大额' }]
  },
  {
    name: '金额-中额', salience: 20, exclusive: true,
    groups: [{ logic: 'AND', conds: [{ field: 'amount', op: '>=', val: '1000' }] }],
    actions: [{ type: 'tag', tag: '订单/金额档', val: '中额' }]
  },
  {
    name: '黑名单命中', salience: 10, exclusive: false,
    groups: [{ logic: 'AND', conds: [{ field: 'status', op: '==', val: 'black' }] }],
    actions: [{ type: 'tag', tag: '风控/黑名单', val: '' }]
  }
])
function addRule() { rules.value.push(emptyRule()) }

const dt = ref({ field: 'amount', tag: '订单/金额档', elseVal: '小额', rows: [{ op: '>=', th: '10000', val: '大额' }, { op: '>=', th: '1000', val: '中额' }] })
function fillDtDemo() {
  dt.value = { field: 'amount', tag: '订单/金额档', elseVal: '小额', rows: [{ op: '>=', th: '10000', val: '大额' }, { op: '>=', th: '1000', val: '中额' }] }
}
function genFromDt() {
  dt.value.rows.forEach((row, i) => {
    if (!row.val || row.th === undefined || row.th === '') return
    rules.value.push({
      name: 'DT-' + dt.value.tag + '-' + (i + 1), salience: 100 - i * 10, exclusive: true,
      groups: [{ logic: 'AND', conds: [{ field: dt.value.field, op: row.op, val: String(row.th) }] }],
      actions: [{ type: 'tag', tag: dt.value.tag, val: row.val }]
    })
  })
  if (dt.value.elseVal) {
    const hasElse = rules.value.some(r => r.name === 'DT-' + dt.value.tag + '-else')
    if (!hasElse) {
      rules.value.push({
        name: 'DT-' + dt.value.tag + '-else', salience: 5, exclusive: true,
        groups: [{ logic: 'AND', conds: [] }],
        actions: [{ type: 'tag', tag: dt.value.tag, val: dt.value.elseVal }]
      })
    }
  }
  toast('已生成互斥规则并追加到「规则视图」')
}

const ruleView = ref('list')
const drlMode = ref('gen')
const rawDrl = ref(`// 手写 DRL(高级)\nrule "黑名单"\n  when\n    $o : Order(status == "black")\n  then\n    tag($o, "风控/黑名单");\nend`)
const genDrl = computed(() => {
  const L = ['// 由平台规则集生成(摘要)']
  rules.value.forEach((r, i) => {
    const conds = (r.groups || []).map(g => (g.conds || []).map(c => (c.field || '?') + ' ' + c.op + ' "' + (c.val || '') + '"').join(' AND ')).join(' OR ')
    const thens = (r.actions || []).map(a => a.type === 'stop' ? 'halt()' : 'tag(' + (a.tag || '?') + (a.val !== undefined && a.val !== '' && !metaBool(a.tag) ? ', "' + a.val + '"' : '') + ')').join('; ')
    L.push('rule "r' + (i + 1) + ' ' + (r.name || '') + '"' + (r.exclusive ? ' activation-group "x' + i + '" salience ' + r.salience : ' salience ' + r.salience))
    L.push('  when')
    L.push('    $f : Fact(' + (conds || 'true') + ')')
    L.push('  then')
    L.push('    ' + thens)
    L.push('end')
  })
  return L.join('\n')
})

const testing = ref(false)
const tested = ref(false)
const hitRows = ref([])
function doTest() {
  testing.value = true
  setTimeout(() => {
    testing.value = false
    tested.value = true
    hitRows.value = rules.value.filter(r => r.name).map((r, i) => ({
      rule: r.name,
      when: (r.groups[0]?.conds || []).map(c => c.field + ' ' + c.op + ' ' + c.val).join(' 且 ') || '恒真(默认档)',
      then: (r.actions || []).map(a => a.type === 'stop' ? '停止' : '打 ' + a.tag + (a.val ? '=' + a.val : '')).join('; '),
      hits: 4000 - i * 850,
      blocked: r.exclusive && i > 0
    }))
  }, 800)
}

const badChecks = computed(() => {
  const out = []
  if (!taskName.value.trim()) out.push('任务名称必填')
  if (!rules.value.length) out.push('至少一条规则')
  const derivedAgg = derived.value.filter(d => /SUM|COUNT|AVG|MAX|MIN/i.test(d.expr)).map(d => d.name)
  if (mode.value === 'online') {
    rules.value.forEach((r, i) => {
      ;(r.actions || []).forEach(a => {
        if (a.type === 'tag' && !metaBool(a.tag)) out.push('在线模式 规则' + (i + 1) + ': 只支持布尔/瞬时标签(' + (a.tag || '?') + ')')
      })
      ;(r.groups || []).forEach(g => (g.conds || []).forEach(c => {
        if (derivedAgg.includes(c.field)) out.push('在线模式 规则' + (i + 1) + ': 引用聚合派生 ' + c.field + ' 不可用, 请用批/流')
      }))
    })
  }
  if (mode.value === 'stream') {
    rules.value.forEach((r, i) => {
      ;(r.actions || []).forEach(a => {
        if (a.type === 'tag' && !metaBool(a.tag) && !a.val) out.push('流模式 规则' + (i + 1) + ': 值型标签需动作带值')
      })
    })
  }
  rules.value.forEach((r, i) => {
    const n = '规则' + (i + 1)
    if (!r.name) out.push(n + ': 缺名称')
    if (!(r.actions || []).length) out.push(n + ': 缺动作')
    ;(r.actions || []).forEach(a => {
      if (a.type === 'tag' && !a.tag) out.push(n + ': 动作未选标签')
      if (badTagVal(a)) out.push(n + ': 标签值不在注册域')
    })
  })
  return [...new Set(out)]
})
</script>

<style scoped>
.sec { min-height: 320px; }
.sec-title { font-weight: 600; font-size: 15px; margin-bottom: 12px; }
.flex { display: flex; }
.cfg-title { font-weight: 600; font-size: 13px; color: #2c3e50; margin: 10px 0 8px; }
.metric-row { display: flex; gap: 8px; align-items: center; margin: 6px 0; flex-wrap: wrap; background: #f8fafc; border-radius: 6px; padding: 6px 10px; }
.rule-card { border: 1px solid #e4e7ed; border-radius: 10px; padding: 12px; margin-bottom: 10px; }
.when-box { border-left: 3px solid #67c23a; background: #f6fdf6; padding: 8px 10px; margin-top: 8px; border-radius: 4px; }
.then-box { border-left: 3px solid #f56c6c; background: #fef7f7; padding: 8px 10px; margin-top: 8px; border-radius: 4px; }
.grp { border: 1px dashed #c2d9c2; border-radius: 4px; padding: 6px 8px; margin: 6px 0; }
.sql-pre { margin: 0; background: #0d1b2a; color: #9fdcb0; padding: 12px; font-size: 12px; line-height: 1.7; border-radius: 6px; overflow: auto; max-height: 340px; white-space: pre-wrap; word-break: break-all; }
.check-bar { background: #fef0f0; border-radius: 6px; padding: 6px 10px; margin: 8px 0; display: flex; align-items: center; flex-wrap: wrap; gap: 4px; }
.nav-bar { display: flex; gap: 10px; margin-top: 16px; align-items: center; }
</style>
