<template>
  <div class="page-card">
    <el-card shadow="never">
      <el-steps :active="step" align-center finish-status="success" style="margin-bottom:16px">
        <el-step title="数据集与事实" />
        <el-step title="绑定规则(单元/流)" />
        <el-step title="试跑与命中分析" />
        <el-step title="调度与发布" />
      </el-steps>

      <!-- ═══ ① 执行形态 + 数据集与事实 ═══ -->
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

        <div class="cfg-title">派生字段(事实预处理: 字段变换 / 聚合统计; 满足规则资产的字段依赖)
          <el-text v-if="mode === 'stream'" size="small" type="warning" style="margin-left:8px">流模式: 聚合将用窗口实现(近 N {{ st.winUnit === 'm' ? '分钟' : '小时' }})</el-text>
          <el-text v-else-if="mode === 'online'" size="small" type="warning" style="margin-left:8px">在线模式: 仅事件字段直取, 不支持聚合派生</el-text>
        </div>
        <div v-for="(d, di) in derived" :key="'d' + di" class="metric-row">
          <el-input v-model="d.name" size="small" style="width:140px" placeholder="字段名, 如 pay7d" />
          <el-text size="small" type="info">=</el-text>
          <el-input v-model="d.expr" size="small" style="flex:1;min-width:260px" spellcheck="false"
            placeholder="表达式: concat(region,'-',city) / upper(status) / 或聚合 SUM(amount) 按实体(近90天, 引擎自动 GROUP BY 实体)" />
          <el-select v-model="d.ins" size="small" style="width:150px" placeholder="插入字段" @change="insD(d)">
            <el-option v-for="f in dsFields[form.dataset]" :key="'i' + f" :value="f" :label="f" />
          </el-select>
          <el-button size="small" text type="danger" @click="derived.splice(di, 1)">✕</el-button>
        </div>
        <div class="flex" style="gap:8px;align-items:center">
          <el-button size="small" type="primary" plain @click="derived.push({ name: 'f' + (derived.length + 1), expr: '', ins: '' })">＋ 派生字段</el-button>
          <el-text size="small" type="info">例: pay7d = SUM(amount)(按实体近7天) · net = pay_amount - coupon_amount · 大区 = concat(region,'大区')</el-text>
        </div>
      </div>

      <!-- ═══ ② 绑定规则(单元/流) ═══ -->
      <div v-if="step === 1" class="sec">
        <div class="sec-title">② 绑定规则 —— 规则在「规则库」独立维护, 这里选择/新建; 执行序 = 平铺单元先跑, 再按序跑各流</div>

        <div class="bind-sec">
          <div class="cfg-title">A. 平铺单元(先执行; 单条直接打标, 互斥组内只命中最高优先级一条)</div>
          <div v-if="!bindUnits.length" class="empty-tip">尚未绑定单元 —— 从规则库选择, 或直接新建一个</div>
          <div v-for="(u, ui) in bindUnits" :key="'bu' + ui" class="asset-row">
            <div class="flex" style="gap:8px;align-items:center;flex-wrap:wrap;flex:1">
              <el-tag size="small" type="primary" effect="plain">单元</el-tag>
              <el-text size="small" style="font-weight:600">{{ u.name }}</el-text>
              <el-tag size="small" effect="plain">v{{ u.ver }}</el-tag>
              <el-tag v-if="u.activationGroup" size="small" type="warning" effect="plain">组:{{ u.activationGroup }}</el-tag>
              <el-text size="small" type="info">{{ whenSum(u) }} → {{ thenSum(u) }}</el-text>
            </div>
            <div class="flex" style="gap:4px;align-items:center">
              <el-tag v-for="d in depsOf(u)" :key="d" size="small" :type="depOk(d) ? 'success' : 'danger'" effect="plain">{{ d }} {{ depOk(d) ? '✓' : '缺' }}</el-tag>
              <el-button size="small" text :disabled="ui === 0" @click="mvArr(bindUnits, ui, -1)">↑</el-button>
              <el-button size="small" text :disabled="ui === bindUnits.length - 1" @click="mvArr(bindUnits, ui, 1)">↓</el-button>
              <el-button size="small" text type="danger" @click="bindUnits.splice(ui, 1)">✕</el-button>
            </div>
          </div>
          <div class="flex" style="gap:8px;margin-top:6px;flex-wrap:wrap">
            <el-button size="small" type="primary" plain @click="openPicker('unit')">从规则库选择单元</el-button>
            <el-button size="small" plain @click="unitQuickOpen = true">＋ 新建单元(落库后自动绑定)</el-button>
            <el-button size="small" text type="primary" @click="$router.push('/tagengine2/rule/unit')">去规则库维护单元 →</el-button>
          </div>
        </div>

        <div class="bind-sec">
          <div class="cfg-title">B. 规则流(后执行, 按绑定顺序跑; 流内决策链: 命中/未命中分支、可终止整链)</div>
          <div v-if="!bindFlows.length" class="empty-tip">尚未绑定规则流 —— 从规则库选择, 或新建一个(示例: 风控优先 + 金额档)</div>
          <div v-for="(fl, fi) in bindFlows" :key="'bf' + fi" class="asset-row">
            <div class="flex" style="gap:8px;align-items:center;flex-wrap:wrap;flex:1">
              <el-tag size="small" type="warning" effect="plain">流</el-tag>
              <el-text size="small" style="font-weight:600">{{ fl.name }}</el-text>
              <el-tag size="small" effect="plain">v{{ fl.ver }}</el-tag>
              <el-text size="small" type="info">{{ flowBrief(fl) }}</el-text>
            </div>
            <div class="flex" style="gap:4px;align-items:center">
              <el-tag v-for="d in flowDeps(fl)" :key="d" size="small" :type="depOk(d) ? 'success' : 'danger'" effect="plain">{{ d }} {{ depOk(d) ? '✓' : '缺' }}</el-tag>
              <el-button size="small" text :disabled="fi === 0" @click="mvArr(bindFlows, fi, -1)">↑</el-button>
              <el-button size="small" text :disabled="fi === bindFlows.length - 1" @click="mvArr(bindFlows, fi, 1)">↓</el-button>
              <el-button size="small" text type="danger" @click="bindFlows.splice(fi, 1)">✕</el-button>
            </div>
          </div>
          <div class="flex" style="gap:8px;margin-top:6px;flex-wrap:wrap">
            <el-button size="small" type="primary" plain @click="openPicker('flow')">从规则库选择流</el-button>
            <el-button size="small" plain @click="flowQuickOpen = true">＋ 新建流(示例决策链)</el-button>
            <el-button size="small" text type="primary" @click="$router.push('/tagengine2/rule/flow')">去规则库维护流 →</el-button>
          </div>
        </div>

        <div v-if="missingDeps.length" class="dep-warn">
          <el-text size="small" type="danger"><b>依赖不满足 {{ missingDeps.length }} 个字段</b>: {{ missingDeps.join('、') }} —— 回到①在「派生字段」中定义(或更换数据集), 否则发布被拦截</el-text>
        </div>
        <el-text v-else size="small" type="success" style="margin-top:6px;display:block">✓ 所选规则的字段依赖全部被 数据集字段 ∪ 派生字段 满足</el-text>
      </div>

      <!-- ═══ ③ 试跑与命中分析 ═══ -->
      <div v-if="step === 2" class="sec">
        <div class="sec-title">③ 试跑与命中分析(引擎按执行序逐 Fact 求值: 平铺单元 → 各流)</div>
        <div class="flex" style="gap:10px;align-items:center">
          <el-button type="primary" :loading="testing" @click="doTest">▶ {{ testLabel }}</el-button>
          <el-text size="small" type="info">{{ testHint }}</el-text>
        </div>
        <template v-if="tested">
          <el-table :data="hitRows" size="small" border stripe style="margin-top:10px">
            <el-table-column type="index" label="序" width="46" />
            <el-table-column label="执行对象" min-width="170">
              <template #default="{ row }">
                <el-tag size="small" :type="row.kind === '流' ? 'warning' : 'primary'" effect="plain">{{ row.kind }}</el-tag>
                <el-text size="small" style="margin-left:4px">{{ row.rule }}</el-text>
              </template>
            </el-table-column>
            <el-table-column label="条件/结构摘要" min-width="240"><template #default="{ row }"><el-text size="small" type="info">{{ row.when }}</el-text></template></el-table-column>
            <el-table-column label="动作/去向" min-width="200"><template #default="{ row }"><el-text size="small">{{ row.then }}</el-text></template></el-table-column>
            <el-table-column label="命中数" width="90"><template #default="{ row }"><el-text size="small" type="success">{{ row.hits }}</el-text></template></el-table-column>
            <el-table-column label="截断/短路" width="100"><template #default="{ row }"><el-text size="small" :type="row.blocked ? 'danger' : 'info'">{{ row.blocked }}</el-text></template></el-table-column>
          </el-table>
          <div class="flex" style="gap:10px;margin-top:10px;align-items:center">
            <el-tag type="success" effect="plain">平铺 {{ bindUnits.length }} 单元 · 流 {{ bindFlows.length }} 个 · 互斥生效 {{ exclCnt }} 组</el-tag>
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
          <el-form-item label="规则快照">
            <el-text size="small" type="info">发布时记录规则版本快照({{ snapshotTxt }}), 规则库后续升版不影响本任务, 除非重新发布</el-text>
          </el-form-item>
        </el-form>
        <div v-if="badChecks.length" class="check-bar">
          <el-text size="small" type="danger">校验 {{ badChecks.length }} 项:</el-text>
          <el-tag v-for="c in badChecks" :key="c" size="small" type="danger" effect="plain" style="margin-left:6px">{{ c }}</el-tag>
        </div>
        <el-button type="success" :disabled="!!badChecks.length" @click="publish">提交审批发布</el-button>
        <el-text size="small" type="info" style="margin-left:10px">发布后: 装配 ds → 派生字段 → 平铺单元(Drools 按优先级/互斥) → 各流(决策链) → 校验落 tag_fact</el-text>
      </div>

      <div class="nav-bar">
        <el-button :disabled="step === 0" @click="step--">上一步</el-button>
        <el-button v-if="step < 3" type="primary" @click="step++">下一步</el-button>
        <div style="flex:1"></div>
        <el-text size="small" type="info">第 {{ step + 1 }} / 4 步 · 规则库独立维护 · 任务绑定执行(mock)</el-text>
      </div>
    </el-card>

    <!-- ═══ 从规则库选择 ═══ -->
    <el-dialog v-model="pickerOpen" :title="pickerKind === 'unit' ? '从规则库选择单元(可多选)' : '从规则库选择流(可多选)'" width="720">
      <el-table :data="pickerKind === 'unit' ? unitCandidates : flowCandidates" size="small" border @selection-change="sel = $event">
        <el-table-column type="selection" width="42" />
        <template v-if="pickerKind === 'unit'">
          <el-table-column label="名称" min-width="150"><template #default="{ row }"><el-text size="small">{{ row.name }} <el-tag size="small" effect="plain">v{{ row.ver }}</el-tag></el-text></template></el-table-column>
          <el-table-column label="WHEN → THEN" min-width="320"><template #default="{ row }"><el-text size="small" type="info">{{ whenSum(row) }} → {{ thenSum(row) }}</el-text></template></el-table-column>
          <el-table-column label="互斥组" width="130"><template #default="{ row }"><el-tag v-if="row.activationGroup" size="small" type="warning" effect="plain">{{ row.activationGroup }}</el-tag><el-text v-else size="small" type="info">—</el-text></template></el-table-column>
        </template>
        <template v-else>
          <el-table-column label="名称" min-width="150"><template #default="{ row }"><el-text size="small">{{ row.name }} <el-tag size="small" effect="plain">v{{ row.ver }}</el-tag></el-text></template></el-table-column>
          <el-table-column label="决策链" min-width="380"><template #default="{ row }"><el-text size="small" type="info">{{ flowBrief(row) }}</el-text></template></el-table-column>
          <el-table-column label="步骤" width="70" align="center"><template #default="{ row }">{{ row.steps.length }}</template></el-table-column>
        </template>
      </el-table>
      <template #footer>
        <el-button @click="pickerOpen = false">取消</el-button>
        <el-button type="primary" @click="addPicked">添加选中({{ sel.length }})</el-button>
      </template>
    </el-dialog>

    <!-- ═══ 快速新建单元 ═══ -->
    <el-dialog v-model="unitQuickOpen" title="新建单元(保存到规则库并自动绑定)" width="560">
      <el-form label-width="90px">
        <el-form-item label="名称"><el-input v-model="uq.name" placeholder="如: 高价值客户" style="width:300px" /></el-form-item>
        <el-form-item label="WHEN 条件">
          <div class="flex" style="gap:6px;flex-wrap:wrap">
            <el-select v-model="uq.field" style="width:150px"><el-option v-for="f in condFields" :key="f" :value="f" :label="f" /></el-select>
            <el-select v-model="uq.op" style="width:90px"><el-option v-for="o in opers" :key="o.v" :value="o.v" :label="o.l" /></el-select>
            <el-input v-model="uq.val" style="width:120px" placeholder="值" />
            <el-checkbox v-model="uq.anyTime">恒真(兜底档)</el-checkbox>
          </div>
        </el-form-item>
        <el-form-item label="THEN 打标签">
          <div class="flex" style="gap:6px;flex-wrap:wrap">
            <el-select v-model="uq.tag" style="width:180px" filterable placeholder="标签(注册表)">
              <el-option v-for="t in tagOptions" :key="t" :value="t" :label="t" />
            </el-select>
            <template v-if="!metaBool(uq.tag)">
              <el-input v-model="uq.val2" style="width:120px" placeholder="标签值" />
              <el-text v-if="badVal2" size="small" type="danger">不在注册域</el-text>
            </template>
            <el-text v-else size="small" type="info">(布尔)</el-text>
          </div>
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="unitQuickOpen = false">取消</el-button>
        <el-button type="primary" :disabled="!uq.name.trim() || (!uq.anyTime && !uq.field)" @click="saveQuickUnit">保存并绑定</el-button>
      </template>
    </el-dialog>

    <!-- ═══ 快速新建流 ═══ -->
    <el-dialog v-model="flowQuickOpen" title="新建规则流(示例决策链, 保存到规则库并自动绑定)" width="520">
      <el-form label-width="90px">
        <el-form-item label="流名称"><el-input v-model="fq.name" placeholder="如: 客户风控+分层链" style="width:300px" /></el-form-item>
        <el-alert type="info" :closable="false" title="生成两步示例: ① 黑名单命中 → 整链终止; ② 金额档(连同互斥组)分档。建好后可去规则库继续编辑步骤。" />
      </el-form>
      <template #footer>
        <el-button @click="flowQuickOpen = false">取消</el-button>
        <el-button type="primary" :disabled="!fq.name.trim()" @click="saveQuickFlow">保存并绑定</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup>
import { ref, computed } from 'vue'
import { ElMessage } from 'element-plus'
import { useRouter } from 'vue-router'
import {
  ruleState, tagOptions, metaBool, domainOf,
  whenSum, thenSum, depsOf, flowDeps, flowBrief, stepSum, nodeUnits,
  addUnit, upsertFlow
} from './mockLib.js'

const router = useRouter()
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
  ? '事实 = 数据行(含派生字段); 平铺单元按优先级/互斥 → 各流决策链逐行求值'
  : mode.value === 'stream'
    ? '事实 = 事件/窗口聚合结果; 同一执行计划在 Flink 算子内求值'
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
const derivedAggNames = computed(() => derived.value.filter(d => /SUM|COUNT|AVG|MAX|MIN/i.test(d.expr)).map(d => d.name))

/* ---------- ② 绑定规则 ---------- */
const bindUnits = ref([])
const bindFlows = ref([])
const seedDefault = () => {
  if (!bindUnits.value.length && !bindFlows.value.length && ruleState.units.length) {
    const u = ruleState.units.find(x => x.id === 'u4') || ruleState.units[0]
    bindUnits.value.push(JSON.parse(JSON.stringify(u)))
    const f = ruleState.flows[0]
    if (f) bindFlows.value.push(JSON.parse(JSON.stringify(f)))
  }
}
seedDefault()

const mvArr = (arr, i, d) => {
  const j = i + d
  ;[arr[i], arr[j]] = [arr[j], arr[i]]
}

/* 字段依赖: 数据集字段 ∪ 派生字段 */
const availFields = computed(() => {
  const s = new Set(dsFields[form.dataset] || [])
  derived.value.forEach(d => { if (d.name) s.add(d.name) })
  return s
})
const depOk = (d) => availFields.value.has(d)
const missingDeps = computed(() => {
  const s = new Set()
  bindUnits.value.forEach(u => depsOf(u).forEach(d => { if (!depOk(d)) s.add(d) }))
  bindFlows.value.forEach(f => flowDeps(f).forEach(d => { if (!depOk(d)) s.add(d) }))
  return [...s]
})

/* 选择器 */
const pickerOpen = ref(false)
const pickerKind = ref('unit')
const sel = ref([])
const unitCandidates = computed(() => ruleState.units.filter(u => u.status !== 0 && !bindUnits.value.some(b => b.id === u.id)))
const flowCandidates = computed(() => ruleState.flows.filter(f => f.status !== 0 && !bindFlows.value.some(b => b.id === f.id)))
const openPicker = (kind) => { pickerKind.value = kind; sel.value = []; pickerOpen.value = true }
function addPicked() {
  if (pickerKind.value === 'unit') {
    sel.value.forEach(u => bindUnits.value.push(JSON.parse(JSON.stringify(u))))
    toast('已绑定 ' + sel.value.length + ' 个单元(平铺, 先执行)')
  } else {
    sel.value.forEach(f => bindFlows.value.push(JSON.parse(JSON.stringify(f))))
    toast('已绑定 ' + sel.value.length + ' 个流(后执行)')
  }
  pickerOpen.value = false
}

/* 快速新建单元 */
const opers = [
  { v: '==', l: '=' }, { v: '!=', l: '≠' }, { v: '>=', l: '≥' }, { v: '<=', l: '≤' },
  { v: '>', l: '>' }, { v: '<', l: '<' }, { v: 'in', l: '∈' }, { v: 'like', l: '含' }
]
const unitQuickOpen = ref(false)
const uq = ref({ name: '', field: 'amount', op: '>=', val: '', anyTime: false, tag: '', val2: '' })
const condFields = computed(() => [...new Set([...dsFields[form.dataset], ...derived.value.map(d => d.name)])])
const badVal2 = computed(() => {
  const dom = domainOf(uq.value.tag)
  return !!(dom.length && uq.value.val2 && !dom.includes(uq.value.val2))
})
function saveQuickUnit() {
  const u = addUnit({
    name: uq.value.name, desc: '任务内快速新建', salience: 10, activationGroup: '', status: 1,
    when: uq.value.anyTime ? [] : [{ logic: 'AND', conds: [{ field: uq.value.field, op: uq.value.op, val: uq.value.val }] }],
    actions: [{ type: 'tag', tag: uq.value.tag, val: metaBool(uq.value.tag) ? '' : uq.value.val2 }]
  })
  bindUnits.value.push(JSON.parse(JSON.stringify(u)))
  unitQuickOpen.value = false
  toast('单元已保存到规则库并绑定: ' + u.name)
}

/* 快速新建流 */
const flowQuickOpen = ref(false)
const fq = ref({ name: '' })
function saveQuickFlow() {
  const black = ruleState.units.find(u => u.id === 'u4')
  const amt = ruleState.units.find(u => u.id === 'u1')
  if (!black || !amt) { toast('规则库缺少示例单元(u4 黑名单 / u1 金额大额), 请先到规则库补建', 'warning'); return }
  const fl = upsertFlow({
    name: fq.value.name, desc: '任务内快速新建(风控优先 + 金额档)', status: 1,
    steps: [
      { unitId: 'u4', withGroup: false, hit: 'stop', hitJump: 0, miss: 'next', missJump: 0 },
      { unitId: 'u1', withGroup: true, hit: 'next', hitJump: 0, miss: 'end', missJump: 0 }
    ]
  })
  bindFlows.value.push(JSON.parse(JSON.stringify(fl)))
  flowQuickOpen.value = false
  toast('流已保存到规则库并绑定: ' + fl.name)
}

/* ---------- ③ 试跑 ---------- */
const testing = ref(false)
const tested = ref(false)
const hitRows = ref([])
const exclCnt = computed(() => {
  const s = new Set()
  bindUnits.value.forEach(u => { if (u.activationGroup) s.add(u.activationGroup) })
  bindFlows.value.forEach(f => (f.steps || []).forEach(s2 => {
    const u = ruleState.units.find(x => x.id === s2.unitId)
    if (s2.withGroup && u && u.activationGroup) s.add(u.activationGroup)
  }))
  return s.size
})
const execPlan = computed(() => {
  const out = []
  bindUnits.value.forEach(u => out.push({ kind: '单元', name: u.name, brief: whenSum(u) + ' → ' + thenSum(u), grp: u.activationGroup }))
  bindFlows.value.forEach(f => {
    out.push({ kind: '流', name: f.name + '(入口)', brief: (f.steps || []).length + ' 步决策链', grp: '', flow: true })
    ;(f.steps || []).forEach((s2, i) => {
      const node = nodeUnits(s2)
      const first = ruleState.units.find(x => x.id === s2.unitId)
      const label = (first ? first.name : '?') + (s2.withGroup && first && first.activationGroup ? '(整组:' + first.activationGroup + ')' : '')
      out.push({
        kind: '流节点', name: f.name + ' · ' + (i + 1) + '. ' + label,
        brief: (node.length > 1 ? '互斥组求值(最多命中 1 条)' : whenSum(node[0]) + ' → ' + thenSum(node[0])) + '; ' + stepSum(s2, i).split(']')[1] || '',
        grp: ''
      })
    })
  })
  return out
})
function doTest() {
  if (!execPlan.value.length) { toast('先在②绑定至少一个单元或流', 'warning'); return }
  testing.value = true
  setTimeout(() => {
    testing.value = false
    tested.value = true
    hitRows.value = execPlan.value.map((p, i) => ({
      kind: p.kind,
      rule: p.name,
      when: p.brief,
      then: p.kind === '流' ? '进入链内节点' : '打标',
      hits: Math.max(600, 5000 - i * 700),
      blocked: p.kind === '流节点' && /互斥组/.test(p.brief) ? '组内互斥' : (p.kind === '流' ? '—' : (i > 0 && p.grp ? '被同组更高档截断' : '—'))
    }))
  }, 800)
}

/* ---------- ④ 校验 ---------- */
const snapshotTxt = computed(() => {
  const u = bindUnits.value.length, f = bindFlows.value.length
  return u + ' 单元 · ' + f + ' 流 · v' + (Math.max(0, ...[...bindUnits.value.map(x => x.ver), ...bindFlows.value.map(x => x.ver)]))
})
const allActions = computed(() => {
  const acts = []
  bindUnits.value.forEach(u => (u.actions || []).forEach(a => acts.push({ src: u.name, a })))
  bindFlows.value.forEach(fl => (fl.steps || []).forEach(s2 => nodeUnits(s2).forEach(u => (u.actions || []).forEach(a => acts.push({ src: fl.name + '/' + u.name, a })))))
  return acts
})
const badChecks = computed(() => {
  const out = []
  if (!taskName.value.trim()) out.push('任务名称必填')
  if (!bindUnits.value.length && !bindFlows.value.length) out.push('至少绑定一个单元或规则流')
  missingDeps.value.forEach(d => out.push('缺依赖字段: ' + d + '(请在①派生字段定义或换数据集)'))
  bindUnits.value.forEach(u => { if (u.status === 0) out.push('绑定了已停用单元: ' + u.name) })
  bindFlows.value.forEach(fl => { if (fl.status === 0) out.push('绑定了已停用流: ' + fl.name) })
  if (mode.value === 'online') {
    allActions.value.forEach(({ src, a }) => {
      if (a.type === 'tag' && !metaBool(a.tag)) out.push('在线模式 ' + src + ': 只支持布尔/瞬时标签(' + (a.tag || '?') + ')')
    })
    bindUnits.value.forEach(u => (u.when || []).forEach(g => (g.conds || []).forEach(c => {
      if (derivedAggNames.value.includes(c.field)) out.push('在线模式 ' + u.name + ': 引用聚合派生 ' + c.field + ' 不可用, 请用批/流')
    })))
  }
  if (mode.value === 'stream') {
    allActions.value.forEach(({ src, a }) => {
      if (a.type === 'tag' && !metaBool(a.tag) && !a.val) out.push('流模式 ' + src + ': 值型标签需动作带值')
    })
  }
  allActions.value.forEach(({ src, a }) => {
    if (a.type === 'tag' && !a.tag) out.push(src + ': 动作未选标签')
    const dom = domainOf(a.tag)
    if (dom.length && a.val && !dom.includes(a.val)) out.push(src + ': 标签值不在注册域(' + dom.join('/') + ')')
  })
  return [...new Set(out)]
})
function publish() {
  toast('提交审批发布(mock) —— 将固化规则版本快照, 装配后按执行序跑平铺单元与各流')
  router.push('/tagengine2/task')
}
</script>

<style scoped>
.sec { min-height: 320px; }
.sec-title { font-weight: 600; font-size: 15px; margin-bottom: 12px; }
.flex { display: flex; }
.cfg-title { font-weight: 600; font-size: 13px; color: #2c3e50; margin: 10px 0 8px; }
.metric-row { display: flex; gap: 8px; align-items: center; margin: 6px 0; flex-wrap: wrap; background: #f8fafc; border-radius: 6px; padding: 6px 10px; }
.bind-sec { border: 1px solid #e4e7ed; border-radius: 10px; padding: 10px 14px; margin-bottom: 12px; }
.asset-row { display: flex; gap: 10px; align-items: center; justify-content: space-between; background: #f8fafc; border-radius: 6px; padding: 6px 10px; margin: 6px 0; flex-wrap: wrap; }
.empty-tip { border: 1px dashed #dcdfe6; color: #909399; border-radius: 6px; padding: 10px; font-size: 13px; }
.dep-warn { background: #fef0f0; border-radius: 6px; padding: 6px 10px; margin-top: 8px; }
.check-bar { background: #fef0f0; border-radius: 6px; padding: 6px 10px; margin: 8px 0; display: flex; align-items: center; flex-wrap: wrap; gap: 4px; }
.nav-bar { display: flex; gap: 10px; margin-top: 16px; align-items: center; }
</style>
