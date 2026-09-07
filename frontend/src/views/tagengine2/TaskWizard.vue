<template>
  <div class="page-card">
    <el-card shadow="never">
      <el-steps :active="step" align-center finish-status="success" style="margin-bottom:16px">
        <el-step title="数据集与事实" />
        <el-step title="绑定规则(规则/规则流)" />
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

        <div class="cfg-title">派生字段(事实预处理: 字段变换 / 聚合统计; 满足所绑规则的字段依赖)
          <el-text v-if="mode === 'stream'" size="small" type="warning" style="margin-left:8px">流模式: 聚合将用窗口实现</el-text>
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
          <el-text size="small" type="info">例: pay7d = SUM(amount)(近7天) · net = pay_amount - coupon_amount · 大区 = concat(region,'大区')</el-text>
        </div>
      </div>

      <!-- ═══ ② 绑定规则 ═══ -->
      <div v-if="step === 1" class="sec">
        <div class="sec-title">② 绑定规则 —— 规则与规则流在「规则库」分开维护(规则管理 / 规则流管理), 这里选择/新建; 执行序 = 规则先跑, 再按序跑规则流</div>

        <div class="bind-sec">
          <div class="cfg-title">A. 规则(先执行: 可视化/决策表/DRL 定义的完整规则, 各自独立判定; 决策表规则内部档位排他)</div>
          <div v-if="!bindRule.length" class="empty-tip">尚未绑定规则 —— 从规则管理选择, 或直接新建一个</div>
          <div v-for="(r, ri) in bindRule" :key="'br' + ri" class="asset-row">
            <div class="flex" style="gap:8px;align-items:center;flex-wrap:wrap;flex:1">
              <el-tag size="small" :type="defTagType(r)" effect="plain">规则·{{ defLabel(r) }}</el-tag>
              <el-text size="small" style="font-weight:600">{{ r.name }}</el-text>
              <el-tag size="small" effect="plain">v{{ r.ver }}</el-tag>
              <el-text size="small" type="info">{{ sumRule(r) }}</el-text>
            </div>
            <div class="flex" style="gap:4px;align-items:center">
              <el-tag v-for="d in depsOf(r)" :key="d" size="small" :type="depOk(d) ? 'success' : 'danger'" effect="plain">{{ d }} {{ depOk(d) ? '✓' : '缺' }}</el-tag>
              <el-button size="small" text :disabled="ri === 0" @click="mvArr(bindRule, ri, -1)">↑</el-button>
              <el-button size="small" text :disabled="ri === bindRule.length - 1" @click="mvArr(bindRule, ri, 1)">↓</el-button>
              <el-button size="small" text type="danger" @click="bindRule.splice(ri, 1)">✕</el-button>
            </div>
          </div>
          <div class="flex" style="gap:8px;margin-top:6px;flex-wrap:wrap">
            <el-button size="small" type="primary" plain @click="openPicker('rule')">从规则管理选择</el-button>
            <el-button size="small" plain @click="quickOpen = true">＋ 新建规则(落库后自动绑定)</el-button>
            <el-button size="small" text type="primary" @click="$router.push('/tagengine2/rule')">去规则库维护 →</el-button>
          </div>
        </div>

        <div class="bind-sec">
          <div class="cfg-title">B. 规则流(后执行, 按绑定顺序; 流内决策链可引用规则与规则流, 命中/未命中分支、可终止)</div>
          <div v-if="!bindFlow.length" class="empty-tip">尚未绑定规则流 —— 从规则流管理选择(如「风控优先决策链」), 或新建一个</div>
          <div v-for="(r, ri) in bindFlow" :key="'bf' + ri" class="asset-row">
            <div class="flex" style="gap:8px;align-items:center;flex-wrap:wrap;flex:1">
              <el-tag size="small" type="warning" effect="plain">规则流</el-tag>
              <el-text size="small" style="font-weight:600">{{ r.name }}</el-text>
              <el-tag size="small" effect="plain">v{{ r.ver }}</el-tag>
              <el-text size="small" type="info">{{ sumRule(r) }}</el-text>
            </div>
            <div class="flex" style="gap:4px;align-items:center">
              <el-tag v-for="d in depsOf(r)" :key="d" size="small" :type="depOk(d) ? 'success' : 'danger'" effect="plain">{{ d }} {{ depOk(d) ? '✓' : '缺' }}</el-tag>
              <el-button size="small" text :disabled="ri === 0" @click="mvArr(bindFlow, ri, -1)">↑</el-button>
              <el-button size="small" text :disabled="ri === bindFlow.length - 1" @click="mvArr(bindFlow, ri, 1)">↓</el-button>
              <el-button size="small" text type="danger" @click="bindFlow.splice(ri, 1)">✕</el-button>
            </div>
          </div>
          <div class="flex" style="gap:8px;margin-top:6px;flex-wrap:wrap">
            <el-button size="small" type="primary" plain @click="openPicker('flow')">从规则流管理选择</el-button>
            <el-button size="small" plain @click="flowQuickOpen = true">＋ 新建规则流(示例决策链)</el-button>
            <el-button size="small" text type="primary" @click="$router.push('/tagengine2/rule')">去规则库维护 →</el-button>
          </div>
        </div>

        <div v-if="missingDeps.length" class="dep-warn">
          <el-text size="small" type="danger"><b>依赖不满足 {{ missingDeps.length }} 个字段</b>: {{ missingDeps.join('、') }} —— 回到①在「派生字段」中定义(或更换数据集), 否则发布被拦截</el-text>
        </div>
        <el-text v-else size="small" type="success" style="margin-top:6px;display:block">✓ 所绑规则/规则流的字段依赖(含流内嵌套递归; DRL 规则除外)全部被 数据集字段 ∪ 派生字段 满足</el-text>
      </div>

      <!-- ═══ ③ 试跑与命中分析 ═══ -->
      <div v-if="step === 2" class="sec">
        <div class="sec-title">③ 试跑与命中分析(按执行序逐 Fact 求值: 规则 → 规则流[链内步骤])</div>
        <div class="flex" style="gap:10px;align-items:center">
          <el-button type="primary" :loading="testing" @click="doTest">▶ {{ testLabel }}</el-button>
          <el-text size="small" type="info">{{ testHint }}</el-text>
        </div>
        <template v-if="tested">
          <el-table :data="hitRows" size="small" border stripe style="margin-top:10px">
            <el-table-column type="index" label="序" width="46" />
            <el-table-column label="执行对象" min-width="190">
              <template #default="{ row }">
                <el-tag size="small" :type="row.kind === '规则流' ? 'warning' : 'primary'" effect="plain">{{ row.kind }}</el-tag>
                <el-text size="small" style="margin-left:4px">{{ row.rule }}</el-text>
              </template>
            </el-table-column>
            <el-table-column label="规则定义/流向摘要" min-width="280"><template #default="{ row }"><el-text size="small" type="info">{{ row.when }}</el-text></template></el-table-column>
            <el-table-column label="动作/去向" min-width="140"><template #default="{ row }"><el-text size="small">{{ row.then }}</el-text></template></el-table-column>
            <el-table-column label="命中数" width="90"><template #default="{ row }"><el-text size="small" type="success">{{ row.hits }}</el-text></template></el-table-column>
            <el-table-column label="截断/短路" width="110"><template #default="{ row }"><el-text size="small" :type="row.blocked ? 'danger' : 'info'">{{ row.blocked }}</el-text></template></el-table-column>
          </el-table>
          <div class="flex" style="gap:10px;margin-top:10px;align-items:center">
            <el-tag type="success" effect="plain">规则 {{ bindRule.length }} · 规则流 {{ bindFlow.length }} · 嵌套流 {{ nestedCnt }} 处</el-tag>
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
            <el-text size="small" type="info">发布时固化所绑规则/规则流版本快照({{ bindRule.length + bindFlow.length }} 条), 规则库后续升版不影响本任务, 除非重新发布</el-text>
          </el-form-item>
        </el-form>
        <div v-if="badChecks.length" class="check-bar">
          <el-text size="small" type="danger">校验 {{ badChecks.length }} 项:</el-text>
          <el-tag v-for="c in badChecks" :key="c" size="small" type="danger" effect="plain" style="margin-left:6px">{{ c }}</el-tag>
        </div>
        <el-button type="success" :disabled="!!badChecks.length" @click="publish">提交审批发布</el-button>
        <el-text size="small" type="info" style="margin-left:10px">发布后: 装配 ds → 派生字段 → 规则(可视化/决策表/DRL 编译执行) → 规则流(决策链) → 校验落 tag_fact</el-text>
      </div>

      <div class="nav-bar">
        <el-button :disabled="step === 0" @click="step--">上一步</el-button>
        <el-button v-if="step < 3" type="primary" @click="step++">下一步</el-button>
        <div style="flex:1"></div>
        <el-text size="small" type="info">第 {{ step + 1 }} / 4 步 · 规则管理(可视化/决策表/DRL) + 规则流管理 · 任务绑定执行(mock)</el-text>
      </div>
    </el-card>

    <!-- ═══ 从规则库选择 ═══ -->
    <el-dialog v-model="pickerOpen" :title="pickerKind === 'rule' ? '从规则管理选择规则(可多选)' : '从规则流管理选择规则流(可多选)'" width="760">
      <el-table :data="candidates" size="small" border @selection-change="sel = $event">
        <el-table-column type="selection" width="42" />
        <el-table-column label="名称" min-width="170">
          <template #default="{ row }">
            <el-text size="small">{{ row.name }} <el-tag size="small" :type="defTagType(row)" effect="plain">{{ defLabel(row) }}</el-tag> <el-tag size="small" effect="plain">v{{ row.ver }}</el-tag></el-text>
          </template>
        </el-table-column>
        <el-table-column label="定义摘要" min-width="380"><template #default="{ row }"><el-text size="small" type="info">{{ sumRule(row) }}</el-text></template></el-table-column>
      </el-table>
      <template #footer>
        <el-button @click="pickerOpen = false">取消</el-button>
        <el-button type="primary" @click="addPicked">添加选中({{ sel.length }})</el-button>
      </template>
    </el-dialog>

    <!-- ═══ 快速新建规则 ═══ -->
    <el-dialog v-model="quickOpen" title="新建规则(保存到规则管理并自动绑定)" width="600">
      <el-radio-group v-model="uq.mode" size="small" style="margin-bottom:10px">
        <el-radio-button value="visual">可视化(单条件)</el-radio-button>
        <el-radio-button value="table">决策表(档位)</el-radio-button>
      </el-radio-group>
      <el-alert type="info" :closable="false" title="快捷创建两种形态; 完整三视图编辑(含 DRL 定义)到「规则库 → 规则管理」。" style="margin-bottom:10px" />
      <el-form label-width="90px">
        <el-form-item label="名称"><el-input v-model="uq.name" style="width:300px" placeholder="如: 高价值客户判定" /></el-form-item>
        <template v-if="uq.mode === 'visual'">
          <el-form-item label="WHEN 条件">
            <div class="flex" style="gap:6px;flex-wrap:wrap">
              <el-select v-model="uq.field" style="width:150px"><el-option v-for="f in condFields" :key="f" :value="f" :label="f" /></el-select>
              <el-select v-model="uq.op" style="width:90px"><el-option v-for="o in opers" :key="o.v" :value="o.v" :label="o.l" /></el-select>
              <el-input v-model="uq.val" style="width:110px" placeholder="值" />
            </div>
          </el-form-item>
          <el-form-item label="THEN 打标签">
            <div class="flex" style="gap:6px;flex-wrap:wrap">
              <el-select v-model="uq.tag" style="width:180px" filterable placeholder="标签(注册表)">
                <el-option v-for="t in tagOptions" :key="t" :value="t" :label="t" />
              </el-select>
              <template v-if="!metaBool(uq.tag)">
                <el-input v-model="uq.val2" style="width:110px" placeholder="标签值" />
                <el-text v-if="badVal2" size="small" type="danger">域外</el-text>
              </template>
              <el-text v-else size="small" type="info">(布尔)</el-text>
            </div>
          </el-form-item>
        </template>
        <template v-else>
          <el-form-item label="决策表">
            <div class="flex" style="gap:6px;flex-wrap:wrap;align-items:center;margin-bottom:6px">
              <el-text size="small" type="info">字段</el-text>
              <el-select v-model="uq.field" size="small" style="width:140px"><el-option v-for="f in condFields" :key="f" :value="f" :label="f" /></el-select>
              <el-text size="small" type="info">标签</el-text>
              <el-select v-model="uq.tag" size="small" style="width:170px"><el-option v-for="t in tagOptions" :key="t" :value="t" :label="t" /></el-select>
            </div>
            <div v-for="(row, ri) in uq.buckets" :key="ri" class="flex" style="gap:6px;margin:4px 0;align-items:center">
              <el-select v-model="row.op" size="small" style="width:66px"><el-option value=">=" label="≥" /><el-option value="<" label="<" /></el-select>
              <el-input v-model="row.th" size="small" style="width:90px" placeholder="阈值" />
              <el-text size="small" type="info">→</el-text>
              <el-input v-model="row.val" size="small" style="width:100px" placeholder="档值" />
              <el-button size="small" text type="danger" @click="uq.buckets.splice(ri, 1)">✕</el-button>
            </div>
            <el-button size="small" text type="primary" @click="uq.buckets.push({ op: '>=', th: '', val: '' })">＋ 档位</el-button>
            <div class="flex" style="gap:6px;margin-top:6px;align-items:center">
              <el-text size="small" type="info">兜底</el-text>
              <el-input v-model="uq.elseVal" size="small" style="width:100px" placeholder="如 小额" />
            </div>
          </el-form-item>
        </template>
      </el-form>
      <template #footer>
        <el-button @click="quickOpen = false">取消</el-button>
        <el-button type="primary" :disabled="!uq.name.trim()" @click="saveQuick">保存并绑定</el-button>
      </template>
    </el-dialog>

    <!-- ═══ 快速新建规则流 ═══ -->
    <el-dialog v-model="flowQuickOpen" title="新建规则流(示例决策链, 保存到规则流管理并自动绑定)" width="520">
      <el-form label-width="90px">
        <el-form-item label="规则流名称"><el-input v-model="fq.name" style="width:300px" placeholder="如: 风控+金额档链" /></el-form-item>
        <el-alert type="info" :closable="false" title="生成两步示例: ① 黑名单命中 → 整链终止; ② 金额档(决策表规则)分档。建好后可去规则流管理继续编辑。" />
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
  ruleState, tagOptions, metaBool, domainOf, assetOf,
  defLabel, defTagType, sumRule, stepText, depNamesOf, upsertAsset
} from './mockLib.js'

const router = useRouter()
const toast = (m, t) => (t ? ElMessage[t](m) : ElMessage.info(m))
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
  ? '事实 = 数据行(含派生字段); 规则 → 规则流逐行求值'
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

/* ---------- ② 绑定 ---------- */
const bindRule = ref([])
const bindFlow = ref([])
function seedDefault() {
  if (bindRule.value.length || bindFlow.value.length) return
  const r2 = ruleState.rules.find(x => x.id === 'r2') || ruleState.rules.find(x => x.kind === 'rule')
  const f1 = ruleState.rules.find(x => x.id === 'f1') || ruleState.rules.find(x => x.kind === 'flow')
  if (r2) bindRule.value.push(JSON.parse(JSON.stringify(r2)))
  if (f1) bindFlow.value.push(JSON.parse(JSON.stringify(f1)))
}
seedDefault()
const mvArr = (arr, i, d) => {
  const j = i + d
  ;[arr[i], arr[j]] = [arr[j], arr[i]]
}
const depsOf = (r) => depNamesOf(r)
const availFields = computed(() => {
  const s = new Set(dsFields[form.dataset] || [])
  derived.value.forEach(d => { if (d.name) s.add(d.name) })
  return s
})
const depOk = (d) => availFields.value.has(d)
const missingDeps = computed(() => {
  const s = new Set()
  ;[...bindRule.value, ...bindFlow.value].forEach(r => depNamesOf(r).forEach(d => { if (!depOk(d)) s.add(d) }))
  return [...s]
})

/* 选择器 */
const pickerOpen = ref(false)
const pickerKind = ref('rule')
const sel = ref([])
const candidates = computed(() => {
  const bound = pickerKind.value === 'rule' ? bindRule.value : bindFlow.value
  return ruleState.rules.filter(r => (pickerKind.value === 'rule' ? r.kind === 'rule' : r.kind === 'flow') && r.status !== 0 && !bound.some(b => b.id === r.id))
})
const openPicker = (kind) => { pickerKind.value = kind; sel.value = []; pickerOpen.value = true }
function addPicked() {
  const arr = pickerKind.value === 'rule' ? bindRule.value : bindFlow.value
  sel.value.forEach(r => arr.push(JSON.parse(JSON.stringify(r))))
  toast('已绑定 ' + sel.value.length + ' 条' + (pickerKind.value === 'rule' ? '规则(A, 先执行)' : '规则流(B, 后执行)'))
  pickerOpen.value = false
}

/* 快速新建规则 */
const opers = [
  { v: '==', l: '=' }, { v: '!=', l: '≠' }, { v: '>=', l: '≥' }, { v: '<=', l: '≤' },
  { v: '>', l: '>' }, { v: '<', l: '<' }, { v: 'in', l: '∈' }, { v: 'like', l: '含' }
]
const quickOpen = ref(false)
const uq = ref({ mode: 'visual', name: '', field: 'amount', op: '>=', val: '', tag: '', val2: '', buckets: [{ op: '>=', th: '', val: '' }], elseVal: '' })
const condFields = computed(() => [...new Set([...dsFields[form.dataset], ...derived.value.map(d => d.name)])])
const badVal2 = computed(() => {
  const dom = domainOf(uq.value.tag)
  return !!(dom.length && uq.value.val2 && !dom.includes(uq.value.val2))
})
function saveQuick() {
  const asset = {
    name: uq.value.name, desc: '任务内快速新建', kind: 'rule', status: 1,
    def: {
      mode: uq.value.mode,
      visual: uq.value.mode === 'visual' ? { when: [{ logic: 'AND', conds: [{ field: uq.value.field, op: uq.value.op, val: uq.value.val }] }], actions: [{ type: 'tag', tag: uq.value.tag, val: metaBool(uq.value.tag) ? '' : uq.value.val2 }] } : { when: [], actions: [] },
      table: uq.value.mode === 'table' ? { field: uq.value.field, tag: uq.value.tag, rows: uq.value.buckets.filter(b => b.th !== '' && b.val), elseVal: uq.value.elseVal } : { field: 'amount', tag: '', rows: [], elseVal: '' },
      drl: ''
    }
  }
  const saved = upsertAsset(asset)
  bindRule.value.push(JSON.parse(JSON.stringify(saved)))
  quickOpen.value = false
  toast('规则已保存到规则管理并绑定(A): ' + saved.name)
}

/* 快速新建规则流 */
const flowQuickOpen = ref(false)
const fq = ref({ name: '' })
function saveQuickFlow() {
  const black = ruleState.rules.find(x => x.id === 'r2')
  const amt = ruleState.rules.find(x => x.id === 'r1')
  if (!black || !amt) { toast('规则库缺少示例规则(黑名单/金额档), 请先到规则管理补建', 'warning'); return }
  const fl = upsertAsset({
    name: fq.value.name, desc: '任务内快速新建(黑名单 → 金额档)', kind: 'flow', status: 1,
    steps: [
      { refId: 'r2', refType: 'rule', hit: 'stop', hitJump: 0, miss: 'next', missJump: 0 },
      { refId: 'r1', refType: 'rule', hit: 'next', hitJump: 0, miss: 'end', missJump: 0 }
    ]
  })
  bindFlow.value.push(JSON.parse(JSON.stringify(fl)))
  flowQuickOpen.value = false
  toast('规则流已保存到规则流管理并绑定(B): ' + fl.name)
}

/* ---------- ③ 试跑 ---------- */
const testing = ref(false)
const tested = ref(false)
const hitRows = ref([])
const nestedCnt = computed(() => {
  let n = 0
  bindFlow.value.forEach(f => (f.steps || []).forEach(s => { if (assetOf(s.refId)?.kind === 'flow') n++ }))
  return n
})
const execPlan = computed(() => {
  const out = []
  const pushRule = (r, prefix, depth) => {
    if (r.kind === 'flow') {
      ;(r.steps || []).forEach((s2, i) => {
        const inner = assetOf(s2.refId)
        if (inner && inner.kind === 'flow') {
          out.push({ kind: '规则流', rule: (prefix ? prefix + ' › ' : '') + r.name + ' · ' + (i + 1) + '. ⤷' + inner.name, when: '嵌套引用: ' + sumRule(inner), then: stepText(s2), hits: 2600, blocked: '—' })
          if (depth < 4) pushRule(inner, (prefix ? prefix + ' › ' : '') + r.name, depth + 1)
        } else {
          out.push({
            kind: '规则流', rule: (prefix ? prefix + ' › ' : '') + r.name + ' · ' + (i + 1) + '. ' + (inner ? inner.name : '?'),
            when: inner ? sumRule(inner) : '规则已删除?', then: stepText(s2), hits: 2600 - i * 300, blocked: '—'
          })
        }
      })
    } else {
      out.push({ kind: '规则', rule: r.name, when: sumRule(r), then: r.def.mode === 'table' ? '档位排他打标' : (r.def.mode === 'drl' ? 'DRL 执行' : '打标'), hits: 0, blocked: '—' })
    }
  }
  bindRule.value.forEach(r => pushRule(r, '', 0))
  bindFlow.value.forEach(r => {
    out.push({ kind: '规则流', rule: r.name + '(链入口)', when: (r.steps || []).length + ' 步', then: '进入链内', hits: 5000, blocked: '—' })
    pushRule(r, '', 1)
  })
  return out
})
function doTest() {
  if (!execPlan.value.length) { toast('先在②绑定至少一条规则或规则流', 'warning'); return }
  testing.value = true
  setTimeout(() => {
    testing.value = false
    tested.value = true
    hitRows.value = execPlan.value.map((p, i) => ({ ...p, hits: p.hits || Math.max(600, 4800 - i * 620) }))
  }, 800)
}

/* ---------- ④ 校验 ---------- */
const allActs = computed(() => {
  const out = []
  const add = (r, src) => {
    if (!r) return
    if (r.kind === 'flow') (r.steps || []).forEach(s => add(assetOf(s.refId), src + '/' + (assetOf(s.refId)?.name || '?')))
    else {
      const d = r.def || {}
      if (d.mode === 'visual') (d.visual?.actions || []).forEach(a => out.push({ src: src + '/' + r.name, a }))
      else if (d.mode === 'table') {
        const t = d.table || {}
        ;(t.rows || []).forEach(x => out.push({ src: src + '/' + r.name, tag: t.tag, val: x.val }))
        if (t.elseVal) out.push({ src: src + '/' + r.name, tag: t.tag, val: t.elseVal })
      }
      /* drl: 人工保证 */
    }
  }
  bindRule.value.forEach(r => add(r, '规则'))
  bindFlow.value.forEach(r => add(r, '规则流'))
  return out
})
const ruleChecks = computed(() => {
  const out = []
  const walk = (r) => {
    if (!r) return
    if (r.kind === 'flow') (r.steps || []).forEach(s => walk(assetOf(s.refId)))
    else if (r.def?.mode !== 'drl') out.push(r)
  }
  bindRule.value.forEach(walk)
  bindFlow.value.forEach(walk)
  return out
})
const badChecks = computed(() => {
  const out = []
  if (!taskName.value.trim()) out.push('任务名称必填')
  if (!bindRule.value.length && !bindFlow.value.length) out.push('至少绑定一条规则或规则流')
  missingDeps.value.forEach(d => out.push('缺依赖字段: ' + d + '(请在①派生字段定义或换数据集)'))
  ;[...bindRule.value, ...bindFlow.value].forEach(r => { if (r.status === 0) out.push('绑定了已停用资产: ' + r.name) })
  if (mode.value === 'online') {
    allActs.value.forEach(x => {
      const tag = x.a ? x.a.tag : x.tag
      if (tag && !metaBool(tag)) out.push('在线模式 ' + x.src + ': 只支持布尔/瞬时标签(' + tag + ')')
    })
    ruleChecks.value.forEach(r => {
      const d = r.def || {}
      const fields = d.mode === 'table' ? [d.table?.field] : (d.visual?.when || []).flatMap(g => (g.conds || []).map(c => c.field))
      fields.filter(Boolean).forEach(cf => { if (derivedAggNames.value.includes(cf)) out.push('在线模式 ' + r.name + ': 引用聚合派生 ' + cf + ' 不可用, 请用批/流') })
    })
  }
  if (mode.value === 'stream') {
    allActs.value.forEach(x => {
      if (x.a && x.a.type === 'tag' && !metaBool(x.a.tag) && !x.a.val) out.push('流模式 ' + x.src + ': 值型标签需动作带值')
    })
  }
  allActs.value.forEach(x => {
    const a = x.a || { tag: x.tag, val: x.val }
    if (!a.tag) return
    const dom = domainOf(a.tag)
    if (dom.length && a.val && !dom.includes(a.val)) out.push(x.src + ': 标签值不在注册域(' + dom.join('/') + ')')
  })
  return [...new Set(out)]
})
function publish() {
  toast('提交审批发布(mock) —— 固化规则/规则流版本快照, 装配后按序跑 规则 → 规则流')
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
