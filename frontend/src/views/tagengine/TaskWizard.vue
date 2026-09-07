<template>
  <div class="page-card">
    <el-card shadow="never">
      <el-steps :active="step" align-center finish-status="success" style="margin-bottom:24px">
        <el-step title="数据集与实体" />
        <el-step title="打标规则" />
        <el-step title="目标标签与调度" />
        <el-step title="试跑与发布" />
      </el-steps>

      <!-- ① 数据集 -->
      <div v-if="step === 0" class="sec">
        <div class="sec-title">① 选择数据集(模型)与实体键</div>
        <div class="ds-row" v-for="(ds, i) in datasets" :key="i">
          <el-select v-model="ds.model" placeholder="选择模型/逻辑表" style="width:240px" @change="onDsChange(i)">
            <el-option v-for="m in modelOptions" :key="m" :value="m" :label="m" />
          </el-select>
          <el-text size="small" type="info" style="margin:0 6px">实体键</el-text>
          <el-select v-model="ds.key" style="width:160px">
            <el-option v-for="k in keysOf(ds.model)" :key="k" :value="k" :label="k" />
          </el-select>
          <el-text v-if="i > 0" size="small" type="info" style="margin:0 6px">JOIN</el-text>
          <el-select v-if="i > 0" v-model="ds.joinOn" style="width:220px" placeholder="关联条件(mock)">
            <el-option v-for="c in joinCandidates(ds.model)" :key="c" :value="c" :label="c" />
          </el-select>
          <el-button v-if="datasets.length > 1" size="small" text type="danger" @click="datasets.splice(i, 1)">移除</el-button>
        </div>
        <el-button size="small" plain type="primary" style="margin-top:6px" @click="addDs">＋ 追加数据集(JOIN)</el-button>

        <div class="mt16">
          <el-form label-width="90px" style="max-width:640px">
            <el-form-item label="时间窗">
              <el-radio-group v-model="timeWin">
                <el-radio-button value="none">不限制</el-radio-button>
                <el-radio-button value="rel">相对当前</el-radio-button>
                <el-radio-button value="cal">固定日历窗</el-radio-button>
              </el-radio-group>
              <div v-if="timeWin !== 'none'" style="display:flex;gap:8px;margin-top:8px">
                <template v-if="timeWin === 'rel'">
                  <el-input-number v-model="winNum" :min="1" :max="3650" size="small" style="width:110px" />
                  <el-select v-model="winUnit" size="small" style="width:100px">
                    <el-option value="d" label="天" /><el-option value="m" label="月" />
                  </el-select>
                  <el-select v-model="winOn" size="small" style="width:180px" placeholder="按哪个时间字段">
                    <el-option value="order_date" label="order_date" /><el-option value="create_time" label="create_time" />
                  </el-select>
                </template>
                <template v-else>
                  <el-date-picker v-model="calRange" type="daterange" size="small" placeholder="上月/上季度(自然窗)" />
                </template>
              </div>
            </el-form-item>
          </el-form>
        </div>
      </div>

      <!-- ② 规则设计 -->
      <div v-if="step === 1" class="sec">
        <div class="sec-title">② 打标规则(可视化算子链 ↔ IR 实时预览)</div>
        <el-alert type="info" :closable="false" style="margin-bottom:10px"
          title="规则 = 算子链: 加工(聚合/窗口) → 条件 → 标签运算(排除/组合); 右侧为同一规则的 IR(JSON), 可切换 SQL 预览" />
        <div style="display:flex;align-items:center;gap:10px;margin-bottom:14px;flex-wrap:wrap">
          <span style="font-weight:600;font-size:13px">规则模式</span>
          <el-radio-group v-model="ruleMode">
            <el-radio-button value="visual">可视化构建(L1)</el-radio-button>
            <el-radio-button value="sql">SQL 声明(L2)</el-radio-button>
            <el-radio-button value="script">Spark 脚本(L4)</el-radio-button>
          </el-radio-group>
          <el-text size="small" type="info">L1-L4 统一任务模型: 调度 / 版本 / 试跑 / 护栏 / 血缘 / 审计 / 结果落 tag_fact</el-text>
        </div>

        <!-- L2 SQL -->
        <div v-if="ruleMode === 'sql'" style="display:flex;gap:16px;align-items:flex-start">
          <div style="flex:1.2;min-width:0">
            <el-text size="small" type="info" style="display:block;margin-bottom:6px">声明式 SQL(编译前会做静态校验: 无时间窗全表扫描 / 缺实体键 JOIN / 超时预算 → 拒绝)</el-text>
            <el-input v-model="sqlRuleText" type="textarea" :rows="12" style="font-family:monospace" />
          </div>
          <div class="ir-panel" style="flex:1">
            <div class="ir-head"><b>产物</b><el-text size="small" style="color:#7d8ea3">Spark SQL 模板 → 实体圈选</el-text></div>
            <pre class="ir-pre">{{ compiledSql }}</pre>
          </div>
        </div>

        <!-- L4 Spark 脚本 -->
        <div v-if="ruleMode === 'script'" style="display:flex;gap:16px;align-items:flex-start">
          <div style="flex:1.3;min-width:0">
            <div style="display:flex;gap:14px;align-items:center;margin-bottom:8px;flex-wrap:wrap">
              <span style="font-weight:600;font-size:13px">语言</span>
              <el-radio-group v-model="scriptLang">
                <el-radio-button value="pyspark">PySpark</el-radio-button>
                <el-radio-button value="scala">Scala / Java jar</el-radio-button>
              </el-radio-group>
              <span style="font-weight:600;font-size:13px;margin-left:10px">提交形态</span>
              <el-radio-group v-model="scriptForm">
                <el-radio-button value="inline">在线编辑器</el-radio-button>
                <el-radio-button value="jar">上传 jar</el-radio-button>
              </el-radio-group>
            </div>

            <template v-if="scriptForm === 'inline'">
              <div style="display:flex;gap:8px;margin-bottom:6px">
                <el-button size="small" @click="loadTpl">加载示例模板</el-button>
                <el-button size="small" type="warning" :loading="testing" @click="testRun">测试运行(试跑)</el-button>
                <el-text size="small" type="info">脚本随任务版本化, 可 diff / 回滚 / 审批</el-text>
              </div>
              <el-input v-model="scriptCode" type="textarea" :rows="14" spellcheck="false"
                style="font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:12px" />
            </template>

            <template v-else>
              <el-upload drag :auto-upload="false" :limit="1" style="max-width:420px" @change="toast('jar 已选择(mock)')">
                <div style="padding:18px">📦 选择/拖入作业 jar(含 mainClass 与依赖说明)</div>
              </el-upload>
              <div style="display:flex;gap:10px;margin-top:10px;flex-wrap:wrap">
                <el-input v-model="mainClass" size="small" style="width:320px" placeholder="mainClass: com.biz.tag.RFMSegment" />
                <el-input v-model="jarArgs" size="small" style="width:260px" placeholder="作业参数(平台注入 ctx 后追加)" />
              </div>
            </template>

            <el-alert type="warning" :closable="false" style="margin-top:10px"
              title="执行契约: 平台注入 ctx(数据集已注册/输出句柄/权限), 脚本不得自连库或自写结果; 输出 schema 必须为 (entity_key, value?) 由平台校验落 tag_fact" />
          </div>
          <div class="ir-panel" style="flex:1">
            <div class="ir-head"><b>输出契约与血缘声明</b></div>
            <div style="padding:10px;color:#c9d8ea;font-size:12px;line-height:1.8">
              <div>输出: <code style="color:#7ee787">df = ctx.emit(实体键列, value列)</code></div>
              <div style="margin:6px 0">读数据集(血缘):</div>
              <div><el-tag size="small" type="info" effect="plain" style="margin-right:4px">ds.order_wide</el-tag>
                <el-tag size="small" type="info" effect="plain" style="margin-right:4px">ds.customer_wide</el-tag>
                <el-tag size="small" type="info" effect="plain">tag_fact(黑名单)</el-tag></div>
              <div style="margin:6px 0">产出标签: <el-tag size="small" effect="plain" type="success">客户分层/高价值</el-tag></div>
              <div style="margin-top:6px">依赖: RFM 聚合中间表(任务 DAG)</div>
              <div style="margin-top:6px">资源: driver 2C/4G · 超时 30min · 失败自动重试 2 次</div>
            </div>
          </div>
        </div>

        <div v-if="ruleMode === 'visual'" class="rule-layout">
          <div class="rule-panel">
            <!-- 步骤列表 -->
            <div v-for="(st, i) in steps" :key="i" class="step-card">
              <div class="step-head">
                <el-tag size="small" :type="opTag(st.op)" effect="plain">{{ opLabel(st.op) }}</el-tag>
                <el-text size="small" type="info">步骤 {{ i + 1 }}</el-text>
                <el-button size="small" text type="danger" style="margin-left:auto" @click="steps.splice(i, 1)">删除</el-button>
              </div>

              <!-- 条件组 -->
              <template v-if="st.op === 'filter'">
                <div v-for="(g, gi) in st.groups" :key="gi" class="grp">
                  <div class="grp-head">
                    <el-tag size="small" type="info" effect="plain">条件组 {{ gi + 1 }}</el-tag>
                    <el-select v-model="g.logic" size="small" style="width:90px;margin:0 6px">
                      <el-option value="AND" label="且" /><el-option value="OR" label="或" />
                    </el-select>
                    <el-text size="small" type="info">组间 {{ stepsLogic }}</el-text>
                  </div>
                  <div v-for="(c, ci) in g.conds" :key="ci" class="cond">
                    <el-select v-model="c.field" size="small" style="width:170px" placeholder="字段">
                      <el-option v-for="f in mockFields" :key="f" :value="f" :label="f" />
                    </el-select>
                    <el-select v-model="c.op" size="small" style="width:110px">
                      <el-option v-for="o in opers" :key="o.v" :value="o.v" :label="o.l" />
                    </el-select>
                    <el-input v-model="c.value" size="small" style="width:130px" placeholder="值" />
                    <el-button size="small" text type="danger" @click="g.conds.splice(ci, 1)">✕</el-button>
                  </div>
                  <el-button size="small" text type="primary" @click="g.conds.push({ field: '', op: '>=', value: '' })">＋ 条件</el-button>
                </div>
                <el-button size="small" text type="success" @click="addGroup(st)">＋ 条件组(多组 = 复杂布尔)</el-button>
              </template>

              <!-- 聚合指标 -->
              <template v-if="st.op === 'aggregate'">
                <el-text size="small" type="info">按实体 {{ entitySummary }} 分组计算指标(写入中间列, 供后续条件引用):</el-text>
                <div v-for="(m, mi) in st.metrics" :key="mi" class="cond">
                  <el-select v-model="m.fn" size="small" style="width:130px">
                    <el-option v-for="o in aggFns" :key="o.v" :value="o.v" :label="o.l" />
                  </el-select>
                  <el-select v-model="m.on" size="small" style="width:150px"><el-option v-for="f in mockFields" :key="f" :value="f" :label="f" /></el-select>
                  <el-text size="small" type="info" style="margin:0 4px">→ 中间列</el-text>
                  <el-input v-model="m.as" size="small" style="width:140px" placeholder="如 amount_90d" />
                  <el-button size="small" text type="danger" @click="st.metrics.splice(mi, 1)">✕</el-button>
                </div>
                <el-button size="small" text type="primary" @click="st.metrics.push({ fn: 'sum', on: '', as: '' })">＋ 指标</el-button>
              </template>

              <!-- 标签运算 -->
              <template v-if="st.op === 'tagset'">
                <div class="cond">
                  <el-select v-model="st.tagOp" size="small" style="width:200px">
                    <el-option value="exclude" label="排除标签(¬)" />
                    <el-option value="union" label="并入标签(∪)" />
                    <el-option value="intersect" label="仅保留命中(∩)" />
                  </el-select>
                  <el-select v-model="st.tagPath" size="small" style="width:200px" placeholder="选择标签(依赖已有标签)">
                    <el-option value="风控/黑名单" label="风控/黑名单" />
                    <el-option value="客户分层/高价值" label="客户分层/高价值" />
                    <el-option value="区域/华东" label="区域/华东" />
                  </el-select>
                </div>
                <el-text size="small" type="info" style="display:block;margin-top:6px">级联: 本步骤读取 tag_fact 中其他任务已产出的标签 → 任务 DAG 自动建立依赖</el-text>
              </template>
            </div>
            <div style="display:flex;gap:8px;margin-top:10px;flex-wrap:wrap">
              <el-button size="small" @click="addStep('filter')">＋ 条件过滤</el-button>
              <el-button size="small" @click="addStep('aggregate')">＋ 聚合指标</el-button>
              <el-button size="small" @click="addStep('tagset')">＋ 标签运算(级联)</el-button>
              <el-select v-model="stepsLogic" size="small" style="width:110px;margin-left:auto">
                <el-option value="全部满足" label="全部满足(AND)" />
                <el-option value="任一满足" label="任一满足(OR)" />
              </el-select>
            </div>
          </div>

          <div class="ir-panel">
            <div class="ir-head">
              <b>IR 实时预览</b>
              <el-radio-group v-model="irView" size="small">
                <el-radio-button value="ir">IR(JSON)</el-radio-button>
                <el-radio-button value="sql">编译 SQL</el-radio-button>
              </el-radio-group>
            </div>
            <pre class="ir-pre">{{ irView === 'ir' ? irText : sqlText }}</pre>
          </div>
        </div>
      </div>

      <!-- ③ 目标标签与调度 -->
      <div v-if="step === 2" class="sec">
        <div class="sec-title">③ 目标标签与调度</div>
        <div style="display:flex;gap:24px">
          <div style="flex:1;min-width:280px">
            <el-text size="small" type="info" style="display:block;margin-bottom:8px">命中的实体打哪些标签(可多选, 支持树上任意层级 → 自动含子孙语义)</el-text>
            <el-tree :data="tagTree" show-checkbox node-key="id" default-expand-all
              :props="{ label: 'name', children: 'children' }" style="border:1px solid #e4e7ed;border-radius:8px;padding:10px;max-height:300px;overflow:auto" />
          </div>
          <div style="flex:1;min-width:280px">
            <el-form label-width="96px" style="max-width:420px">
              <el-form-item label="标签有效期(ttl)">
                <el-select v-model="ttl" style="width:150px"><el-option value="1d" label="1 天" /><el-option value="7d" label="7 天" /><el-option value="永久" label="永久" /></el-select>
              </el-form-item>
              <el-form-item label="冲突策略">
                <el-select v-model="conflict" style="width:200px">
                  <el-option value="overwrite" label="后发布覆盖" />
                  <el-option value="merge" label="保留更高优先级" />
                </el-select>
              </el-form-item>
              <el-form-item label="执行引擎">
                <el-radio-group v-model="engine">
                  <el-radio-button value="spark">Spark(批)</el-radio-button>
                  <el-radio-button value="sql">SQL-on-TiDB</el-radio-button>
                </el-radio-group>
              </el-form-item>
              <el-form-item label="调度">
                <el-radio-group v-model="schedType">
                  <el-radio-button value="manual">手动</el-radio-button>
                  <el-radio-button value="interval">定时</el-radio-button>
                  <el-radio-button value="event">事件触发</el-radio-button>
                </el-radio-group>
                <div v-if="schedType === 'interval'" style="margin-top:8px">
                  <el-input v-model="cron" size="small" style="width:260px" placeholder="0 0 2 * * ?" />
                </div>
                <div v-if="schedType === 'event'" style="margin-top:8px">
                  <el-select v-model="eventDep" size="small" style="width:260px" placeholder="依赖标签/任务完成后触发">
                    <el-option value="t1" label="RFM-高价值客户分层 完成后" />
                    <el-option value="t2" label="华东区域活跃客户圈选 完成后" />
                  </el-select>
                </div>
              </el-form-item>
            </el-form>
          </div>
        </div>
      </div>

      <!-- ④ 试跑与发布 -->
      <div v-if="step === 3" class="sec">
        <div class="sec-title">④ 试跑预览与发布</div>
        <el-descriptions :column="2" border size="small" style="max-width:760px">
          <el-descriptions-item label="任务名">{{ taskName }}</el-descriptions-item>
          <el-descriptions-item label="数据集">{{ datasets.map(d => d.model).join(' + ') }}</el-descriptions-item>
          <el-descriptions-item label="产出标签">{{ chosenTagNames }}</el-descriptions-item>
          <el-descriptions-item label="引擎 / 调度">{{ engine }} / {{ schedType === 'interval' ? cron : (schedType === 'event' ? '事件触发' : '手动') }}</el-descriptions-item>
        </el-descriptions>

        <div class="mt16">
          <el-button type="primary" :loading="previewing" @click="doPreview">▶ 试跑(1% 采样预览)</el-button>
          <el-button @click="toast('保存草稿(mock)')">存为草稿</el-button>
        </div>

        <template v-if="previewDone">
          <el-divider />
          <div class="pv-grid">
            <el-card shadow="never" class="pv-card">
              <div class="pv-num">{{ preview.hits.toLocaleString() }}</div>
              <div class="pv-label">预估命中实体(全量外推)</div>
            </el-card>
            <el-card shadow="never" class="pv-card">
              <div class="pv-num">{{ preview.coverage }}%</div>
              <div class="pv-label">覆盖率(占实体总数)</div>
            </el-card>
            <el-card shadow="never" class="pv-card">
              <div class="pv-num" :class="{ bad: preview.warn }">{{ preview.prevHits.toLocaleString() }}</div>
              <div class="pv-label">上版本命中(变化率 {{ preview.delta }}%)</div>
            </el-card>
          </div>
          <el-alert v-if="preview.warn" type="error" :closable="false" show-icon style="max-width:720px"
            title="命中数较上版本变化 +212%, 超过护栏阈值 30% —— 建议检查规则条件后再发布; 可开启护栏强制拦截" />
          <div class="mt16" style="display:flex;gap:16px;align-items:center">
            <el-checkbox v-model="guard">发布护栏: 命中变化率 > 30% 时拦截</el-checkbox>
            <el-button type="success" @click="toast('提交审批发布(mock)')">提交审批发布</el-button>
            <el-text size="small" type="info">发布走审批流(草稿→审批→上线), 重跑开新版本窗口, 支持回滚</el-text>
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
const taskName = ref('RFM-高价值客户分层(新)')
const modelOptions = ['customer_wide', 'order_wide', 'behavior_log_wide', 'risk_event_wide', 'user_model']
const datasets = ref([{ model: 'customer_wide', key: 'customer_id', joinOn: '' }])
const timeWin = ref('rel')
const winNum = ref(90)
const winUnit = ref('d')
const winOn = ref('order_date')
const calRange = ref(null)

const keysOf = () => ['customer_id', 'order_id', 'user_id', 'id_card_no']
const joinCandidates = () => ['customer_id = customer_id', 'order_id = order_id', 'user_id = user_id']
function onDsChange() { /* mock */ }
function addDs() { datasets.value.push({ model: 'order_wide', key: 'customer_id', joinOn: 'customer_id = customer_id' }) }
const entitySummary = computed(() => datasets.value.map(d => `${d.model}.${d.key}`).join(' / '))

const mockFields = ['amount', 'order_count', 'order_date', 'region', 'status', 'score', 'last_active_at', 'create_time']
const opers = [
  { v: '>=', l: '≥' }, { v: '<=', l: '≤' }, { v: '==', l: '=' }, { v: '!=', l: '≠' },
  { v: 'in', l: '∈ (列表)' }, { v: 'like', l: '含(正则)' }, { v: 'null', l: '为空' }, { v: 'notNull', l: '非空' }
]
const aggFns = [
  { v: 'sum', l: 'SUM 求和' }, { v: 'count', l: 'COUNT 计数' },
  { v: 'countDistinct', l: 'COUNT DISTINCT 去重' }, { v: 'max', l: 'MAX 最近' },
  { v: 'avg', l: 'AVG 平均' }
]
const stepsLogic = ref('全部满足')
const steps = ref([
  {
    op: 'aggregate', metrics: [
      { fn: 'sum', on: 'order_wide.amount', as: 'amount_90d' },
      { fn: 'countDistinct', on: 'order_wide.order_no', as: 'orders_90d' }
    ]
  },
  {
    op: 'filter', groups: [
      { logic: 'OR', conds: [{ field: 'amount_90d', op: '>=', value: '5000' }, { field: 'orders_90d', op: '>=', value: '3' }] }
    ]
  },
  { op: 'tagset', tagOp: 'exclude', tagPath: '风控/黑名单' }
])
const addStep = (op) => {
  if (op === 'filter') steps.value.push({ op, groups: [{ logic: 'AND', conds: [{ field: '', op: '==', value: '' }] }] })
  else if (op === 'aggregate') steps.value.push({ op, metrics: [{ fn: 'sum', on: '', as: '' }] })
  else steps.value.push({ op: 'tagset', tagOp: 'exclude', tagPath: '' })
}
const addGroup = (st) => st.groups.push({ logic: 'AND', conds: [{ field: '', op: '==', value: '' }] })
const opLabel = (op) => ({ filter: '条件过滤', aggregate: '聚合加工', tagset: '标签运算' }[op])
const opTag = (op) => ({ filter: 'warning', aggregate: 'success', tagset: 'primary' }[op])

// ── 规则模式: visual | sql | script(L4) ──
const ruleMode = ref('visual')
const sqlRuleText = ref(`-- L2 声明式: 密级映射为单个值型标签(枚举一次打完, 不拆 N 个任务)\nSELECT customer_id AS entity_key,\n       CASE data_secret WHEN 4 THEN '绝密' WHEN 3 THEN '机密'\n                        WHEN 2 THEN '内部' ELSE '公开' END AS tag_value\nFROM customer_wide\nWHERE data_secret IS NOT NULL\n-- 产出: 标签=数据密级, value=上表值 (emit 自动落 tag_fact)`)
const compiledSql = `-- 产物(Spark 执行):\n-- 圈选 customer_id + tag_value → emit\n-- 护栏: 无时间窗全表扫描警告 / 实体键完整性校验通过`
const scriptLang = ref('pyspark')
const scriptForm = ref('inline')
const mainClass = ref('')
const jarArgs = ref('')
const testing = ref(false)
const scriptCode = ref(`def run(ctx):\n    # ctx 由平台注入: 数据集已注册、输出句柄、权限; 脚本禁止自连库/自写结果\n    orders = ctx.table("ds.order_wide")          # 近 90 天订单(时间窗已下发)\n    cust = ctx.table("ds.customer_wide")\n\n    agg = (orders.groupBy("customer_id")\n           .agg(F.sum("amount").alias("amount_90d"),\n                F.countDistinct("order_no").alias("orders_90d")))\n\n    # 条件分支: 同一枚举域一次打完(高/中/一般), 而不是拆三个任务\n    result = (agg.join(cust, "customer_id")\n              .select("customer_id",\n                      F.when((F.col("amount_90d") >= 5000) | (F.col("orders_90d") >= 3), "高价值")\n                       .when(F.col("amount_90d") >= 1000, "中价值")\n                       .otherwise("一般").alias("tag_value")))\n\n    # 契约: 输出 (entity_key, value); 标签归属/版本/窗口由任务声明, 平台落 tag_fact\n    return ctx.emit(result)`)
function loadTpl() {
  scriptCode.value = `def run(ctx):\n    # 模型分 -> 值型标签(分数原样存, 阈值分箱可作为后续步骤)\n    scored = ctx.table("ds.customer_wide").select("customer_id", "churn_prob")\n    return ctx.emit(scored.select("customer_id", F.col("churn_prob").cast("double").alias("tag_value")))`
}
function testRun() {
  testing.value = true
  setTimeout(() => {
    testing.value = false
    toast('试跑完成(1% 采样): 预估命中 12.8 万实体, 输出 schema 校验通过, 未落正式事实')
  }, 900)
}

const irView = ref('ir')
const irText = `{
  "task": "RFM-高价值客户分层(新)",
  "dataset": {
    "models": [
      { "model": "customer_wide", "alias": "c", "entityKey": "customer_id" },
      { "model": "order_wide", "alias": "o", "join": "o.customer_id = c.customer_id" }
    ],
    "timeRange": { "window": "last_90d", "on": "o.order_date" }
  },
  "steps": [
    { "op": "aggregate", "by": "c.customer_id",
      "metrics": [
        { "name": "amount_90d", "expr": "sum(o.amount)" },
        { "name": "orders_90d", "expr": "count(distinct o.order_no)" }
      ] },
    { "op": "filter",
      "where": "(amount_90d >= 5000) OR (orders_90d >= 3)" },
    { "op": "excludeTag", "tag": "风控/黑名单" }
  ],
  "emit": [
    { "tag": "客户分层/高价值", "ttl": "1d", "overwrite": true },
    { "tag": "客户分层/中价值", "ttl": "1d", "overwrite": true }
  ],
  "engine": "spark", "schedule": "cron 0 0 2 * * ?"
}`
const sqlText = `-- 编译产物(SQL-on-TiDB 后端示例, Spark 后端同 IR)
WITH agg AS (
  SELECT o.customer_id,
         SUM(o.amount)          AS amount_90d,
         COUNT(DISTINCT o.order_no) AS orders_90d
  FROM order_wide o
  WHERE o.order_date >= NOW() - INTERVAL 90 DAY
  GROUP BY o.customer_id
), hit AS (
  SELECT c.customer_id
  FROM customer_wide c JOIN agg ON agg.customer_id = c.customer_id
  WHERE (agg.amount_90d >= 5000 OR agg.orders_90d >= 3)
    AND c.customer_id NOT IN (
      SELECT entity_key FROM tag_fact
      WHERE tag_id = (SELECT id FROM tag_dict WHERE name='黑名单') AND valid_to IS NULL
    )
)
SELECT customer_id AS entity_key,
       21 AS tag_id, NOW() AS valid_from FROM hit;`

const tagTree = ref([
  { id: 1, name: '区域', children: [{ id: 11, name: '华东' }, { id: 12, name: '华南' }, { id: 13, name: '华北' }] },
  {
    id: 2, name: '客户分层', children: [
      { id: 21, name: '高价值' }, { id: 22, name: '中价值' }, { id: 23, name: '沉睡' }
    ]
  },
  { id: 3, name: '行为', children: [{ id: 31, name: '高频复购' }, { id: 32, name: '首月评分' }] },
  { id: 4, name: '风控', children: [{ id: 41, name: '黑名单' }, { id: 42, name: '疑似套现' }] }
])
const chosenTagNames = '客户分层/高价值, 客户分层/中价值'
const ttl = ref('1d')
const conflict = ref('overwrite')
const engine = ref('spark')
const schedType = ref('interval')
const cron = ref('0 0 2 * * ?')
const eventDep = ref('t1')

const previewing = ref(false)
const previewDone = ref(false)
const guard = ref(true)
const preview = ref({ hits: 0, coverage: 0, prevHits: 0, delta: 0, warn: false })
function doPreview() {
  previewing.value = true
  setTimeout(() => {
    preview.value = { hits: 128540, coverage: 34.2, prevHits: 41203, delta: '+212%', warn: true }
    previewing.value = false
    previewDone.value = true
  }, 800)
}
</script>

<style scoped>
.sec { min-height: 340px; }
.sec-title { font-weight: 600; font-size: 15px; margin-bottom: 14px; }
.mt16 { margin-top: 16px; }
.ds-row { display: flex; align-items: center; margin-bottom: 8px; flex-wrap: wrap; gap: 4px; }
.rule-layout { display: flex; gap: 16px; align-items: flex-start; }
.rule-panel { flex: 1.2; min-width: 0; }
.ir-panel { flex: 1; min-width: 0; border: 1px solid #e4e7ed; border-radius: 8px; background: #0d1b2a; }
.ir-head { display: flex; justify-content: space-between; align-items: center; padding: 8px 12px; color: #d7e3f4; border-bottom: 1px solid #22344a; }
.ir-pre { margin: 0; padding: 12px; font-size: 12px; line-height: 1.6; color: #9fdcb0; overflow: auto; max-height: 460px; white-space: pre-wrap; word-break: break-all; }
.step-card { border: 1px solid #e4e7ed; border-radius: 8px; padding: 10px 12px; margin-bottom: 10px; }
.step-head { display: flex; align-items: center; gap: 8px; margin-bottom: 8px; }
.grp { border-left: 3px solid #409eff; background: #f7faff; padding: 6px 10px; border-radius: 4px; margin-bottom: 8px; }
.grp-head { display: flex; align-items: center; margin-bottom: 6px; }
.cond { display: flex; align-items: center; gap: 6px; margin: 4px 0; flex-wrap: wrap; }
.pv-grid { display: flex; gap: 16px; margin-bottom: 12px; flex-wrap: wrap; }
.pv-card { width: 220px; text-align: center; }
.pv-num { font-size: 24px; font-weight: 700; color: #409eff; }
.pv-num.bad { color: #f56c6c; }
.pv-label { font-size: 12px; color: #8f959e; margin-top: 4px; }
.nav-bar { display: flex; gap: 10px; margin-top: 18px; align-items: center; }
</style>
