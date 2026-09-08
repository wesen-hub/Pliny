<template>
  <div class="page-card">
    <el-card shadow="never">
      <div class="flex-between">
        <div>
          <div class="pg-title">数据订阅中心<el-tag size="small" effect="plain" style="margin-left:8px">设计稿 v0.2 · 演示数据</el-tag></div>
          <el-text size="small" type="info">
            对外数据供给。选数 = <b>数据视图构造器</b>(逻辑表维 ∪ 标签维, 可组合回表); 交付 = <b>PULL 数据服务 API</b> / <b>PUSH Kafka·Webhook(验签)</b> × 定时/实时/批量/流式; 全生命周期与审计。
          </el-text>
        </div>
        <div class="flex" style="gap:8px">
          <el-button size="small" type="primary" @click="tab='create'">＋ 新建订阅</el-button>
        </div>
      </div>

      <el-tabs v-model="tab" style="margin-top:8px">
        <!-- ① 契约 -->
        <el-tab-pane label="① 订阅契约" name="contracts">
          <el-table :data="contracts" size="small" border>
            <el-table-column label="契约" min-width="150">
              <template #default="{ row }"><div style="font-weight:600">{{ row.name }}</div><el-text size="small" type="info">#{{ row.id }} · {{ row.app }}</el-text></template>
            </el-table-column>
            <el-table-column label="数据视图" min-width="240">
              <template #default="{ row }">
                <template v-if="row.vtype === 'logical'">
                  <el-tag size="small" type="primary" effect="plain">{{ row.tables.join('+') }}</el-tag>
                  <el-tag v-if="row.merged" size="small" effect="plain">同构合并</el-tag>
                  <el-text size="small" type="info" class="dim2"> 列{{ row.cols }} 条件{{ row.cond ? '🔒' : '无' }}</el-text>
                </template>
                <template v-else>
                  <el-tag v-for="t in row.tags" :key="t" size="small" type="warning" effect="plain" style="margin:1px">{{ t }}</el-tag>
                  <el-text size="small" type="info" class="dim2">{{ row.tagOp }} {{ row.exportShape === 'wide' ? '· 回表 ' + (row.joinTable||'') : row.exportShape === 'matrix' ? '· 标签矩阵' : '· 实体键' }}</el-text>
                </template>
              </template>
            </el-table-column>
            <el-table-column label="交付" width="150">
              <template #default="{ row }">
                <el-tag size="small" :type="row.push === 'push' ? 'danger' : 'primary'" effect="plain">{{ row.push.toUpperCase() }}</el-tag>
                <el-text size="small" class="dim2" style="margin-left:4px">{{ row.targetType || '' }} · {{ row.freq }}</el-text>
              </template>
            </el-table-column>
            <el-table-column label="状态" width="100">
              <template #default="{ row }"><el-tag size="small" :type="stType(row.status)" effect="plain">{{ row.status }}</el-tag></template>
            </el-table-column>
            <el-table-column label="交付(7日)" width="120">
              <template #default="{ row }"><div>{{ row.rows7d }} 行</div><el-text size="small" type="info" class="dim2">{{ row.lastAt }}</el-text></template>
            </el-table-column>
            <el-table-column label="操作" width="170">
              <template #default="{ row }">
                <el-button size="small" text type="primary" @click="openDetail(row)">详情/协议</el-button>
                <el-button v-if="row.status === '待审批'" size="small" text type="warning" @click="tab='approve'">审批</el-button>
              </template>
            </el-table-column>
          </el-table>
        </el-tab-pane>

        <!-- ② 新建: 数据视图构造器 -->
        <el-tab-pane label="② 新建订阅" name="create">
          <el-steps :active="step" align-center finish-status="success" style="max-width:900px;margin:0 auto 14px">
            <el-step title="构造数据视图" /><el-step title="交付节奏/方式" /><el-step title="目标与协议" /><el-step title="密级/有效期" /><el-step title="确认提交" />
          </el-steps>

          <!-- 步骤1: 视图构造器 -->
          <div v-if="step === 0" style="max-width:900px;margin:auto">
            <el-radio-group v-model="w.viewType" size="small">
              <el-radio-button label="logical">逻辑表维度</el-radio-button>
              <el-radio-button label="tag">标签维度</el-radio-button>
            </el-radio-group>
            <el-switch v-model="w.compose" size="small" style="margin-left:16px" active-text="组合: 标签圈选→回表字段" inactive-text="单一维度" />

            <!-- 逻辑表面板 -->
            <div v-if="w.viewType === 'logical'" class="panel">
              <div class="sec-title">① 逻辑表(多选)</div>
              <div class="chips">
                <el-check-tag v-for="t in tables" :key="t.name" :checked="w.tables.includes(t.name)" @change="toggleTable(t.name)">
                  {{ t.name }}<span class="dim2"> · {{ t.biz }}</span>
                </el-check-tag>
              </div>
              <el-radio-group v-if="w.tables.length > 1" v-model="w.mergeMode" size="small" style="margin:6px 0">
                <el-radio label="merge">同构合并(行带 source_table)</el-radio>
                <el-radio label="separate">分别契约(各建一条)</el-radio>
              </el-radio-group>

              <div class="sec-title">② 交付字段({{ w.cols.length }})</div>
              <div class="chips">
                <el-check-tag v-for="c in allCols" :key="c" :checked="w.cols.includes(c)" @change="toggleCol(c)">{{ c }}</el-check-tag>
              </div>

              <div class="sec-title">③ 行条件(条件组构造器)</div>
              <div v-for="(g, gi) in w.condGroups" :key="gi" class="cond-group">
                <el-tag v-if="gi > 0" size="small" effect="plain">{{ w.condGroups[gi - 1].logic === 'OR' ? 'OR' : 'AND' }}</el-tag>
                <div v-for="(c, ci) in g.conds" :key="ci" class="cond-line">
                  <el-select v-model="c.field" size="small" style="width:140px"><el-option v-for="f in allCols" :key="f" :label="f" :value="f" /></el-select>
                  <el-select v-model="c.op" size="small" style="width:90px"><el-option v-for="o in ['=', '!=', '>', '>=', '<', '<=', 'in', 'like', 'null']" :key="o" :label="o" :value="o" /></el-select>
                  <el-input v-model="c.val" size="small" style="width:150px" placeholder="值 / 逗号列表" />
                  <el-button v-if="g.conds.length > 1" size="small" text type="danger" @click="g.conds.splice(ci, 1)">删</el-button>
                </div>
                <div class="flex" style="gap:6px">
                  <el-button size="small" text @click="g.conds.push({ field: allCols[0], op: '=', val: '' })">＋ 条件</el-button>
                  <el-tag size="small">组间 {{ gi === 0 ? '—' : (w.condGroups[gi - 1].logic) }}</el-tag>
                </div>
              </div>
              <div class="flex" style="gap:6px;margin-top:4px">
                <el-button size="small" @click="w.condGroups.push({ conds: [{ field: allCols[0], op: '=', val: '' }] })">＋ 条件组</el-button>
                <el-text size="small" type="info">组内 AND; 组间关系由上一组 logic 决定(AND/OR)</el-text>
                <el-checkbox v-model="w.condTag" size="small">引用标签圈选(仅…) </el-checkbox>
                <el-select v-if="w.condTag" v-model="w.condTagVal" size="small" style="width:200px">
                  <el-option label="业务域/风控/黑名单" value="业务域/风控/黑名单" /><el-option label="业务域/交易/订单/金额档=大额" value="金额档大额" />
                </el-select>
              </div>
            </div>

            <!-- 标签面板 -->
            <div v-else class="panel">
              <div class="sec-title">① 标签圈选(多选 × 关系)</div>
              <div class="chips">
                <el-check-tag v-for="t in tagOpts" :key="t.path" :checked="w.tags.includes(t.path)" @change="toggleTag(t.path)">{{ t.path }}</el-check-tag>
              </div>
              <div class="flex" style="gap:12px;margin:8px 0">
                <el-radio-group v-model="w.tagOp" size="small">
                  <el-radio label="AND">全部命中</el-radio>
                  <el-radio label="OR">任一命中</el-radio>
                </el-radio-group>
                <el-text size="small" type="info">exclude: <el-select v-model="w.exclude" clearable size="small" style="width:200px"><el-option label="业务域/交易(排除)" value="业务域/交易" /></el-select></el-text>
              </div>
              <div class="sec-title">② 输出形态</div>
              <el-radio-group v-model="w.exportShape" size="small">
                <el-radio-button label="entity">仅实体键</el-radio-button>
                <el-radio-button label="matrix">实体 + 标签值矩阵</el-radio-button>
                <el-radio-button label="wide">人群宽表(回表字段)</el-radio-button>
              </el-radio-group>
              <div v-if="w.exportShape === 'wide'" class="flex" style="gap:8px;margin-top:8px">
                <el-text size="small">回表: </el-text>
                <el-select v-model="w.joinTable" size="small" style="width:200px"><el-option v-for="t in tables" :key="t.name" :label="t.name" :value="t.name" /></el-select>
                <el-text size="small">字段</el-text>
                <el-check-tag v-for="c in allCols" :key="c" :checked="w.joinCols.includes(c)" @change="toggleJoin(c)">{{ c }}</el-check-tag>
                <el-text size="small" type="info" class="dim2">(按实体键 join; 无匹配实体保留实体+空字段)</el-text>
              </div>
              <div v-if="w.exportShape === 'matrix'" class="dim2" style="margin-top:6px">输出: 每个实体一行, 列 = 所选标签的当前值(未命中=∅)</div>
            </div>

            <!-- 预览 -->
            <el-card shadow="never" class="sub" style="margin-top:10px">
              <template #header><b>视图预览(schema)</b></template>
              <div class="flex" style="gap:6px;flex-wrap:wrap">
                <el-tag v-for="c in previewCols" :key="c" size="small" effect="plain">{{ c }}</el-tag>
              </div>
              <div class="dim2" style="margin-top:6px">主键: entity_key(标签维) / 逻辑表主键 · 增量键: _seq / 变更游标 · 预估行数: ~{{ 120 + w.cols.length * 30 + (w.condGroups.length > 1 ? 40 : 0) }} 万(演示)</div>
            </el-card>
            <div style="text-align:center;margin-top:10px"><el-button type="primary" @click="step = 1">下一步: 交付方式</el-button></div>
          </div>

          <!-- 步骤2: 交付 -->
          <div v-if="step === 1" style="max-width:760px;margin:auto">
            <el-form label-width="110px">
              <el-form-item label="交付方式">
                <el-radio-group v-model="w.push">
                  <el-radio-button label="pull"><b>PULL 拉取</b><div class="dim2">订阅方调数据服务 API 取数(快照/增量游标/导出)</div></el-radio-button>
                  <el-radio-button label="push"><b>PUSH 推送</b><div class="dim2">平台主动推送(Kafka / Webhook)</div></el-radio-button>
                </el-radio-group>
              </el-form-item>
              <el-form-item label="获取节奏">
                <el-radio-group v-model="w.freq">
                  <el-radio-button label="定时">定时(每日/每小时)</el-radio-button>
                  <el-radio-button label="实时">实时</el-radio-button>
                  <el-radio-button label="批量">批量全量</el-radio-button>
                  <el-radio-button label="流式">流式增量</el-radio-button>
                </el-radio-group>
              </el-form-item>
              <div class="hint">
                逻辑表增量 = 平台 _seq 游标; 标签视图增量 = 标签变更游标(实体打标/去标事件)。
                PULL 快照/增量走 API; PUSH 实时/流式推事件, 定时推汇总批。
              </div>
            </el-form>
            <div style="text-align:center;display:flex;gap:8px;justify-content:center">
              <el-button @click="step = 0">上一步</el-button><el-button type="primary" @click="step = 2">下一步: 目标与协议</el-button>
            </div>
          </div>

          <!-- 步骤3: 目标与协议 -->
          <div v-if="step === 2" style="max-width:860px;margin:auto">
            <template v-if="w.push === 'push'">
              <el-form label-width="110px">
                <el-form-item label="推送目标">
                  <el-radio-group v-model="w.targetType">
                    <el-radio label="kafka">Kafka Topic</el-radio>
                    <el-radio label="webhook">HTTP API(Webhook)</el-radio>
                  </el-radio-group>
                </el-form-item>
                <el-form-item v-if="w.targetType === 'webhook'" label="目标 URL">
                  <el-input v-model="w.webhook" placeholder="https://consumer.example.com/api/data" style="width:420px" />
                </el-form-item>
              </el-form>
              <div class="sec-title">{{ w.targetType === 'kafka' ? 'Kafka 交付协议' : 'Webhook 交付协议' }}</div>
              <pre class="mono">
{{ w.targetType === 'kafka' ? kafkaProto : webhookProto }}</pre>
            </template>
            <template v-else>
              <div class="sec-title">PULL 拉取协议(数据服务 API)</div>
              <pre class="mono">{{ pullProto }}</pre>
              <div class="dim2" style="margin-top:4px">批量导出 = 异步文件(JSON/CSV): POST /export 创建任务 → 轮询 → 分片下载(URL 带 TTL)。</div>
            </template>
            <div style="text-align:center;display:flex;gap:8px;justify-content:center;margin-top:10px">
              <el-button @click="step = 1">上一步</el-button><el-button type="primary" @click="step = 3">下一步</el-button>
            </div>
          </div>

          <!-- 步骤4 -->
          <div v-if="step === 3" style="max-width:760px;margin:auto">
            <el-form label-width="110px">
              <el-form-item label="密级上限"><el-select v-model="w.maxSecret" style="width:180px"><el-option v-for="(n, i) in ['', '公开', '内部', '机密', '绝密']" :key="i" :label="n || '不限'" :value="i" /></el-select>
                <el-text size="small" type="info" style="margin-left:8px">仅可授 ≤ 管理员范围; 机密+需审批</el-text></el-form-item>
              <el-form-item label="有效期"><el-date-picker v-model="w.ttl" type="date" style="width:220px" placeholder="到期自动回收" /></el-form-item>
              <el-form-item label="拉取配额(pull)"><el-input v-model="w.qps" style="width:220px" placeholder="如 1000 QPS / 10万行每日" /></el-form-item>
            </el-form>
            <div style="text-align:center;display:flex;gap:8px;justify-content:center">
              <el-button @click="step = 2">上一步</el-button><el-button type="primary" @click="step = 4">下一步</el-button>
            </div>
          </div>

          <!-- 步骤5 确认 -->
          <div v-if="step === 4" style="max-width:860px;margin:auto">
            <el-descriptions :column="1" size="small" border>
              <el-descriptions-item label="数据视图">{{ viewSummary }}</el-descriptions-item>
              <el-descriptions-item label="交付">{{ w.push.toUpperCase() }} · {{ w.freq }} <template v-if="w.push === 'push'">→ {{ w.targetType === 'kafka' ? 'Kafka dcp.sub.{app}.{cid}(SASL+ACL)' : w.webhook + ' (HMAC 验签)' }}</template><template v-else>数据服务 API(快照/增量/导出)</template></el-descriptions-item>
              <el-descriptions-item label="密级/有效期/配额">≤{{ secName(w.maxSecret) }} · {{ w.ttl || '永久' }} · {{ w.qps || '默认' }}</el-descriptions-item>
              <el-descriptions-item label="说明">提交进审批; 生效后自动签发取数凭据与协议文档; 全审计。</el-descriptions-item>
            </el-descriptions>
            <div style="text-align:center;display:flex;gap:8px;justify-content:center;margin-top:12px">
              <el-button @click="step = 3">上一步</el-button><el-button type="primary" @click="submit">提交审批(设计稿)</el-button>
            </div>
          </div>
        </el-tab-pane>

        <!-- ③ 审批 -->
        <el-tab-pane label="③ 审批" name="approve">
          <div v-for="p in pending" :key="p.id" class="ap-card">
            <div class="flex-between"><b>{{ p.name }}</b><el-text size="small" type="info">{{ p.app }} · {{ p.freq }} · {{ p.push.toUpperCase() }}</el-text></div>
            <div class="dim2" style="margin:4px 0">{{ p.viewSummary }}</div>
            <div class="flex" style="gap:6px">
              <el-button size="small" type="warning" @click="openApprove(p)">审批(可调范围)</el-button>
              <el-button size="small" text type="danger" @click="reject(p)">拒绝</el-button>
            </div>
          </div>
          <el-text v-if="!pending.length" size="small" type="info">暂无待审批</el-text>
        </el-tab-pane>

        <!-- ④ 交付监控 -->
        <el-tab-pane label="④ 交付监控" name="deliver">
          <el-table :data="deliveries" size="small" border>
            <el-table-column prop="ts" label="时间" width="170" />
            <el-table-column label="契约" min-width="150"><template #default="{ row }">{{ row.contract }}</template></el-table-column>
            <el-table-column label="通道" width="130"><template #default="{ row }"><el-tag size="small" :type="row.push==='push' ? 'danger' : 'primary'" effect="plain">{{ row.push.toUpperCase() }} · {{ row.ch }}</el-tag></template></el-table-column>
            <el-table-column prop="range" label="数据范围" width="150" />
            <el-table-column prop="rows" label="行数" width="70" align="center" />
            <el-table-column label="结果" width="90"><template #default="{ row }"><el-tag size="small" :type="row.ok ? 'success' : 'danger'" effect="plain">{{ row.ok ? '成功' : row.retrying ? '重试中' : '失败' }}</el-tag></template></el-table-column>
            <el-table-column label="操作" width="90"><template #default="{ row }"><el-button size="small" text type="primary">详情</el-button></template></el-table-column>
          </el-table>
        </el-tab-pane>
      </el-tabs>
    </el-card>

    <!-- 契约详情(含协议文档) -->
    <el-drawer v-model="dlg" :title="'契约 #' + (cur ? cur.id : '') + ' · ' + (cur ? cur.name : '')" size="640px">
      <template v-if="cur">
        <el-descriptions :column="1" size="small" border>
          <el-descriptions-item label="订阅方">{{ cur.app }} · appKey sk-****{{ (cur.app || '').slice(-4) }}</el-descriptions-item>
          <el-descriptions-item label="数据视图">{{ cur.vtype === 'logical' ? cur.tables.join(' + ') + (cur.merged ? '(同构合并)' : '') + ' · 列' + cur.cols : cur.tags.join(' ' + cur.tagOp + ' ') + ' · ' + (cur.exportShape === 'wide' ? '回表 ' + cur.joinTable : cur.exportShape === 'matrix' ? '标签矩阵' : '实体键') }}</el-descriptions-item>
          <el-descriptions-item label="交付">{{ cur.push.toUpperCase() }} · {{ cur.targetType }} · {{ cur.freq }}</el-descriptions-item>
          <el-descriptions-item label="状态">{{ cur.status }} · 审批 {{ cur.approvedBy || '—' }} {{ cur.approvedAt || '' }}</el-descriptions-item>
        </el-descriptions>
        <div class="sec-title">交付协议(取数指南)</div>
        <pre class="mono">{{ cur.push === 'pull' ? pullProto : (cur.targetType === 'kafka' ? kafkaProto : webhookProto) }}</pre>
      </template>
    </el-drawer>

    <!-- 审批弹层 -->
    <el-dialog v-model="apDlg" :title="'审批 · ' + (apCur ? apCur.name : '')" width="580">
      <template v-if="apCur">
        <el-form label-width="110px" size="small">
          <el-form-item label="可用列"><el-checkbox-group v-model="apForm.cols"><el-checkbox v-for="c in allCols" :key="c" :label="c" /></el-checkbox-group></el-form-item>
          <el-form-item label="密级上限"><el-select v-model="apForm.maxSecret" style="width:160px"><el-option v-for="(n, i) in ['', '公开', '内部', '机密']" :key="i" :label="n || '不限'" :value="i" /></el-select></el-form-item>
          <el-form-item label="配额"><el-input v-model="apForm.qps" style="width:220px" /></el-form-item>
          <el-form-item label="备注"><el-input v-model="apForm.note" /></el-form-item>
        </el-form>
      </template>
      <template #footer><el-button size="small" @click="apDlg = false">取消</el-button><el-button size="small" type="success" @click="approve">批准生效</el-button></template>
    </el-dialog>
  </div>
</template>

<script setup>
import { ref, computed } from 'vue'
import { ElMessage } from 'element-plus'

const tab = ref('contracts')
const step = ref(0)
const dlg = ref(false)
const cur = ref(null)
const apDlg = ref(false)
const apCur = ref(null)
const apForm = ref({ cols: [], maxSecret: 2, qps: '', note: '' })
const secName = (s) => ['', '公开', '内部', '机密', '绝密'][s] || '不限'
const stType = (s) => ({ '生效中': 'success', '待审批': 'warning', '已到期': 'info' }[s] || 'info')

const tables = [
  { name: 'customer_wide', biz: '客户域' }, { name: 't_order_model', biz: '交易域' },
  { name: 't_user_model', biz: '用户域' }, { name: 't_mix_poc', biz: '混域' }
]
const tagOpts = [
  { path: '业务域/风控/黑名单' }, { path: '业务域/交易/订单/金额档' }, { path: '业务域/交易/订单/净支付额' }
]
const allCols = ['id', 'customer_id', 'region', 'status', 'amount', 'name']

const contracts = ref([
  { id: 1, name: 'OMS 风控人群实时推送', app: 'oms-prod', vtype: 'tag', tags: ['业务域/风控/黑名单'], tagOp: 'OR', exportShape: 'wide', joinTable: 'customer_wide', push: 'push', targetType: 'webhook', freq: '实时', status: '生效中', rows7d: 1280, lastAt: '21:40', approvedBy: 'admin', approvedAt: '09-01' },
  { id: 2, name: '数仓客户视图日快照', app: 'dwh-batch', vtype: 'logical', tables: ['customer_wide'], merged: false, cols: 4, push: 'pull', targetType: 'API', freq: '每日 02:00', status: '生效中', rows7d: 4200, lastAt: '02:00', approvedBy: 'admin', approvedAt: '09-02' },
  { id: 3, name: '多源同构订单合并(增量拉)', app: 'risk-engine', vtype: 'logical', tables: ['t_order_model', 't_mix_poc'], merged: true, cols: 5, cond: true, push: 'pull', targetType: 'API', freq: '批量·游标增量', status: '待审批', rows7d: 0, lastAt: '', viewSummary: 't_order_model+t_mix_poc 同构合并 · 5 列 · amount≥5000' },
  { id: 4, name: '营销大额人群流式', app: 'mkt-push', vtype: 'tag', tags: ['业务域/交易/订单/金额档'], tagOp: 'AND', exportShape: 'wide', joinTable: 'customer_wide', push: 'push', targetType: 'kafka', freq: '流式', status: '待审批', rows7d: 0, lastAt: '', viewSummary: '金额档=大额 圈选 → 回表 customer_wide · Kafka SASL+ACL' }
])
const pending = computed(() => contracts.value.filter(c => c.status === '待审批'))
const deliveries = ref([
  { ts: '2026-09-08 21:40', contract: 'OMS 风控人群实时推送', push: 'push', ch: 'Webhook', range: 'seq>120', rows: 3, ok: true },
  { ts: '2026-09-08 21:36', contract: 'OMS 风控人群实时推送', push: 'push', ch: 'Webhook', range: 'seq>119', rows: 2, ok: true },
  { ts: '2026-09-08 02:00', contract: '数仓客户视图日快照', push: 'pull', ch: 'API', range: '全量', rows: 1200, ok: true },
  { ts: '2026-09-08 02:00', contract: '多源同构订单合并(增量拉)', push: 'pull', ch: 'API', range: 'cursor 88', rows: 500, ok: true },
  { ts: '2026-09-08 01:30', contract: '营销大额人群流式', push: 'push', ch: 'Kafka', range: 'tag 事件', rows: 0, ok: false, retrying: true }
])

const pullProto = `# PULL · 数据服务 API(Bearer token = appKey 换发)
GET /api/data-sub/{cid}/schema
GET /api/data-sub/{cid}/snapshot?pageToken=
GET /api/data-sub/{cid}/changes?after={cursor}&limit=1000
POST /api/data-sub/{cid}/export?format=csv|json   # 异步文件
响应: {"code":0,"data":{"rows":[...],"nextCursor":"..."}}
语义: at-least-once · 主键幂等 · 游标订阅方持有 · 每次调用审计
示例: curl -H "Authorization: Bearer $TOKEN" \\
  "https://dcp.example.com/api/data-sub/3/changes?after=88"`

const webhookProto = `# PUSH · Webhook 协议
POST {目标URL}
Headers:
  Content-Type: application/json
  X-Dcp-Contract: {cid}
  X-Dcp-Timestamp: 1720000000000
  X-Dcp-Sign: HMAC-SHA256(body+timestamp, secret=订阅方密钥)
Body(实时事件): {"op":"INSERT|UPDATE|DELETE","ts":...,"cursor":...,"row":{...}}
Body(标签事件): {"op":"tag|untag","entity":...,"tag":...,"value":...,"ts":...}
语义: 2xx=成功 · 验签+防重放(±5min) · 5xx/超时指数退避 1m/5m/30m/2h → 死信可重投`

const kafkaProto = `# PUSH · Kafka 交付协议
Topic: dcp.sub.{appKey}.{contractId}   # 每契约独立
认证: SASL/PLAIN + Topic ACL(订阅方账号仅读写本契约 topic, 与平台内部隔离)
分区键: 主键 hash(保序) · at-least-once · 主键幂等
批/快照: 行数组 + header(contract/version/batchTs)
增量事件: {"op":"INSERT|UPDATE|DELETE","ts":...,"cursor":...,"row":{...}}
标签事件: {"op":"tag|untag","entity":...,"tag":...,"value":...,"ts":...}`

const w = ref({
  viewType: 'logical', compose: false,
  tables: [], mergeMode: 'merge', cols: [], condGroups: [{ conds: [{ field: 'amount', op: '>=', val: '5000' }] }],
  condTag: false, condTagVal: '',
  tags: [], tagOp: 'OR', exclude: '', exportShape: 'wide', joinTable: 'customer_wide', joinCols: [],
  push: 'pull', freq: '定时', targetType: 'webhook', webhook: '',
  maxSecret: 2, ttl: null, qps: ''
})
const previewCols = computed(() => w.value.viewType === 'logical' ? w.value.cols : ['entity_key', ...(w.value.exportShape === 'wide' ? w.value.joinCols : w.value.exportShape === 'matrix' ? w.value.tags : [])])
const viewSummary = computed(() => w.value.viewType === 'logical'
  ? w.value.tables.join(' + ') + (w.value.tables.length > 1 && w.value.mergeMode === 'merge' ? '(同构合并)' : '') + ' · 列' + w.value.cols.join(',')
  : w.value.tags.join(' ' + w.value.tagOp + ' ') + (w.value.exportShape === 'wide' ? ' → 回表 ' + w.value.joinTable + '(' + w.value.joinCols.join(',') + ')' : w.value.exportShape === 'matrix' ? '(标签矩阵)' : '(实体键)'))
function toggleTable(t) { const i = w.value.tables.indexOf(t); i >= 0 ? w.value.tables.splice(i, 1) : w.value.tables.push(t) }
function toggleCol(c) { const i = w.value.cols.indexOf(c); i >= 0 ? w.value.cols.splice(i, 1) : w.value.cols.push(c) }
function toggleJoin(c) { const i = w.value.joinCols.indexOf(c); i >= 0 ? w.value.joinCols.splice(i, 1) : w.value.joinCols.push(c) }
function toggleTag(t) { const i = w.value.tags.indexOf(t); i >= 0 ? w.value.tags.splice(i, 1) : w.value.tags.push(t) }
function submit() { ElMessage.success('已提交审批(设计稿)'); step = 0; tab = 'contracts' }
function openDetail(row) { cur = row; dlg = true }
function openApprove(p) { apCur = p; apForm = { cols: [...allCols], maxSecret: 2, qps: '1000 QPS', note: '' }; apDlg = true }
function approve() { const c = contracts.value.find(x => x.id === apCur.id); if (c) { c.status = '生效中'; c.approvedBy = 'admin'; c.approvedAt = '2026-09-08' } apDlg = false; ElMessage.success('已批准(设计稿)') }
function reject(p) { p.status = '已拒绝'; ElMessage.warning('已拒绝(设计稿)') }
</script>

<style scoped>
.page-card { max-width: 1580px; }
.pg-title { font-size: 17px; font-weight: 700; }
.flex { display: flex; align-items: center; }
.flex-between { display: flex; justify-content: space-between; align-items: center; }
.dim2 { color: #999; font-size: 12px; }
.chips { display: flex; flex-wrap: wrap; gap: 6px; }
.sec-title { font-size: 13px; font-weight: 600; margin: 10px 0 6px; }
.panel { border: 1px solid var(--el-border-color-lighter); border-radius: 8px; padding: 10px 14px; }
.cond-group { border-left: 2px solid var(--el-color-primary-light-5); padding-left: 10px; margin: 4px 0; }
.cond-line { display: flex; gap: 6px; align-items: center; margin: 4px 0; flex-wrap: wrap; }
.hint { background: #f0f7ff; border: 1px solid #c6e0f7; color: #2a5d8f; padding: 8px 10px; border-radius: 6px; font-size: 12px; }
.mono { background: #0d1b2a; color: #9fdcb0; padding: 10px; font-size: 12px; border-radius: 6px; white-space: pre-wrap; word-break: break-all; }
.ap-card { border: 1px solid var(--el-border-color-lighter); border-radius: 8px; padding: 10px 12px; margin-bottom: 8px; }
.sub { border: none; }
</style>
