<template>
  <div class="lineage-page">
    <!-- ===== 顶栏 ===== -->
    <div class="lg-head">
      <div class="lg-title">⛓ 数据血缘分析
        <el-tag size="small" effect="plain" style="margin-left:8px">知识图谱 · Neo4j 就绪后接真</el-tag>
      </div>
      <div class="lg-nav">
        <div v-for="t in tabs" :key="t.k" class="lg-nav-item" :class="{ on: tab === t.k }" @click="tab = t.k">{{ t.t }}</div>
      </div>
    </div>

    <!-- ===== ① 血缘图谱 ===== -->
    <div v-show="tab === 'graph'" class="lg-view">
      <div class="graph-shell">
        <div class="gbar">
          <el-input v-model="q" size="small" placeholder="输入节点名定位: customer_wide / amount / 订阅 #34" style="width:300px" clearable @keyup.enter="locate()">
            <template #prefix>🔍</template>
          </el-input>
          <el-button size="small" type="primary" plain @click="focusField('FAMT')">聚焦 amount(3 源合流)</el-button>
          <el-button size="small" type="danger" plain @click="focusField('FPH')">聚焦敏感 phone</el-button>
          <el-button size="small" @click="clearFocus()">清除聚焦</el-button>
          <el-button size="small" @click="toggleFields()">{{ fieldOn ? '字段: 收起' : '字段: 展开' }}</el-button>
          <el-button size="small" @click="locate()">定位</el-button>
          <span class="gstats">节点 {{ allNodes().length }} · 边 {{ EDGES.length }} · 图存储 Neo4j(解析中)</span>
        </div>
        <div class="canvas" ref="canvasRef" @pointerdown="canvasDown" @pointermove="canvasMove" @pointerup="canvasUp" @wheel.prevent="onWheel">
          <svg ref="svgRef" viewBox="0 0 1500 860"></svg>
          <div class="zoom-col">
            <el-button size="small" @click.stop="zoomBy(1.2)">＋</el-button>
            <el-button size="small" @click.stop="zoomBy(1 / 1.2)">－</el-button>
            <el-button size="small" @click.stop="fitAll()">⤢</el-button>
          </div>
          <div class="legend">
            <div><b style="color:#cfe3ff">图例</b></div>
            <div v-for="l in legend" :key="l.t" class="lg-row"><span class="sw" :style="{ borderColor: l.c }"></span>{{ l.n }}</div>
            <div class="hint">拖节点 · 滚轮缩放 · 拖空白平移 · 悬停摘要 · 点击详情</div>
          </div>
          <div class="g-tip" ref="tipRef"></div>
        </div>
      </div>

      <!-- 节点/边详情抽屉 -->
      <el-drawer v-model="drawer" :title="dt" size="440px">
        <div class="dm">{{ dm }}</div>
        <div class="dtabs">
          <div v-for="(t, i) in dtabs" :key="t" class="dtab" :class="{ on: dtab === i }" @click="setDtab(i)">{{ t }}</div>
        </div>
        <div class="dbody" v-html="dhtml"></div>
      </el-drawer>
    </div>

    <!-- ===== ② 字段映射矩阵 ===== -->
    <div v-show="tab === 'matrix'" class="lg-view page-card">
      <div class="pg-title">字段级血缘矩阵<el-tag size="small" effect="plain" style="margin-left:8px">一行 = 一条来源边</el-tag></div>
      <div class="dim2" style="margin:6px 0 10px">目标: customer_wide · 同一目标字段多条来源 = 多行(合流), 每行独立: 源系统/任务/转换/策略/优先级</div>
      <el-alert type="info" :closable="false" show-icon style="margin-bottom:8px" title="amount = 3 条来源边(3 行): 源① 系统A 直映射(主) / 源② 系统B 合流(后写覆盖) / 源③ 规则 r4 派生写回 —— 修改任一来源沿各自行评估影响" />
      <el-table :data="matrix" size="small" border max-height="560" :span-method="mSpan">
        <el-table-column label="目标字段" width="150"><template #default="{ row }"><b>{{ row.target }}</b></template></el-table-column>
        <el-table-column label="来源序号" width="90"><template #default="{ row }"><el-tag v-if="row.tag" size="small" :type="row.tagType || 'info'" effect="plain">{{ row.tag }}</el-tag><span v-else class="dim2">—</span></template></el-table-column>
        <el-table-column label="源(系统.表.列)" min-width="180"><template #default="{ row }"><span class="mono2">{{ row.src }}</span><div class="dim2">{{ row.task }}</div></template></el-table-column>
        <el-table-column label="转换类型" width="100"><template #default="{ row }"><el-tag size="small" :type="row.t === '直映射' ? 'primary' : row.t === '派生/规则产出' ? 'danger' : 'warning'" effect="plain">{{ row.t }}</el-tag></template></el-table-column>
        <el-table-column prop="rule" label="转换规则" min-width="140" />
        <el-table-column label="合并策略" width="110"><template #default="{ row }"><span v-if="row.strategy">{{ row.strategy }}</span><span v-else class="dim2">—</span></template></el-table-column>
        <el-table-column label="优先级" width="80"><template #default="{ row }"><span v-if="row.pri">{{ row.pri }}</span><span v-else class="dim2">—</span></template></el-table-column>
        <el-table-column label="密级" width="70"><template #default="{ row }"><el-tag size="small" :type="row.secret === 'L3' ? 'danger' : 'info'" effect="plain">{{ row.secret }}</el-tag></template></el-table-column>
      </el-table>
      <div class="dim2" style="margin-top:6px">约定: 一行=一条 FIELD_MAPPING/PRODUCES 边(与血缘图字段级边一一对应); 同目标字段多行=水平合流, 无垂直主从之分, 仅标注优先级与覆盖策略</div>
    </div>

    <!-- ===== ③ 影响分析 ===== -->
    <div v-show="tab === 'impact'" class="lg-view page-card">
      <div class="pg-title">影响分析<el-tag size="small" effect="plain" style="margin-left:8px">3 层依赖 · 路径集</el-tag></div>
      <div class="dim2" style="margin:6px 0 10px">变更对象: <b>customer_wide.amount</b>(合流字段 · 3 来源 · 4 消费路径)</div>
      <div class="imp-sum">⚠ <b>影响评估:</b> 共影响 <b>6</b> 个下游 —— 订阅契约 3、BI 报表 1(预留)、检索服务 1(预留)、Iceberg 湖 1;经长链路触及金额档圈选订阅 3 个</div>
      <div class="imp-cols">
        <div>
          <div class="imp-lv"><div class="lv">第 1 层<br>直接依赖</div>
            <div class="lv-body">
              <div class="ic"><div class="i1">📮 订阅 #34 大额日快照 <el-tag size="small" effect="plain">SUBSCRIBE</el-tag><span class="dim2" style="margin-left:auto">dwh-batch · Owner 王工</span></div><div class="i2">6 列视图含 amount(≥5000 条件)</div></div>
              <div class="ic"><div class="i1">📮 订阅 #47 人群宽表 <el-tag size="small" effect="plain">SUBSCRIBE</el-tag><span class="dim2" style="margin-left:auto">mkt-ext · Owner 李敏</span></div><div class="i2">宽表回表取 amount</div></div>
              <div class="ic"><div class="i1">⬇️ Iceberg 湖表 <el-tag size="small" type="success" effect="plain">LAKE</el-tag><span class="dim2" style="margin-left:auto">Owner 张工</span></div><div class="i2">15min 快照 · schema 自动演进</div></div>
            </div></div>
          <div class="imp-lv"><div class="lv">第 2 层<br>加工/长链路</div>
            <div class="lv-body">
              <div class="ic"><div class="i1">🎯 规则 r1 金额档 <el-tag size="small" type="danger" effect="plain">RULE</el-tag></div><div class="i2">标签事实重算 → 圈选名单变化</div></div>
              <div class="ic"><div class="i1">📊 BI 经营分析 <el-tag size="small" type="warning" effect="plain">REPORT 预留</el-tag><span class="dim2" style="margin-left:auto">Owner 陈工</span></div><div class="i2">amount → 金额档 → dws_weekly → BI</div></div>
            </div></div>
          <div class="imp-lv"><div class="lv">第 3 层<br>圈选订阅</div>
            <div class="lv-body"><div class="ic"><div class="i1">🎯 金额档实体订阅 ×3 <el-tag size="small" effect="plain">间接</el-tag></div><div class="i2">#48(push kafka)· #50 · #52 —— 依赖标签「金额档」</div></div></div></div>
        </div>
        <div class="act-card">
          <div style="font-weight:600;margin-bottom:8px">变更动作</div>
          <div class="dim2" style="line-height:2">通知对象(血缘自动圈选):</div>
          <el-checkbox v-model="n1" label="dwh-batch 王工" />
          <el-checkbox v-model="n2" label="mkt-ext 李敏" />
          <el-checkbox v-model="n3" label="BI/检索 陈工" />
          <el-checkbox v-model="n4" label="湖任务 张工" />
          <el-input v-model="note" type="textarea" :rows="3" placeholder="变更说明: amount 口径改为净支付额(含退款)…" style="margin:8px 0" />
          <div class="flex" style="gap:8px">
            <el-button size="small" type="primary" @click="sendNotice">生成变更影响通知</el-button>
            <el-button size="small" @click="noticeOk('已仅记录审计')">仅记录审计</el-button>
          </div>
        </div>
      </div>
    </div>

    <!-- ===== ④ 溯源分析 ===== -->
    <div v-show="tab === 'trace'" class="lg-view page-card">
      <div class="pg-title">溯源分析<el-tag size="small" effect="plain" style="margin-left:8px">最终来源 + 备选路径集</el-tag></div>
      <div class="dim2" style="margin:6px 0 10px">字段: <b>customer_wide.amount</b> —— 多源合流, 返回全路径集(非单链)</div>
      <div class="tr-final">✔ <b>最终来源(主): 系统A(t_user_base.user_balance)</b> —— 直映射, 占比最高</div>
      <div v-for="(p, i) in traces" :key="i" class="tr-card">
        <div class="tr-title">{{ p.t }}</div>
        <div class="tr-steps">
          <span v-for="(s, j) in p.steps" :key="j" class="tr-node" :class="s.k">{{ s.x }}</span>
          <span v-if="p.desc" class="dim2" style="margin-left:10px">{{ p.desc }}</span>
        </div>
      </div>
      <el-alert type="warning" :closable="false" style="margin-top:8px" title="多源合流说明: amount 存在 3 条来源路径(两同步任务 + 一规则), 任一上游变更均沿全部路径评估; 未声明依赖(DRL r6)标「来源未知」, 人工补边后自动入图" />
    </div>

    <!-- ===== ⑤ 变更影响通知 ===== -->
    <div v-show="tab === 'notices'" class="lg-view page-card">
      <div class="pg-title">变更影响通知<el-tag size="small" effect="plain" style="margin-left:8px">标准方案 §5.5 · 平台内闭环</el-tag></div>
      <div class="dim2" style="margin:6px 0 10px">触发: 结构变更探测器 + 影响分析生成 · 落库/审计/回执(外发邮件/IM 留扩展)</div>
      <div class="notice-card">
        <div class="nt">📋 数据变更影响通知</div>
        <div class="kv"><b>变更资产</b>customer_wide.amount(逻辑表)</div>
        <div class="kv"><b>变更类型</b>字段语义变更(口径: 消费金额 → 净支付额含退款)</div>
        <div class="kv"><b>变更时间</b>2026-09-09 15:00:00 · 由 admin 发起</div>
        <div class="split" />
        <div style="font-weight:600;margin-bottom:4px">受影响下游(4 直接 + 1 长链路):</div>
        <div class="tree">├── 📮 订阅 #34 dwh-batch <el-tag size="small" effect="plain">Owner 王工</el-tag> — 评估口径影响并确认是否再审批<br>
          ├── 📮 订阅 #47 mkt-ext <el-tag size="small" effect="plain">Owner 李敏</el-tag> — 回表数值口径复核<br>
          ├── 📊 BI 经营分析(预留) <el-tag size="small" effect="plain">Owner 陈工</el-tag> — 经金额档长链路<br>
          └── ⬇️ Iceberg 湖表 <el-tag size="small" effect="plain">Owner 张工</el-tag> — 快照自动同步</div>
        <div class="split" />
        <div class="dim2">请相关 Owner 在 <b style="color:#fa8c16">3 个工作日</b>内完成评估反馈。</div>
        <div style="margin-top:10px;display:flex;gap:8px">
          <el-button size="small" type="primary" @click="noticeOk('已确认收到(回执)')">确认收到(回执)</el-button>
          <el-button size="small" @click="tab = 'graph'">查看影响全图</el-button>
        </div>
      </div>
      <el-table :data="notices" size="small" border style="margin-top:12px" max-height="260">
        <el-table-column prop="id" label="通知单" min-width="240"><template #default="{ row }"><b>{{ row.id }}</b> {{ row.name }}</template></el-table-column>
        <el-table-column prop="trigger" label="触发" width="140" />
        <el-table-column prop="to" label="收件" width="180" />
        <el-table-column label="状态" width="110"><template #default="{ row }"><el-tag size="small" :type="row.st === '待确认' ? 'warning' : 'success'" effect="plain">{{ row.st }}</el-tag></template></el-table-column>
        <el-table-column label="" width="80"><template #default><el-button size="small" text type="primary">详情</el-button></template></el-table-column>
      </el-table>
    </div>
  </div>
</template>

<script setup>
import { ref, reactive, onMounted } from 'vue'
import { ElMessage } from 'element-plus'

const tabs = [
  { k: 'graph', t: '⛓ 血缘图谱' }, { k: 'matrix', t: '字段映射矩阵' },
  { k: 'impact', t: '影响分析' }, { k: 'trace', t: '溯源分析' }, { k: 'notices', t: '变更影响通知' }
]
const tab = ref('graph')
const q = ref('')
/* 通知动作 */
const n1 = ref(true); const n2 = ref(true); const n3 = ref(true); const n4 = ref(true)
const note = ref('amount 由"消费金额"改为"净支付额(含退款)" — 口径变化, schema 不变')
function noticeOk(m) { ElMessage.success(m) }
function sendNotice() { noticeOk('已生成变更影响通知 #N-1025(收件人按血缘圈选)') }

/* ================= 图谱数据(mock, 接真后来自 /api/lineage) ================= */
const NODES = [
  { id: 'A', k: 'sys', x: 70, y: 160, icon: '🗄', label: '系统A', sub: '核心交易 MySQL', own: '李工' },
  { id: 'B', k: 'sys', x: 70, y: 330, icon: '🗄', label: '系统B', sub: '账务 PostgreSQL', own: '孙工' },
  { id: 'C', k: 'sys', x: 70, y: 500, icon: '🗄', label: '系统C', sub: 'CRM Oracle', own: '赵工' },
  { id: 'T12', k: 'task', x: 250, y: 160, icon: '⇄', label: '同步 #12', sub: 'CDC · 字段映射7', own: '王工' },
  { id: 'T21', k: 'task', x: 250, y: 330, icon: '⇄', label: '同步 #21', sub: '批量日切', own: '王工' },
  { id: 'T33', k: 'task', x: 250, y: 500, icon: '⇄', label: '同步 #33', sub: '实时', own: '王工' },
  { id: 'R4', k: 'rule', x: 250, y: 690, icon: '⚙️', label: '规则 r4', sub: '净支付额 → amount', own: '张工' },
  { id: 'CW', k: 'core', x: 540, y: 330, icon: '▤', label: 'customer_wide', sub: '逻辑表 · DWD 客户域 · L2', own: '王工', meta: '模型 M1 · 9 列 · 每5min · 健康98' },
  { id: 'R1', k: 'rule', x: 790, y: 180, icon: '🎯', label: '规则 r1', sub: '金额档(依赖 amount)', own: '张工' },
  { id: 'DWS', k: 'task', x: 790, y: 310, icon: '▤', label: 'dws_weekly', sub: '派生周汇总表', own: '陈工' },
  { id: 'S34', k: 'sub', x: 1060, y: 80, icon: '📮', label: '订阅 #34', sub: 'dwh-batch · pull', own: '王工' },
  { id: 'S47', k: 'sub', x: 1060, y: 190, icon: '📮', label: '订阅 #47', sub: 'mkt-ext · 宽表回表', own: '李敏' },
  { id: 'S48', k: 'sub', x: 1060, y: 300, icon: '📮', label: '订阅 #48', sub: 'mkt-k · push', own: '李敏' },
  { id: 'TAG', k: 'tag', x: 950, y: 240, icon: '🏷', label: '标签 · 金额档', sub: 'registry', own: '张工' },
  { id: 'BI', k: 'app', x: 1060, y: 430, icon: '📊', label: 'BI 经营分析', sub: 'REPORT 预留', own: '陈工' },
  { id: 'API', k: 'app', x: 1060, y: 530, icon: '🔌', label: '订单 API', sub: 'API 预留', own: '李工' },
  { id: 'SEARCH', k: 'app', x: 1060, y: 630, icon: '🔎', label: '检索服务', sub: 'SEARCH 预留', own: '陈工' },
  { id: 'LAKE', k: 'app', x: 1060, y: 740, icon: '⬇️', label: 'Iceberg 湖', sub: '15min 快照', own: '张工' }
]
const FIELDS = [
  { id: 'FAMT', k: 'field', fx: 540, fy: 420, label: 'amount', sub: '合流 3源', rel: 'amt', icon: '#' },
  { id: 'FPH', k: 'field', fx: 540, fy: 455, label: 'phone 🔒', sub: 'L3 mask', rel: 'sen', icon: '#' }
]
const EDGES = [
  { f: 'A', to: 'T12', t: 'sync' }, { f: 'B', to: 'T21', t: 'sync' }, { f: 'C', to: 'T33', t: 'sync' },
  { f: 'T12', to: 'CW', t: 'sync', l: 'user_id→id · phone · user_balance→amount', amt: 1 },
  { f: 'T21', to: 'CW', t: 'sync', l: 'bal→amount 合流②(后写覆盖)', amt: 1 },
  { f: 'T33', to: 'CW', t: 'sync', l: 'org+user→name(concat)' },
  { f: 'R4', to: 'CW', t: 'prod', l: 'net_amt−fee→amount 派生③', amt: 1 },
  { f: 'CW', to: 'S34', t: 'sub', l: '6列视图 amount≥5000', amt: 1 },
  { f: 'CW', to: 'S47', t: 'sub', l: '宽表回表(含 amount)', amt: 1 },
  { f: 'CW', to: 'BI', t: 'chain', l: '经 r1→dws_weekly', amt: 1 },
  { f: 'CW', to: 'API', t: 'reserve', l: '预留' },
  { f: 'CW', to: 'SEARCH', t: 'reserve', l: '预留' },
  { f: 'CW', to: 'LAKE', t: 'sub', l: '15min 快照' },
  { f: 'CW', to: 'R1', t: 'chain', l: 'amount→金额档判定', amt: 1 },
  { f: 'R1', to: 'DWS', t: 'chain', l: '金额档→周聚合', amt: 1 },
  { f: 'R1', to: 'TAG', t: 'prod', l: '产出标签 金额档' },
  { f: 'TAG', to: 'S48', t: 'sub', l: '标签矩阵圈选 · push' },
  { f: 'DWS', to: 'BI', t: 'chain', l: '周表→报表', amt: 1 }
]
/* 字段级边(展开字段时显示; 真血缘: 与矩阵一行一边对应) */
const FEDGES = [
  { f: 'T12', to: 'FAMT', t: 'sync', l: 'user_balance→amount', amt: 1, src: '源①' },
  { f: 'T21', to: 'FAMT', t: 'sync', l: 'bal→amount 后写覆盖', amt: 1, src: '源②' },
  { f: 'R4', to: 'FAMT', t: 'prod', l: 'net_amt−fee→amount', amt: 1, src: '源③' },
  { f: 'FAMT', to: 'S34', t: 'sub', l: '6列视图 amount≥5000', amt: 1 },
  { f: 'FAMT', to: 'S47', t: 'sub', l: '宽表回表取 amount', amt: 1 },
  { f: 'FAMT', to: 'R1', t: 'chain', l: '金额档判定入参', amt: 1 },
  { f: 'T12', to: 'FPH', t: 'sync', l: 'phone 直映射(源敏感)', sen: 1 },
  { f: 'FPH', to: 'LAKE', t: 'sub', l: '湖快照明文(治理项)', sen: 1, dash: 1 }
]
const COL = { sync: '#38bdf8', prod: '#c084fc', sub: '#34d399', chain: '#fbbf24', reserve: '#64748b' }
const SZ = { sys: [168, 48], task: [150, 46], rule: [170, 50], core: [196, 74], sub: [172, 48], app: [172, 48], field: [128, 30], tag: [150, 36] }
const KT = { sys: ['系统', '#f59e0b'], task: ['同步任务', '#38bdf8'], rule: ['规则/加工', '#c084fc'], core: ['逻辑表', '#22d3ee'], sub: ['订阅', '#34d399'], app: ['应用(预留)', '#64748b'], field: ['字段', '#fbbf24'], tag: ['标签', '#fb923c'] }
const legend = [
  { t: 'sync', n: '同步任务映射', c: '#38bdf8' }, { t: 'prod', n: '规则产出', c: '#c084fc' },
  { t: 'sub', n: '订阅消费', c: '#34d399' }, { t: 'chain', n: '长链路/加工', c: '#fbbf24' },
  { t: 'reserve', n: '预留应用', c: '#64748b' }
]

/* ================= 子页数据 ================= */
/* 一行 = 一条来源边(与图谱字段级边一一对应); 同目标字段多行=水平合流 */
const matrix = [
  { target: 'id', src: 't_user_base.user_id', task: '系统A · 同步 #12', t: '直映射', rule: 'BIGINT', secret: 'L1' },
  { target: 'customer_id', src: 't_user_base.user_id', task: '系统A · 同步 #12', t: '直映射', rule: '别名', secret: 'L1' },
  { target: 'phone', src: 't_user_base.phone', task: '系统A · 同步 #12', t: '直映射', rule: 'VARCHAR(源敏感)', secret: 'L3' },
  { target: 'name', src: 't_user_org.org + t_user_base.user', task: '系统C · 同步 #33', t: '拼接', rule: 'CONCAT(org,user)', secret: 'L1' },
  { target: 'amount', mergeLabel: '合流 3 来源', tag: '源①', src: 't_user_base.user_balance', task: '系统A · 同步 #12', t: '直映射', rule: 'DECIMAL(12,2)', strategy: '主来源(占比最高)', pri: 'P1', secret: 'L2' },
  { target: 'amount', tag: '源②', tagType: 'warning', src: 't_acct.bal', task: '系统B · 同步 #21', t: '直映射', rule: 'DECIMAL(12,2)', strategy: '合流 · 后写覆盖', pri: 'P2', secret: 'L2' },
  { target: 'amount', tag: '源③', src: 'net_amt − fee', task: '规则 r4 · 任务 T7', t: '派生/规则产出', rule: '≥5000 判定写回', strategy: '派生写回', pri: 'P3', secret: 'L2' },
  { target: 'region', src: 't_user_base.region', task: '系统A · 同步 #12', t: '直映射', rule: 'VARCHAR', secret: 'L1' },
  { target: 'status', src: 't_user_base.status', task: '系统A · 同步 #12', t: '直映射', rule: '枚举', secret: 'L1' }
]
const _mg = {}
matrix.forEach((r, i) => { if (!(r.target in _mg)) _mg[r.target] = { start: i, len: 0 }; _mg[r.target].len++ })
function mSpan({ row, rowIndex, columnIndex }) {
  if (columnIndex !== 0) return
  const g = _mg[row.target]
  if (!g) return
  return rowIndex === g.start ? { rowspan: g.len, colspan: 1 } : { rowspan: 0, colspan: 0 }
}
const traces = [
  { t: '路径 1 · 最终来源(主)', steps: [{ x: '系统A t_user_base.user_balance', k: 'src' }, { x: '直映射 #12', k: '' }, { x: 'customer_wide.net_amt', k: '' }, { x: '规则 r4 依赖', k: '' }, { x: '净支付额 net_amt−fee', k: 'tr' }, { x: 'customer_wide.amount', k: 'cur' }] },
  { t: '路径 2 · 备选来源(系统B 合流)', steps: [{ x: '系统B t_acct.bal', k: 'src' }, { x: '合流 #21 后写覆盖', k: '' }, { x: 'customer_wide.amount', k: 'cur' }] },
  { t: '路径 3 · 备选来源(规则 r4 派生)', steps: [{ x: '规则 r4(net_amt−fee)', k: 'src' }, { x: '派生写回 T7', k: '' }, { x: 'customer_wide.amount', k: 'cur' }] }
]
const notices = [
  { id: '#N-1024', name: 'customer_wide.amount 口径变更', trigger: '人工/影响分析', to: '王工/李敏/陈工/张工', st: '待确认' },
  { id: '#N-1018', name: 'phone 源脱敏计划', trigger: '敏感路径扫描', to: '张工/湖 Owner', st: '已确认' },
  { id: '#N-1011', name: '契约 #34 到期续期提醒', trigger: '生命周期', to: 'dwh-batch', st: '已确认' }
]

/* ================= 图谱渲染 ================= */
const svgRef = ref(null); const canvasRef = ref(null); const tipRef = ref(null)
const NS = 'http://www.w3.org/2000/svg'
let world = null, nodeEls = {}, edgeEls = [], fieldOn = false, curNode = null
let scale = 1, panX = 0, panY = 0, drag = null, panning = null
const drawer = ref(false); const dt = ref(''); const dm = ref(''); const dtabs = ref([]); const dtab = ref(0); const dhtml = ref('')
function allNodes() { return [...NODES, ...FIELDS] }
function el(t, c) { const n = document.createElementNS(NS, t); if (c) n.setAttribute('class', c); return n }
function cx(n) { return (n.k === 'field' ? n.fx : n.x) + (n.ox || 0) }
function cy(n) { return (n.k === 'field' ? n.fy : n.y) + (n.oy || 0) }
function applyView() { world.setAttribute('transform', `translate(${panX},${panY}) scale(${scale})`) }
function zoomBy(f) { scale = Math.min(2.4, Math.max(0.5, scale * f)); applyView() }
function fitAll() { scale = 1; panX = 0; panY = 0; applyView() }
function onWheel(e) { zoomBy(e.deltaY < 0 ? 1.15 : 1 / 1.15) }
function build() {
  const svg = svgRef.value
  world = el('g'); world.id = 'world'; svg.appendChild(world)
  const defs = el('defs'); world.appendChild(defs)
  let defsI = 0
  allEdgeList().forEach((e) => {
    const m = el('marker'); m.id = 'ar' + (defsI++); m.setAttribute('viewBox', '0 0 10 10'); m.setAttribute('refX', '9'); m.setAttribute('refY', '5')
    m.setAttribute('markerWidth', '6.5'); m.setAttribute('markerHeight', '6.5'); m.setAttribute('orient', 'auto')
    const p = el('path'); p.setAttribute('d', 'M0,0 L10,5 L0,10 z'); p.setAttribute('fill', COL[e.t]); m.appendChild(p); defs.appendChild(m)
    const g = el('g')
    const path = el('path'); path.setAttribute('class', 'edge' + (e.dash ? ' dash' : '')); path.setAttribute('stroke', COL[e.t]); path.setAttribute('marker-end', 'url(#' + m.id + ')')
    const tx = el('text'); tx.setAttribute('class', 'el'); tx.textContent = e.l || ''
    g.appendChild(path); g.appendChild(tx); world.appendChild(g)
    e._p = path; e._t = tx; e._g = g
    path.addEventListener('click', ev => { ev.stopPropagation(); selEdge(e) })
    edgeEls.push(e)
  })
  NODES.forEach(n => { n.ox = 0; n.oy = 0; nodeEls[n.id] = mkNode(n) })
  FIELDS.forEach(n => { n.ox = 0; n.oy = 0; n.nodeEl = mkNode(n); nodeEls[n.id] = n.nodeEl; n.nodeEl.style.display = 'none' })
  refreshAll()
}
function mkNode(n) {
  const [w, h] = SZ[n.k]
  const g = el('g'); g.setAttribute('class', 'nd' + (n.k === 'core' ? ' core' : ''))
  const bkg = el('rect'); bkg.setAttribute('class', 'bkg'); g.appendChild(bkg)
  const ic = el('text'); ic.setAttribute('class', 'ico'); g.appendChild(ic)
  const lb = el('text'); lb.setAttribute('class', 'lb'); lb.setAttribute('text-anchor', 'middle'); g.appendChild(lb)
  const sb = el('text'); sb.setAttribute('class', 'sb'); sb.setAttribute('text-anchor', 'middle'); g.appendChild(sb)
  world.appendChild(g)
  paintNode(g, n, w, h)
  g.addEventListener('mouseenter', ev => showTip(ev, n))
  g.addEventListener('mousemove', ev => moveTip(ev))
  g.addEventListener('mouseleave', () => { if (tipRef.value) tipRef.value.style.display = 'none' })
  g.addEventListener('click', ev => { if (ev.target.closest('.edge')) return; selNode(n) })
  g.addEventListener('pointerdown', ev => {
    if (ev.button !== 0) return
    ev.preventDefault(); ev.stopPropagation()
    drag = { g, n, sx: ev.clientX, sy: ev.clientY, ox: n.ox, oy: n.oy }
    g.setPointerCapture(ev.pointerId)
  })
  return g
}
function paintNode(g, n, w, h) {
  const x = n.k === 'field' ? n.fx : n.x, y = n.k === 'field' ? n.fy : n.y
  const b = g.querySelector('.bkg')
  b.setAttribute('x', x - w / 2); b.setAttribute('y', y - h / 2); b.setAttribute('width', w); b.setAttribute('height', h); b.setAttribute('rx', 10)
  const ic = g.querySelector('.ico'); ic.setAttribute('x', x - w / 2 + 13); ic.setAttribute('y', y + 4); ic.setAttribute('text-anchor', 'middle'); ic.textContent = n.icon
  const lb = g.querySelector('.lb'); lb.setAttribute('x', x + 4); lb.setAttribute('y', y - 2); lb.setAttribute('font-size', n.k === 'core' ? '13' : '11'); lb.textContent = n.label
  const sb = g.querySelector('.sb'); sb.setAttribute('x', x + 4); sb.setAttribute('y', y + 14); sb.setAttribute('text-anchor', 'middle'); sb.textContent = n.sub || ''
}
function allEdgeList() { return [...EDGES, ...FEDGES] }
function refreshAll() {
  allEdgeList().forEach(e => {
    if (e._g) e._g.style.display = (e.isF && !fieldOn) ? 'none' : ''
    const a = allNodes().find(x => x.id === e.f), b = allNodes().find(x => x.id === e.to)
    if (!a || !b || !e._p) return
    const aW = (SZ[a.k] || [0, 0])[0], bW = (SZ[b.k] || [0, 0])[0]
    const fx = cx(a) + aW / 2, tx = cx(b) - bW / 2
    e._p.setAttribute('d', `M${fx},${cy(a)} C${(fx + tx) / 2},${cy(a)} ${(fx + tx) / 2},${cy(b)} ${tx},${cy(b)}`)
    e._t.setAttribute('x', (fx + tx) / 2); e._t.setAttribute('y', Math.min(cy(a), cy(b)) - 4)
  })
}
/* 画布 pan / 节点 drag */
function canvasDown(ev) {
  if (ev.target.closest('.nd') || ev.target.closest('.zoom-col') || ev.target.closest('.legend')) return
  if (drag) return
  panning = { x: ev.clientX, y: ev.clientY, px: panX, py: panY }
}
function canvasMove(ev) {
  if (drag) {
    const dx = ev.clientX - drag.sx, dy = ev.clientY - drag.sy
    drag.n.ox = drag.ox + dx / scale; drag.n.oy = drag.oy + dy / scale
    paintNode(drag.g, drag.n, SZ[drag.n.k][0], SZ[drag.n.k][1]); refreshAll()
  } else if (panning) {
    panX = panning.px + (ev.clientX - panning.x); panY = panning.py + (ev.clientY - panning.y); applyView()
  }
}
function canvasUp() { drag = null; panning = null }
/* tip */
function tipHtml(n) {
  let h = `<b>${n.icon} ${n.label}</b><br>${KT[n.k][0]}` + (n.own ? ` · Owner ${n.own}` : '')
  if (n.k === 'core') h += `<br>${n.meta}`
  if (n.id === 'FAMT') h += '<br>⚠ 合流 3 源: #12/#21(后写覆盖)/r4 · 点击看详情'
  if (n.id === 'FPH') h += '<br>🔒 敏感 L3 · 入口: #12 直映射'
  return h
}
function showTip(ev, n) { const t = tipRef.value; t.innerHTML = tipHtml(n); t.style.display = 'block'; moveTip(ev) }
function moveTip(ev) {
  const t = tipRef.value; const r = canvasRef.value.getBoundingClientRect()
  t.style.left = ev.clientX - r.left + 14 + 'px'; t.style.top = ev.clientY - r.top + 14 + 'px'
}
/* 聚焦/字段: 字段级子图(字段边真连线) */
function focusField(id) {
  id = id || 'FAMT'
  clearFocus()
  if (!fieldOn) { fieldOn = true; FIELDS.forEach(n => { n.nodeEl.style.display = '' }); refreshAll() }
  const isPhone = id === 'FPH'
  const hitE = FEDGES.filter(e => isPhone ? e.sen : e.amt)
  if (!isPhone) {
    ['CW→R1', 'R1→DWS', 'DWS→BI', 'R1→TAG', 'TAG→S48'].forEach(k => {
      const e = EDGES.find(x => x.f + '→' + x.to === k)
      if (e && !hitE.includes(e)) hitE.push(e)
    })
  }
  allEdgeList().forEach(e => {
    const on = hitE.includes(e)
    e._p.classList.toggle('hit', on); e._p.classList.toggle('dim', !on)
    e._t.style.display = on ? '' : 'none'
  })
  const keep = isPhone ? ['CW', 'T12', 'FPH', 'LAKE']
    : ['CW', 'T12', 'T21', 'R4', 'FAMT', 'S34', 'S47', 'S48', 'R1', 'TAG', 'DWS', 'BI']
  allNodes().forEach(n => { const en = nodeEls[n.id]; en.classList.toggle('hit', keep.includes(n.id)); en.classList.toggle('dim', !keep.includes(n.id)) })
  ElMessage({ message: isPhone ? 'phone 敏感子图: #12 直映射 → 湖明文(治理项)' : 'amount 合流子图: 3 来源边 → 4 消费路径(字段级)', type: 'info', duration: 1800 })
}
function clearFocus() {
  allEdgeList().forEach(e => { e._p.classList.remove('hit', 'dim'); e._t.style.display = '' })
  allNodes().forEach(n => nodeEls[n.id].classList.remove('hit', 'dim'))
}
function toggleFields() {
  fieldOn = !fieldOn
  FIELDS.forEach(n => { n.nodeEl.style.display = fieldOn ? '' : 'none' })
  refreshAll()
}
function locate() {
  const v = q.value.trim(); if (!v) return
  const hit = allNodes().find(n => (n.label + ' ' + n.id + ' ' + (n.sub || '')).includes(v))
  if (!hit) { ElMessage.warning('未找到: ' + v); return }
  const c = canvasRef.value.getBoundingClientRect()
  scale = 1; panX = c.width / 2 - cx(hit); panY = c.height / 2 - cy(hit); applyView()
  const elNode = nodeEls[hit.id]; if (elNode) { elNode.classList.add('blink'); setTimeout(() => elNode.classList.remove('blink'), 1400) }
  if (hit.k !== 'field') selNode(hit)
}
/* 详情 */
function setDtab(i) { dtab.value = i; if (curNode) dhtml.value = pgHtml(curNode, i) }
function selNode(n) {
  curNode = n
  const dd = {
    CW: { t: '▤ customer_wide', m: '逻辑表 · 模型 M1 · DWD 客户域 · L2 · Owner 王工', tabs: ['字段表', '字段矩阵', '上下游', '操作'] },
    FAMT: { t: '字段 amount', m: '合流字段 · 3 来源 · 4 消费路径', tabs: ['合流来源', '消费路径'] },
    FPH: { t: '字段 phone', m: '敏感 🔒 L3 · mask', tabs: ['来源', '出口'] },
    R4: { t: '⚙️ 规则 r4', m: 'visual · 任务 T7 · 产出 amount', tabs: ['规则', '依赖/产出'] },
    S34: { t: '📮 订阅 #34', m: 'dwh-batch · pull 定时', tabs: ['契约', '引用列'] },
    S48: { t: '📮 订阅 #48', m: 'mkt-k · push kafka · 经标签', tabs: ['契约', '血缘路径'] },
    DWS: { t: '▤ dws_weekly', m: '派生周汇总表', tabs: ['加工', '上下游'] },
    TAG: { t: '🏷 标签 · 金额档', m: 'registry 注册标签 · 由规则 r1 产出', tabs: ['说明'] },
    BI: { t: '📊 BI 经营分析', m: 'REPORT 预留', tabs: ['说明'] },
    LAKE: { t: '⬇️ Iceberg 湖', m: '15min 快照', tabs: ['说明'] }
  }[n.id] || { t: n.label, m: (KT[n.k] || ['', ''])[0] + (n.own ? ' · Owner ' + n.own : ''), tabs: ['概要'] }
  dt.value = dd.t; dm.value = dd.m; dtabs.value = dd.tabs; dtab.value = 0
  dhtml.value = pgHtml(n, 0)
  drawer.value = true
}
function chip(t, c) { return '<span class="mini-tag" style="color:' + (c || '#fff') + ';background:' + (c ? c + '22' : '#333') + '">' + t + '</span>' }
function selEdge(e) {
  const a = NODES.find(x => x.id === e.f), b = NODES.find(x => x.id === e.to)
  dt.value = '边: ' + (a ? a.label : '') + ' → ' + (b ? b.label : '')
  dm.value = '关系 ' + e.t.toUpperCase() + ' · 解析自 field_mapping / 依赖声明 / 契约 view_json'
  dtabs.value = ['边详情']; dtab.value = 0
  dhtml.value = `<div class="kv2"><b>类型</b>${chip(e.t.toUpperCase())}</div><div class="kv2"><b>载荷</b>${e.l || '—'}</div>`
  drawer.value = true
}
function pgHtml(n, i) {
  const box = (h) => `<div style="line-height:2">${h}</div>`
  if (n.id === 'CW') {
    if (i === 0) return box('<b>amount</b> decimal L2 ⚠3源<br><b>phone</b> varchar 🔒L3 mask<br><b>id</b> bigint L1<br><b>name</b> varchar L1<br><b>region/status</b>…(共 9 列)')
    if (i === 1) return box('id ← user_id(#12 直映射)<br>phone ← phone(#12, 源敏感)<br>amount ← user_balance(#12)<br>amount ← bal(#21 合流后写覆盖)<br>amount ← net_amt−fee(r4 派生)<br>name ← org+user(#33 concat)')
    if (i === 2) return box('上游: 同步 #12/#21/#33 + 规则 r4<br>下游: 订阅 #34/#47/#48 · 湖 · BI/API/检索(预留)')
    return box('<button class="mini-btn" onclick="__focusField()">图谱聚焦 amount</button>')
  }
  if (n.id === 'FAMT') {
    if (i === 0) return box('源① 系统A #12 user_balance → amount(直映射)<br>源② 系统B #21 bal → amount(合流 · 后写覆盖)<br>源③ 规则 r4 net_amt−fee → amount(派生)')
    return box('订阅 #34(6 列视图) / #47(宽表回表)<br>长链路: → r1 金额档 → dws_weekly → BI<br>共 4 条消费路径')
  }
  if (n.id === 'FPH') return box(i === 0 ? '系统A #12: phone → phone(源敏感直映射)<br>字段级出边: → Iceberg 湖(明文, 治理项)' : '详情/预览 mask(138****1234)<br>湖快照明文(治理项)<br>契约 #50 待复核')
  if (n.id === 'TAG') return box('标签: 金额档(registry)<br>由规则 r1 产出<br>消费: 订阅 #48(kafka push · 标签矩阵圈选)<br>间接影响: 金额档实体订阅 ×3')
  if (n.id === 'R4') return box(i === 0 ? 'visual · net_amt − fee ≥ 5000 → 金额档/超限' : '依赖: net_amt, fee(已声明)<br>产出: amount + 标签「金额档」')
  if (n.id === 'S34') return box(i === 0 ? 'pull 定时 · token 生效 · 增量 _seq 游标' : '引用 6 列(amount≥5000)<br><span style="color:#fa8c16">命中 amount 变更</span>')
  if (n.id === 'S48') return box(i === 0 ? 'push kafka · 定时 5min · 标签矩阵' : 'amount → r1 金额档 → tag_fact → 圈选矩阵(间接)')
  return box(n.sub || '' + (n.own ? ' Owner: ' + n.own : ''))
}
onMounted(() => { build(); applyView() })
window.__focusField = focusField
window.__tabGraph = () => { tab.value = 'graph' }
</script>

<style scoped>
.lineage-page { min-height: calc(100vh - 120px); }
.lg-head { display: flex; align-items: center; gap: 20px; background: #fff; border: 1px solid var(--el-border-color-lighter); border-radius: 8px; padding: 8px 14px; margin-bottom: 10px; flex-wrap: wrap; }
.lg-title { font-size: 15px; font-weight: 700; }
.lg-nav { display: flex; gap: 2px; margin-left: 6px; }
.lg-nav-item { padding: 6px 14px; cursor: pointer; color: #666; border-radius: 6px; font-size: 13px; }
.lg-nav-item:hover { background: #f5f7fa; }
.lg-nav-item.on { background: #e6f4ff; color: #1677ff; font-weight: 600; }
.page-card { background: #fff; border: 1px solid var(--el-border-color-lighter); border-radius: 8px; padding: 16px 18px; }
.pg-title { font-size: 16px; font-weight: 700; }
.dim2 { color: #999; font-size: 12px; }
.flex { display: flex; align-items: center; }
/* 图谱 */
.graph-shell { border-radius: 10px; overflow: hidden; border: 1px solid #1e2a3d; }
.gbar { background: #0d1626; padding: 8px 12px; display: flex; gap: 8px; align-items: center; flex-wrap: wrap; }
.gbar :deep(.el-input__wrapper) { background: #12203a; box-shadow: none; border: 1px solid #2a3b56; }
.gbar :deep(.el-input__inner) { color: #e6edf5; }
.gstats { margin-left: auto; color: #5a6a7d; font-size: 11px; }
.canvas { position: relative; height: 640px; background: radial-gradient(ellipse at 30% 20%, #12202f 0%, #0b1420 60%); overflow: hidden; cursor: default; }
.canvas svg { width: 100%; height: 100%; display: block; }
.zoom-col { position: absolute; right: 12px; bottom: 90px; display: flex; flex-direction: column; gap: 4px; }
.zoom-col .el-button { width: 30px; padding: 0; background: #0d1626; border-color: #2a3b56; color: #cfe3ff; }
.legend { position: absolute; left: 10px; bottom: 10px; background: rgba(13, 22, 38, .92); border: 1px solid #2a3b56; border-radius: 8px; padding: 7px 10px; font-size: 11px; color: #8b98ab; line-height: 1.8; }
.legend .lg-row { display: flex; align-items: center; gap: 6px; }
.sw { width: 20px; border-top: 2px solid; display: inline-block; }
.hint { margin-top: 4px; border-top: 1px solid #1e2a3d; padding-top: 4px; color: #5a6a7d; }
.g-tip { position: absolute; display: none; background: rgba(13, 22, 38, .95); border: 1px solid #2a3b56; border-radius: 8px; padding: 7px 10px; font-size: 12px; pointer-events: none; z-index: 30; max-width: 300px; }
.dm { color: #888; font-size: 12px; margin-bottom: 8px; }
.dtabs { display: flex; border-bottom: 1px solid var(--el-border-color-lighter); margin-bottom: 8px; }
.dtab { padding: 6px 14px; cursor: pointer; color: #666; font-size: 13px; border-bottom: 2px solid transparent; }
.dtab.on { color: #1677ff; border-bottom-color: #1677ff; font-weight: 600; }
.dbody { font-size: 13px; }
.kv2 { display: flex; margin: 6px 0; font-size: 13px; } .kv2 b { width: 60px; color: #999; font-weight: 400; }
:deep(.mini-btn) { background: #1677ff; color: #fff; border: none; border-radius: 5px; padding: 4px 12px; cursor: pointer; font-size: 12px; margin-top: 6px; }
:deep(.mini-tag) { display: inline-block; padding: 0 7px; border-radius: 3px; font-size: 11px; line-height: 18px; }
/* 影响 */
.imp-sum { background: #fff7e6; border: 1px solid #ffd591; border-radius: 8px; padding: 9px 13px; font-size: 13px; color: #874d00; margin-bottom: 10px; line-height: 1.8; }
.imp-cols { display: grid; grid-template-columns: 1.5fr 1fr; gap: 12px; }
.imp-lv { display: flex; gap: 8px; margin: 8px 0; }
.imp-lv .lv { width: 84px; flex-shrink: 0; color: #999; font-size: 12px; padding-top: 6px; }
.lv-body { flex: 1; }
.ic { border: 1px solid var(--el-border-color-lighter); border-left: 3px solid #1677ff; border-radius: 6px; padding: 6px 10px; margin-bottom: 6px; background: #fafbfc; }
.ic .i1 { font-size: 13px; font-weight: 600; display: flex; gap: 6px; align-items: center; }
.ic .i2 { font-size: 12px; color: #888; margin-top: 2px; }
.act-card { border: 1px solid var(--el-border-color-lighter); border-radius: 8px; padding: 12px 14px; height: fit-content; }
/* 溯源 */
.tr-final { border: 1px solid #b7eb8f; background: #f6ffed; border-radius: 8px; padding: 8px 12px; font-size: 13px; margin-bottom: 10px; color: #237804; }
.tr-card { border: 1px solid var(--el-border-color-lighter); border-radius: 8px; margin-bottom: 8px; overflow-x: auto; }
.tr-title { background: #fafafa; padding: 6px 10px; font-size: 12px; color: #666; border-bottom: 1px solid var(--el-border-color-lighter); }
.tr-steps { display: flex; align-items: center; gap: 4px; padding: 10px; flex-wrap: wrap; }
.tr-node { border: 1px solid #adc6ff; background: #f0f5ff; color: #0b5bd3; border-radius: 6px; padding: 3px 9px; font-size: 12px; white-space: nowrap; }
.tr-node.src { border-color: #ffe58f; background: #fffbe6; color: #ad6800; }
.tr-node.tr { border-style: dashed; border-color: #d3adf7; background: #f9f0ff; color: #722ed1; }
.tr-node.cur { border-color: #1677ff; color: #fff; background: #1677ff; font-weight: 600; }
/* 通知 */
.notice-card { border: 1px solid var(--el-border-color-lighter); border-left: 4px solid #fa8c16; border-radius: 10px; padding: 14px 16px; line-height: 1.9; background: #fff; }
.nt { font-size: 15px; font-weight: 700; margin-bottom: 8px; }
.kv { display: flex; font-size: 13px; } .kv b { width: 110px; color: #999; font-weight: 400; }
.split { border-top: 1px dashed var(--el-border-color); margin: 8px 0; }
.tree { font-size: 13px; line-height: 2; }
/* svg 节点(非 scoped 深色容器内) */
:deep(.nd) { cursor: grab; }
:deep(.nd .bkg) { fill: #16223a; stroke: #2a3b56; stroke-width: 1; }
:deep(.nd.core .bkg) { fill: rgba(34, 211, 238, .09); stroke: #22d3ee; stroke-width: 1.7; }
:deep(.nd .lb) { fill: #dbe7f5; font-size: 11px; font-weight: 600; }
:deep(.nd.core .lb) { fill: #7ce7f7; font-size: 13px; }
:deep(.nd .sb) { fill: #6b7d94; font-size: 9.5px; }
:deep(.nd.core .sb) { fill: #9ae8f5; }
:deep(.nd .ico) { font-size: 13px; }
:deep(.nd.dim) { opacity: .14; }
:deep(.nd.hit .bkg) { stroke: #fde68a; stroke-width: 2.4; }
:deep(.nd.blink .bkg) { stroke: #fff; stroke-width: 2.5; }
:deep(.edge) { fill: none; stroke-width: 1.7; opacity: .8; }
:deep(.edge.dim) { opacity: .05; }
:deep(.edge.hit) { stroke-width: 3.2; opacity: 1; }
:deep(.edge.dash) { stroke-dasharray: 5 4; }
.mono2 { font-family: ui-monospace, Menlo, Consolas, monospace; }
:deep(.el) { fill: #5a6a7d; font-size: 9.5px; pointer-events: none; }
</style>
