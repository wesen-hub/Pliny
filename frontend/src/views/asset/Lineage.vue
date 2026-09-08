<template>
  <div class="lineage-page">
    <div class="lg-head">
      <div class="lg-title">⛓ 数据血缘分析
        <el-tag size="small" effect="plain" style="margin-left:8px" :type="loaded ? 'success' : 'warning'">{{ loaded ? '已连接 Neo4j 真实图谱' : '加载中/未连接' }}</el-tag>
      </div>
      <div class="lg-nav">
        <div v-for="t in tabs" :key="t.k" class="lg-nav-item" :class="{ on: tab === t.k }" @click="tab = t.k">{{ t.t }}</div>
      </div>
      <div style="margin-left:auto;font-size:12px;color:#999" v-if="loaded">表 {{ statN('Table') }} · 字段 {{ statN('Field') }} · 任务 {{ statN('Job') }} · 规则 {{ statN('Rule') }} · 标签 {{ statN('Tag') }} · 应用 {{ statN('App') }} · 边 {{ L.edges.length }}</div>
    </div>

    <!-- ============ ① 血缘图谱(真实数据) ============ -->
    <div v-show="tab === 'graph'" class="lg-view">
      <div class="graph-shell" v-loading="loading">
        <div class="gbar">
          <el-input v-model="q" size="small" placeholder="检索节点: 表/任务/规则/契约…" style="width:280px" clearable @keyup.enter="locate()">
            <template #prefix>🔍</template>
          </el-input>
          <el-button size="small" type="primary" plain @click="locate()">定位</el-button>
          <el-button size="small" @click="reload(true)">重建血缘图</el-button>
          <el-button size="small" @click="fitAll()">适应全图</el-button>
          <span class="gstats">拖节点调位 · 滚轮缩放 · 拖空白平移 · 悬停摘要 · 点击详情(字段/映射见右侧与矩阵页)</span>
        </div>
        <div class="canvas" ref="canvasRef" @pointerdown="canvasDown" @pointermove="canvasMove" @pointerup="canvasUp" @wheel.prevent="onWheel">
          <svg ref="svgRef" viewBox="0 0 1500 860"></svg>
          <div class="zoom-col">
            <el-button size="small" @click.stop="zoomBy(1.2)">＋</el-button>
            <el-button size="small" @click.stop="zoomBy(1 / 1.2)">－</el-button>
            <el-button size="small" @click.stop="fitAll()">⤢</el-button>
          </div>
          <div class="legend">
            <div style="font-weight:600;color:#cfe3ff">图例</div>
            <div class="lg-row"><span class="sq" style="background:#1e3a5f;border:1px solid #22d3ee"></span>逻辑表(核心)</div>
            <div class="lg-row"><span class="sq" style="background:#f59e0b55"></span>源系统 / 同步任务</div>
            <div class="lg-row"><span class="sq" style="background:#c084fc55"></span>规则 / 标签</div>
            <div class="lg-row"><span class="sq" style="background:#34d39955"></span>数据应用(订阅)</div>
            <div class="hint">字段级({{ statN('Field') }})进详情/矩阵页 · 深色线=同步 · 紫=规则/标签 · 绿=订阅消费</div>
          </div>
          <div class="g-tip" ref="tipRef"></div>
        </div>
      </div>

      <el-drawer v-model="drawer" :title="dt" size="460px">
        <div class="dm">{{ dm }}</div>
        <div class="dbody" v-html="dhtml"></div>
      </el-drawer>
    </div>

    <!-- ============ ② 字段映射矩阵(真实) ============ -->
    <div v-show="tab === 'matrix'" class="lg-view page-card">
      <div class="pg-title">字段级血缘矩阵<el-tag size="small" effect="plain" style="margin-left:8px">一行 = 一条来源边(Neo4j FIELD_MAPPING/DERIVED)</el-tag></div>
      <div class="flex" style="gap:8px;margin:8px 0">
        <span class="dim2">逻辑表:</span>
        <el-select v-model="mTable" size="small" style="width:260px" filterable @change="refreshMatrix">
          <el-option v-for="t in tables" :key="t.id" :label="t.label" :value="t.id" />
        </el-select>
        <span class="dim2" v-if="mRows.length">字段 {{ mFields().length }} · 来源边 {{ mRows.length }}(多源合流=同字段多行)</span>
      </div>
      <el-table :data="mRows" size="small" border max-height="480">
        <el-table-column label="目标字段" width="150"><template #default="{ row }"><b>{{ row.target }}</b></template></el-table-column>
        <el-table-column label="来源(任务/规则)" min-width="170"><template #default="{ row }">{{ row.job }}</template></el-table-column>
        <el-table-column label="源字段" min-width="150"><template #default="{ row }"><span class="mono2">{{ row.sourceField || '—' }}</span></template></el-table-column>
        <el-table-column label="转换" width="120"><template #default="{ row }"><el-tag size="small" effect="plain" :type="row.derived ? 'danger' : 'primary'">{{ row.transform || '—' }}</el-tag></template></el-table-column>
        <el-table-column prop="edgeType" label="边类型" width="130" />
      </el-table>
      <div class="dim2" style="margin-top:6px" v-if="!mRows.length">该表暂无已登记的字段级映射边(字段见详情; 同步/派生任务登记后自动出现)</div>
    </div>

    <!-- ============ ③ 影响分析(真实路径) ============ -->
    <div v-show="tab === 'impact'" class="lg-view page-card">
      <div class="pg-title">影响分析<el-tag size="small" effect="plain" style="margin-left:8px">真实图路径集(双向可达, 按端点分类)</el-tag></div>
      <div class="flex" style="gap:8px;margin:10px 0">
        <span class="dim2">变更对象(表):</span>
        <el-select v-model="iTable" size="small" style="width:260px" filterable @change="loadImpact">
          <el-option v-for="t in tables" :key="t.id" :label="t.label" :value="t.id" />
        </el-select>
        <el-button size="small" type="primary" @click="loadImpact">分析影响</el-button>
        <span class="dim2" v-if="impact.pathCount">共 {{ impact.pathCount }} 条路径</span>
      </div>
      <div v-if="impact.pathCount" class="imp-sum" style="margin-bottom:10px">
        ⚠ 可达下游分类: 订阅应用 <b>{{ impact.counts.App || 0 }}</b> · 标签 <b>{{ impact.counts.Tag || 0 }}</b> · 规则 <b>{{ impact.counts.Rule || 0 }}</b> · 任务 <b>{{ impact.counts.Job || 0 }}</b> · 逻辑表 <b>{{ impact.counts.Table || 0 }}</b>
      </div>
      <div style="max-height:520px;overflow:auto">
        <div v-for="(p, i) in impact.paths" :key="i" class="ic" style="border-left-color:#1677ff">
          <div class="i2">
            <el-tag v-for="(n, j) in p.nodes" :key="j" size="small" effect="plain" :type="ntype(n.type)" style="margin:1px">{{ n.label }}</el-tag>
            <span v-if="p.edgeTypes" class="dim2" style="margin-left:6px">{{ p.edgeTypes }}</span>
          </div>
        </div>
      </div>
    </div>

    <!-- ============ ④ 溯源分析(真实) ============ -->
    <div v-show="tab === 'trace'" class="lg-view page-card">
      <div class="pg-title">溯源分析<el-tag size="small" effect="plain" style="margin-left:8px">真实图 · 最终来源 + 全路径集</el-tag></div>
      <div class="flex" style="gap:8px;margin:10px 0">
        <span class="dim2">逻辑表:</span>
        <el-select v-model="tTable" size="small" style="width:220px" filterable @change="refreshTrace">
          <el-option v-for="t in tables" :key="t.id" :label="t.label" :value="t.id" />
        </el-select>
        <span class="dim2">字段:</span>
        <el-select v-model="tField" size="small" style="width:200px" filterable @change="loadTrace">
          <el-option v-for="f in mFields()" :key="f" :label="f" :value="f" />
        </el-select>
        <el-button size="small" type="primary" @click="loadTrace">溯源</el-button>
      </div>
      <div v-if="trace.up && trace.up.length">
        <div class="sec">▲ 来源路径({{ trace.up.length }})</div>
        <div v-for="(p, i) in trace.up.slice(0, 12)" :key="'u' + i" class="tr-card">
          <div class="tr-steps" style="flex-wrap:wrap">
            <el-tag v-for="(n, j) in p.nodes" :key="j" size="small" effect="plain" :type="ntype(n.type)" style="margin:1px">{{ n.label }}</el-tag>
          </div>
        </div>
      </div>
      <div v-if="trace.down && trace.down.length">
        <div class="sec" style="margin-top:12px">▼ 消费路径({{ trace.down.length }})</div>
        <div v-for="(p, i) in trace.down.slice(0, 12)" :key="'d' + i" class="tr-card">
          <div class="tr-steps" style="flex-wrap:wrap">
            <el-tag v-for="(n, j) in p.nodes" :key="j" size="small" effect="plain" :type="ntype(n.type)" style="margin:1px">{{ n.label }}</el-tag>
          </div>
        </div>
      </div>
      <el-empty v-if="loaded && !trace.up && !trace.down" description="选择字段后查看来源/消费链路" :image-size="60" />
    </div>

    <!-- ============ ⑤ 变更通知(原型) ============ -->
    <div v-show="tab === 'notices'" class="lg-view page-card">
      <div class="pg-title">变更影响通知<el-tag size="small" effect="plain" style="margin-left:8px">前端原型 · 服务端 P2</el-tag></div>
      <div class="dim2" style="margin:8px 0">由影响分析(本页/图谱)触发 · 落库+审计+回执(外发留扩展)。演示单:</div>
      <div class="notice-card">
        <div class="nt">📋 数据变更影响通知</div>
        <div class="kv"><b>变更资产</b>customer_wide.amount(逻辑表)</div>
        <div class="kv"><b>变更类型</b>字段语义变更(演示)</div>
        <div class="kv"><b>受影响</b>沿真实血缘可达: 订阅/标签/规则见「影响分析」</div>
        <div style="margin-top:10px"><el-button size="small" type="primary">确认收到(回执)</el-button></div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, reactive, computed, watch, onMounted } from 'vue'
import { ElMessage } from 'element-plus'
import request from '../../api/request'

const tabs = [
  { k: 'graph', t: '⛓ 血缘图谱' }, { k: 'matrix', t: '字段映射矩阵' },
  { k: 'impact', t: '影响分析' }, { k: 'trace', t: '溯源分析' }, { k: 'notices', t: '变更影响通知' }
]
const tab = ref('graph')
const q = ref('')
const loading = ref(false)
const loaded = ref(false)
const L = reactive({ nodes: [], edges: [] })
const byId = computed(() => { const m = {}; L.nodes.forEach(n => { m[n.id] = n }); return m })
const parentT = computed(() => { const m = {}; L.edges.forEach(e => { if (e.type === 'CONTAINS') m[e.to] = e.from }); return m })
const tables = computed(() => L.nodes.filter(n => n.type === 'Table'))
const statN = (t) => L.nodes.filter(n => n.type === t).length
function ntype(t) { return ({ Source: 'info', Job: 'warning', Table: 'primary', Rule: 'danger', Tag: 'warning', App: 'success' })[t] || 'info' }
const tlabel = (id) => (byId.value[id] && byId.value[id].label) || id

/* ============ 图谱渲染 ============ */
const svgRef = ref(null), canvasRef = ref(null), tipRef = ref(null)
const NS = 'http://www.w3.org/2000/svg'
let world = null, vis = [], scale = 1, panX = 0, panY = 0, drag = null, panning = null, curSel = null
const drawer = ref(false), dt = ref(''), dm = ref(''), dhtml = ref('')
const SZ = { Source: [168, 44], Job: [200, 44], Table: [210, 56], Rule: [168, 44], Tag: [168, 40], App: [210, 48] }
const COL = { Source: '#f59e0b', Job: '#38bdf8', Table: '#22d3ee', Rule: '#c084fc', Tag: '#f472b6', App: '#34d399' }
const KN = { Source: '源系统', Job: '数据任务', Table: '逻辑表', Rule: '规则', Tag: '标签', App: '订阅应用' }

function el(t, c) { const n = document.createElementNS(NS, t); if (c) n.setAttribute('class', c); return n }
async function reload(rebuild) {
  loading.value = true
  try {
    if (rebuild) await request.post('/lineage/rebuild', {})
    const d = await request.get('/lineage/graph')
    L.nodes = d.nodes || []; L.edges = d.edges || []
    loaded.value = true
    const tbs = L.nodes.filter(n => n.type === 'Table')
    if (tbs.length && !mTable.value) { mTable.value = tbs[0].id; iTable.value = tbs[0].id; tTable.value = tbs[0].id }
    await nextTickSafe()
    buildGraph()
    ElMessage.success('血缘图已加载: ' + L.nodes.length + ' 节点 / ' + L.edges.length + ' 边')
  } catch (e) {
    ElMessage.error('血缘图加载失败(Neo4j 未就绪?): ' + (e.message || e))
  } finally { loading.value = false }
}
function nextTickSafe() { return new Promise(r => setTimeout(r, 50)) }

/* 主图: 去掉 Field 与 CONTAINS; 字段端点提升到父表; 同对多边合并 */
function buildGraph() {
  const svg = svgRef.value
  if (!svg) return
  while (svg.firstChild) svg.removeChild(svg.firstChild)
  world = el('g'); svg.appendChild(world)
  const parent = parentT.value
  // 可见节点(排除 Field)
  vis = L.nodes.filter(n => n.type !== 'Field')
  // 布局: 列分区
  const colOf = (n) => n.type === 'Source' || n.type === 'Job' ? 0 : n.type === 'Table' ? 1 : (n.type === 'Rule' || n.type === 'Tag') ? 2 : 3
  const X = { 0: 130, 1: 430, 2: 740, 3: 1060 }
  const cols = [[], [], [], []]
  vis.forEach(n => cols[colOf(n)].push(n))
  cols.forEach((cs, ci) => {
    const n = cs.length, gap = n > 1 ? Math.min(64, (700 - 80) / (n - 1)) : 0
    const y0 = n > 1 ? 70 + ((700 - (n - 1) * gap)) / 2 - 0 : 400
    cs.forEach((nd, i) => {
      nd.x = X[ci] + (Math.random() - 0.5) * 8
      nd.y = n > 1 ? y0 + i * gap : 400 + (ci - 1.5) * 30
    })
  })
  // 边: 提升字段端点
  const es = []
  L.edges.forEach(e => {
    if (e.type === 'CONTAINS') return
    let f = e.f, t = e.to
    if (byId.value[f] && byId.value[f].type === 'Field') f = parent[f] || f
    if (byId.value[t] && byId.value[t].type === 'Field') t = parent[t] || t
    if (!byId.value[f] || !byId.value[t] || f === t) return
    const k = f + '|' + t + '|' + e.type
    const ex = es.find(x => x.k === k)
    if (ex) { ex.n++ } else { es.push({ k, f, t, type: e.type, n: 1 }) }
  })
  // defs 箭头
  const defs = el('defs'); world.appendChild(defs)
  const ECOL = { SOURCE: '#38bdf8', SOURCE_OF: '#38bdf8', DERIVED_FROM: '#c084fc', USES_RULE: '#c084fc', PRODUCES: '#f472b6', DEPENDS_ON: '#f472b6', CONSUMES: '#34d399', FIELD_MAPPING: '#64748b' }
  let ai = 0
  es.forEach(e => {
    const m = el('marker'); m.id = 'lm' + ai++; m.setAttribute('viewBox', '0 0 10 10'); m.setAttribute('refX', '9'); m.setAttribute('refY', '5')
    m.setAttribute('markerWidth', '6'); m.setAttribute('markerHeight', '6'); m.setAttribute('orient', 'auto')
    const p = el('path'); p.setAttribute('d', 'M0,0 L10,5 L0,10 z'); p.setAttribute('fill', ECOL[e.type] || '#888')
    m.appendChild(p); defs.appendChild(m)
    const g = el('g')
    const path = el('path'); path.setAttribute('class', 'edge'); path.setAttribute('stroke', ECOL[e.type] || '#888'); path.setAttribute('marker-end', 'url(#' + m.id + ')')
    const tx = el('text'); tx.setAttribute('class', 'el'); tx.textContent = e.type + (e.n > 1 ? '×' + e.n : '')
    g.appendChild(path); g.appendChild(tx); world.appendChild(g)
    e._p = path; e._t = tx
  })
  // 节点卡
  vis.forEach(n => {
    const [w, h] = SZ[n.type] || [160, 44]
    const g = el('g'); g.setAttribute('class', 'nd' + (n.type === 'Table' ? ' core' : ''))
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
      drag = { g, n, sx: ev.clientX, sy: ev.clientY }
      g.setPointerCapture(ev.pointerId)
    })
    n._g = g
  })
  refreshEdges()
  fitAll()
}
function paintNode(g, n, w, h) {
  const b = g.querySelector('.bkg')
  b.setAttribute('x', n.x - w / 2); b.setAttribute('y', n.y - h / 2); b.setAttribute('width', w); b.setAttribute('height', h); b.setAttribute('rx', 10)
  const ic = g.querySelector('.ico'); ic.setAttribute('x', n.x - w / 2 + 14); ic.setAttribute('y', n.y + 4); ic.setAttribute('text-anchor', 'middle')
  ic.textContent = { Source: '🗄', Job: '⇄', Table: '▤', Rule: '⚙️', Tag: '🏷', App: '📮' }[n.type] || '•'
  const lb = g.querySelector('.lb'); lb.setAttribute('x', n.x + 6); lb.setAttribute('y', n.y - 2); lb.setAttribute('font-size', n.type === 'Table' ? '13' : '11'); lb.textContent = (n.label || n.id).slice(0, 22)
  const sb = g.querySelector('.sb'); sb.setAttribute('x', n.x + 6); sb.setAttribute('y', n.y + 14); sb.setAttribute('text-anchor', 'middle')
  sb.textContent = subOf(n)
}
function subOf(n) {
  if (n.type === 'Table') return (n.layer ? n.layer + ' · ' : '') + (n.kind || '') + (n.domain ? ' · ' + n.domain : '')
  if (n.type === 'Job') return n.syncMode || n.name || ''
  if (n.type === 'App') return n.subscriber || ''
  if (n.type === 'Source') return n.dbType || ''
  return ''
}
function refreshEdges() { redrawEdges() }
let curEdges = []
function redrawEdges() {
  curEdges = []
  L.edges.forEach(e => { if (e.type === 'CONTAINS') return; let f = e.f, t = e.to; const parent = parentT.value
    if (byId.value[f] && byId.value[f].type === 'Field') f = parent[f] || f
    if (byId.value[t] && byId.value[t].type === 'Field') t = parent[t] || t
    if (!byId.value[f] || !byId.value[t] || f === t) return
    const k = f + '|' + t + '|' + e.type
    if (!curEdges.find(x => x.k === k)) curEdges.push({ k, f, t, type: e.type, n: 1 }); else curEdges.find(x => x.k === k).n++
  })
  const svg = svgRef.value
  const paths = [...svg.querySelectorAll('path.edge')], texts = [...svg.querySelectorAll('text.el')]
  curEdges.forEach((e, i) => {
    const a = byId.value[e.f], b = byId.value[e.t]
    if (!a || !b || !paths[i]) return
    const aW = (SZ[a.type] || [160, 44])[0] / 2, bW = (SZ[b.type] || [160, 44])[0] / 2
    const fx = (a.x + (a.ox || 0)) + aW, tx = (b.x + (b.ox || 0)) - bW
    paths[i].setAttribute('d', `M${fx},${a.y + (a.oy || 0)} C${(fx + tx) / 2},${a.y + (a.oy || 0)} ${(fx + tx) / 2},${b.y + (b.oy || 0)} ${tx},${b.y + (b.oy || 0)}`)
    texts[i].setAttribute('x', (fx + tx) / 2); texts[i].setAttribute('y', Math.min(a.y + (a.oy || 0), b.y + (b.oy || 0)) - 4)
    texts[i].textContent = e.type + (e.n > 1 ? '×' + e.n : '')
  })
}
/* 视图操作 */
function applyView() { world.setAttribute('transform', `translate(${panX},${panY}) scale(${scale})`) }
function zoomBy(f) { scale = Math.min(2.2, Math.max(0.4, scale * f)); applyView() }
function fitAll() { scale = 1; panX = 0; panY = 0; applyView() }
function onWheel(e) { zoomBy(e.deltaY < 0 ? 1.15 : 1 / 1.15) }
function canvasDown(ev) {
  if (ev.target.closest('.nd') || ev.target.closest('.zoom-col') || ev.target.closest('.legend') || drag) return
  panning = { x: ev.clientX, y: ev.clientY, px: panX, py: panY }
}
function canvasMove(ev) {
  if (drag) {
    const dx = ev.clientX - drag.sx, dy = ev.clientY - drag.sy
    drag.n.ox = (drag.n.ox || 0) + dx / scale; drag.n.oy = (drag.n.oy || 0) + dy / scale
    paintNode(drag.g, drag.n, SZ[drag.n.type][0], SZ[drag.n.type][1])
    const svg = svgRef.value
    const paths = [...svg.querySelectorAll('path.edge')], texts = [...svg.querySelectorAll('text.el')]
    curEdges.forEach((e, i) => {
      const a = byId.value[e.f], b = byId.value[e.t]
      if (!a || !b || !paths[i]) return
      const aW = SZ[a.type][0] / 2, bW = SZ[b.type][0] / 2
      const fx = a.x + a.ox + aW, tx = b.x + b.ox - bW
      paths[i].setAttribute('d', `M${fx},${a.y + a.oy} C${(fx + tx) / 2},${a.y + a.oy} ${(fx + tx) / 2},${b.y + b.oy} ${tx},${b.y + b.oy}`)
      texts[i].setAttribute('x', (fx + tx) / 2)
    })
  } else if (panning) {
    panX = panning.px + (ev.clientX - panning.x); panY = panning.py + (ev.clientY - panning.y); applyView()
  }
}
function canvasUp() { drag = null; panning = null }
function showTip(ev, n) {
  const t = tipRef.value
  let h = `<b>${(n.label || n.id).slice(0, 40)}</b><br>${KN[n.type] || n.type}`
  if (n.type === 'Table') h += `<br>${n.layer || ''} ${n.kind || ''} ${n.domain || ''} · 字段见详情`
  if (n.type === 'App') h += n.subscriber ? `<br>订阅方: ${n.subscriber}` : ''
  if (n.type === 'Source') h += n.dbType ? `<br>类型: ${n.dbType}` : ''
  t.innerHTML = h; t.style.display = 'block'; moveTip(ev)
}
function moveTip(ev) {
  const t = tipRef.value, r = canvasRef.value.getBoundingClientRect()
  t.style.left = ev.clientX - r.left + 14 + 'px'; t.style.top = ev.clientY - r.top + 14 + 'px'
}
function locate() {
  const v = q.value.trim(); if (!v) return
  const hit = vis.find(n => ((n.label || '') + ' ' + n.id).includes(v))
  if (!hit) { ElMessage.warning('未找到: ' + v); return }
  const c = canvasRef.value.getBoundingClientRect()
  scale = 1; panX = c.width / 2 - hit.x; panY = c.height / 2 - hit.y; applyView()
  if (hit._g) { hit._g.classList.add('blink'); setTimeout(() => hit._g.classList.remove('blink'), 1400) }
  selNode(hit)
}
/* ============ 节点详情(真实聚合) ============ */
function fnameOf(fid) { return (byId.value[fid] || {}).label || fid }
function selNode(n) {
  curSel = n
  dt.value = (n.label || n.id)
  dm.value = `${KN[n.type] || n.type}${n.subscriber ? ' · 订阅方 ' + n.subscriber : ''}${n.layer ? ' · ' + n.layer + '/' + n.kind : ''}`
  let h = ''
  if (n.type === 'Table') {
    const fields = L.edges.filter(e => e.type === 'CONTAINS' && e.from === n.id).map(e => fnameOf(e.to))
    const mapEdges = L.edges.filter(e => (e.type === 'FIELD_MAPPING') && fields.includes(fnameOf(e.to)))
    const upJobs = L.edges.filter(e => (e.type === 'SOURCE_OF' || e.type === 'DERIVED_FROM') && e.to === n.id).map(e => tlabel(e.f))
    const consumers = L.edges.filter(e => e.type === 'CONSUMES' && (e.to === n.id)).map(e => tlabel(e.f))
    const rules = L.edges.filter(e => e.type === 'DEPENDS_ON' && e.to.startsWith('field:' + n.id.split(':')[1] + '.')).map(e => tlabel(e.f))
    h = `<div style="font-weight:600">字段(${fields.length})</div><div style="line-height:1.9">${fields.slice(0, 40).join(' · ') || '—'}</div>
      <div style="font-weight:600;margin-top:8px">上游任务/同步</div><div style="line-height:1.9">${upJobs.join(' · ') || '无登记同步任务'}</div>
      <div style="font-weight:600;margin-top:8px">订阅消费方</div><div style="line-height:1.9">${consumers.join(' · ') || '—'}</div>
      ${rules.length ? `<div style="font-weight:600;margin-top:8px">依赖本表字段的规则</div><div>${rules.join(' · ')}</div>` : ''}
      ${mapEdges.length ? `<div style="font-weight:600;margin-top:8px">字段映射边(详见矩阵页)</div>` : ''}`
  } else if (n.type === 'Job') {
    const dst = L.edges.filter(e => (e.type === 'SOURCE_OF' || e.type === 'DERIVED_FROM') && e.from === n.id).map(e => tlabel(e.to))
    const src = L.edges.filter(e => e.type === 'SOURCE' && e.to === n.id).map(e => tlabel(e.f))
    const rules = L.edges.filter(e => e.type === 'USES_RULE' && e.from === n.id).map(e => tlabel(e.to))
    h = `目标表: ${dst.join(' · ') || '—'}<br>源系统: ${src.join(' · ') || '—'}<br>执行规则: ${rules.join(' · ') || '—'}`
  } else if (n.type === 'Rule') {
    const deps = L.edges.filter(e => e.type === 'DEPENDS_ON' && e.from === n.id).map(e => fnameOf(e.to))
    const tags = L.edges.filter(e => e.type === 'PRODUCES' && e.from === n.id).map(e => fnameOf(e.to))
    const jobs = L.edges.filter(e => e.type === 'USES_RULE' && e.to === n.id).map(e => tlabel(e.f))
    h = `依赖字段: ${deps.join(' · ') || '—'}<br>产出标签: ${tags.join(' · ') || '—'}<br>被任务使用: ${jobs.join(' · ') || '—'}`
  } else if (n.type === 'Tag') {
    const from = L.edges.filter(e => e.type === 'PRODUCES' && e.to === n.id).map(e => tlabel(e.f))
    const apps = L.edges.filter(e => e.type === 'CONSUMES' && e.to === n.id).map(e => tlabel(e.f))
    h = `产出规则: ${from.join(' · ') || '—'}<br>圈选订阅: ${apps.join(' · ') || '—'}`
  } else if (n.type === 'App') {
    const tgt = L.edges.filter(e => e.type === 'CONSUMES' && e.from === n.id).map(e => fnameOf(e.to))
    h = `消费: ${tgt.join(' · ') || '—'}`
  } else h = n.id
  dhtml.value = '<div style="line-height:2;font-size:13px">' + h + '</div>'
  drawer.value = true
}
/* ============ 矩阵(真实) ============ */
const mTable = ref(''); const mRows = ref([])
function refreshMatrix() { if (!mTable.value) return; const tn = mTable.value.split(':')[1]; const fields = mFields()
  const rows = []
  L.edges.forEach(e => {
    if (e.type !== 'FIELD_MAPPING') return
    const toF = byId.value[e.to]
    if (!toF || toF.type !== 'Field' || !toF.label || !fields.includes(toF.label)) return
    if ((toF.label || '').startsWith('_')) return
    const from = byId.value[e.f]
    rows.push({ target: toF.label, job: (from ? from.label : e.f), sourceField: e.sourceField || '', transform: e.transform || '', derived: (e.transform || '').startsWith('derived'), edgeType: 'FIELD_MAPPING' })
  })
  mRows.value = rows
}
function mFields() {
  const tn = (mTable.value || '').split(':')[1]
  return L.edges.filter(e => e.type === 'CONTAINS' && e.from === 'table:' + tn).map(e => fnameOf(e.to)).filter(f => !f.startsWith('_'))
}
/* ============ 影响(真实) ============ */
const iTable = ref(''); const impact = reactive({ paths: [], pathCount: 0, counts: {} })
async function loadImpact() {
  if (!iTable.value) return
  const tn = iTable.value.split(':')[1]
  try {
    const d = await request.get('/lineage/impact', { params: { table: tn, depth: 4 } })
    const paths = (d.paths || []).slice(0, 60)
    const counts = {}
    paths.forEach(p => {
      const last = p.nodes[p.nodes.length - 1]
      counts[last.type] = (counts[last.type] || 0) + 1
      p.edgeTypes = (p.edges || []).map(x => x.type).filter((v, i, a) => a.indexOf(v) === i).join('/')
    })
    impact.paths = paths; impact.pathCount = d.pathCount || 0; impact.counts = counts
  } catch (e) { ElMessage.error('影响分析失败: ' + (e.message || e)) }
}
/* ============ 溯源(真实) ============ */
const tTable = ref(''); const tField = ref(''); const trace = reactive({ up: [], down: [] })
function refreshTrace() { tField.value = ''; trace.up = []; trace.down = [] }
async function loadTrace() {
  if (!tTable.value || !tField.value) return
  const tn = tTable.value.split(':')[1]
  try {
    const d = await request.get(`/lineage/field/${tn}/${tField.value}/trace`)
    trace.up = (d.upstream || []).slice(0, 12)
    trace.down = (d.downstream || []).slice(0, 12)
  } catch (e) { ElMessage.error('溯源失败: ' + (e.message || e)) }
}
watch(mTable, refreshMatrix)
watch(tTable, refreshTrace)
onMounted(() => { reload(false); if (tables.value.length) { mTable.value = tables.value[0].id; iTable.value = tables.value[0].id; tTable.value = tables.value[0].id } })
</script>

<style scoped>
.lineage-page { min-height: calc(100vh - 120px); }
.lg-head { display: flex; align-items: center; gap: 14px; background: #fff; border: 1px solid var(--el-border-color-lighter); border-radius: 8px; padding: 8px 14px; margin-bottom: 10px; flex-wrap: wrap; }
.lg-title { font-size: 15px; font-weight: 700; }
.lg-nav { display: flex; gap: 2px; }
.lg-nav-item { padding: 6px 14px; cursor: pointer; color: #666; border-radius: 6px; font-size: 13px; }
.lg-nav-item:hover { background: #f5f7fa; }
.lg-nav-item.on { background: #e6f4ff; color: #1677ff; font-weight: 600; }
.page-card { background: #fff; border: 1px solid var(--el-border-color-lighter); border-radius: 8px; padding: 16px 18px; }
.pg-title { font-size: 16px; font-weight: 700; }
.dim2 { color: #999; font-size: 12px; }
.flex { display: flex; align-items: center; }
.sec { font-size: 13px; font-weight: 600; margin: 6px 0; }
.graph-shell { border-radius: 10px; overflow: hidden; border: 1px solid #1e2a3d; }
.gbar { background: #0d1626; padding: 8px 12px; display: flex; gap: 8px; align-items: center; flex-wrap: wrap; }
.gbar :deep(.el-input__wrapper) { background: #12203a; box-shadow: none; border: 1px solid #2a3b56; }
.gbar :deep(.el-input__inner) { color: #e6edf5; }
.gstats { margin-left: auto; color: #5a6a7d; font-size: 11px; }
.canvas { position: relative; height: 660px; background: radial-gradient(ellipse at 30% 20%, #12202f 0%, #0b1420 60%); overflow: hidden; }
.canvas svg { width: 100%; height: 100%; display: block; }
.zoom-col { position: absolute; right: 12px; bottom: 90px; display: flex; flex-direction: column; gap: 4px; }
.zoom-col .el-button { width: 30px; padding: 0; background: #0d1626; border-color: #2a3b56; color: #cfe3ff; }
.legend { position: absolute; left: 10px; bottom: 10px; background: rgba(13, 22, 38, .92); border: 1px solid #2a3b56; border-radius: 8px; padding: 7px 10px; font-size: 11px; color: #8b98ab; line-height: 1.9; }
.lg-row { display: flex; align-items: center; gap: 6px; }
.sq { width: 10px; height: 10px; border-radius: 2px; display: inline-block; }
.hint { margin-top: 4px; border-top: 1px solid #1e2a3d; padding-top: 4px; color: #5a6a7d; }
.g-tip { position: absolute; display: none; background: rgba(13, 22, 38, .95); border: 1px solid #2a3b56; border-radius: 8px; padding: 7px 10px; font-size: 12px; pointer-events: none; z-index: 30; max-width: 320px; }
.dm { color: #888; font-size: 12px; margin-bottom: 8px; }
.dbody { font-size: 13px; }
.mono2 { font-family: ui-monospace, Menlo, Consolas, monospace; }
.imp-sum { background: #fff7e6; border: 1px solid #ffd591; border-radius: 8px; padding: 9px 13px; font-size: 13px; color: #874d00; line-height: 1.8; }
.ic { border: 1px solid var(--el-border-color-lighter); border-left: 3px solid #1677ff; border-radius: 6px; padding: 6px 10px; margin-bottom: 6px; background: #fafbfc; }
.tr-card { border: 1px solid var(--el-border-color-lighter); border-radius: 8px; margin-bottom: 6px; padding: 8px 10px; }
.notice-card { border: 1px solid var(--el-border-color-lighter); border-left: 4px solid #fa8c16; border-radius: 10px; padding: 14px 16px; line-height: 1.9; background: #fff; }
.notice-card .nt { font-size: 15px; font-weight: 700; margin-bottom: 8px; }
.kv { display: flex; font-size: 13px; } .kv b { width: 110px; color: #999; font-weight: 400; }
:deep(.nd) { cursor: grab; }
:deep(.nd .bkg) { fill: #16223a; stroke: #2a3b56; stroke-width: 1; transition: filter .15s; }
:deep(.nd:hover .bkg) { filter: brightness(1.4); }
:deep(.nd.core .bkg) { fill: rgba(34, 211, 238, .1); stroke: #22d3ee; stroke-width: 1.7; }
:deep(.nd.core .lb) { fill: #7ce7f7; font-size: 13px; }
:deep(.nd .lb) { fill: #dbe7f5; font-size: 11px; font-weight: 600; }
:deep(.nd .sb) { fill: #6b7d94; font-size: 9.5px; }
:deep(.nd .ico) { font-size: 13px; }
:deep(.nd.blink .bkg) { stroke: #fff; stroke-width: 2.5; }
:deep(.edge) { fill: none; stroke-width: 1.6; opacity: .75; }
:deep(.el) { fill: #5a6a7d; font-size: 9.5px; pointer-events: none; }
</style>
