<template>
  <div class="cat-page">
    <div class="cat-card">
      <div class="flex-between">
        <div class="pg-title">数据资产目录<el-tag size="small" effect="plain" style="margin-left:8px" :type="loaded ? 'success' : 'warning'">{{ loaded ? '血缘图实时聚合(表资产 ' + rows.length + ')' : '加载中…' }}</el-tag></div>
        <div class="flex" style="gap:8px">
          <el-button size="small" :type="favOnly ? 'warning' : 'default'" @click="favOnly = !favOnly">★ 我的收藏 {{ favs.length }}</el-button>
          <el-button size="small" type="primary" plain @click="reload()">刷新</el-button>
        </div>
      </div>
    </div>
    <div class="cat-body">
      <!-- 左: 标签目录 -->
      <div class="cat-tree">
        <div class="tree-head">🏷 标签目录</div>
        <el-input v-model="tk2" size="small" placeholder="过滤资产…" clearable style="margin:6px 0" />
        <el-tree :data="tagTree" node-key="id" default-expand-all highlight-current :props="tp" :expand-on-click-node="false" @node-click="onTag">
          <template #default="{ data }">
            <span class="tnode">
              <span>{{ data.name }}</span>
              <el-tag v-if="data.count != null" size="small" effect="plain" style="margin-left:auto;border:none">{{ data.count }}</el-tag>
            </span>
          </template>
        </el-tree>
        <div class="dim2" style="margin-top:8px" v-if="curTag">当前筛选: <el-tag size="small" closable @close="curTag = ''; curPath = ''">{{ curPath }}</el-tag></div>
        <div class="dim2" style="margin-top:4px">目录=标签(字典树)动态聚合 · 自动标签: 来源/层/类型; 业务域=人工打标(P0 绑定 API 后启用)</div>
      </div>
      <!-- 右: 资产列表 -->
      <div class="cat-list">
        <el-table :data="rows" size="small" border v-loading="loading" max-height="calc(100vh - 220px)" @row-dblclick="openDetail">
          <el-table-column label="资产" min-width="190">
            <template #default="{ row }">
              <span class="t-name" @click="openDetail(row)">{{ row.label }}</span>
              <el-tag size="small" effect="plain" style="margin-left:6px">{{ row.kind || 'physical' }}</el-tag>
              <el-tag v-if="row.fav" size="small" type="warning" effect="dark" style="margin-left:4px">★</el-tag>
            </template>
          </el-table-column>
          <el-table-column label="层 / 域" min-width="120">
            <template #default="{ row }"><span class="dim2">{{ row.layer || '—' }}<template v-if="row.domain"> · {{ row.domain }}</template></span></template>
          </el-table-column>
          <el-table-column label="数据来源" min-width="120">
            <template #default="{ row }">
              <el-tag v-for="s in row.sources" :key="s" size="small" effect="plain" style="margin:1px">{{ s }}</el-tag>
              <span v-if="!row.sources.length" class="dim2">—</span>
            </template>
          </el-table-column>
          <el-table-column prop="fields" label="字段" width="60" align="center" />
          <el-table-column label="上游任务" width="80" align="center"><template #default="{ row }">{{ row.jobs || '—' }}</template></el-table-column>
          <el-table-column label="依赖规则" width="80" align="center"><template #default="{ row }">{{ row.rules || '—' }}</template></el-table-column>
          <el-table-column label="订阅应用" width="80" align="center">
            <template #default="{ row }"><el-tag v-if="row.apps" size="small" type="success" effect="plain">{{ row.apps }}</el-tag><span v-else class="dim2">—</span></template>
          </el-table-column>
          <el-table-column label="操作" width="230" fixed="right">
            <template #default="{ row }">
              <el-button size="small" text type="primary" @click="goLineage(row)">血缘</el-button>
              <el-button size="small" text type="warning" @click="goLineage(row, 'i')">影响</el-button>
              <el-button size="small" text @click="openDetail(row)">详情</el-button>
              <el-button size="small" text :type="row.fav ? 'warning' : 'info'" @click="toggleFav(row)">★</el-button>
            </template>
          </el-table-column>
        </el-table>
      </div>
    </div>

    <!-- 详情抽屉 -->
    <el-drawer v-model="dlg" :title="cur ? '📄 ' + cur.label : ''" size="520px">
      <template v-if="cur">
        <div class="dim2" style="margin-bottom:8px">{{ cur.layer || '' }} {{ cur.kind || '' }}<template v-if="cur.domain"> · {{ cur.domain }}</template> · 数据来源: {{ cur.sources.join(', ') || '—' }}</div>
        <el-descriptions :column="2" size="small" border>
          <el-descriptions-item label="字段">{{ cur.fields }}</el-descriptions-item>
          <el-descriptions-item label="上游任务">{{ cur.jobs || '—' }}</el-descriptions-item>
          <el-descriptions-item label="依赖规则">{{ cur.rules || '—' }}</el-descriptions-item>
          <el-descriptions-item label="订阅应用">{{ cur.apps || '—' }}</el-descriptions-item>
        </el-descriptions>
        <div class="sec" style="margin-top:12px">字段列表</div>
        <el-table :data="fieldsOf(cur)" size="small" border max-height="240">
          <el-table-column prop="name" label="字段" width="130" />
          <el-table-column prop="k" label="类型/说明" />
        </el-table>
        <div class="sec" style="margin-top:12px">操作</div>
        <div style="display:flex;gap:8px;flex-wrap:wrap">
          <el-button size="small" type="primary" plain @click="goLineage(cur)">进入血缘图谱(锚定本表)</el-button>
          <el-button size="small" type="warning" plain @click="goLineage(cur, 'i')">影响分析</el-button>
          <el-button size="small" @click="subscribe(cur)">订阅此表 →</el-button>
        </div>
        <div class="dim2" style="margin-top:8px">字段密级🔒/敏感 mask/样例预览 = P1(接 data_policy); Owner/业务描述 = P1(data_asset 主档); 人工打标 = P0 绑定 API</div>
      </template>
    </el-drawer>
  </div>
</template>

<script setup>
import { ref, reactive, computed, onMounted } from 'vue'
import { useRouter } from 'vue-router'
import { ElMessage } from 'element-plus'
import request from '../../api/request'

const router = useRouter()
const loading = ref(false)
const loaded = ref(false)
const L = reactive({ nodes: [], edges: [] })
const byId = computed(() => { const m = {}; L.nodes.forEach(n => { m[n.id] = n }); return m })
const tk = ref('')
const tk2 = ref('')
const favOnly = ref(false)
const curTag = ref('')
const curPath = ref('')
const dlg = ref(false)
const cur = ref(null)
const FAV_KEY = 'dcp_catalog_favs'
const favs = ref([])

/* ============ 数据: 血缘图 Table 聚合 ============ */
async function reload() {
  loading.value = true
  try {
    const d = await request.get('/lineage/graph')
    L.nodes = d.nodes || []
    L.edges = (d.edges || []).map(e => ({ ...e, f: e.f || e.from, to: e.to }))
    loaded.value = true
    if (!rows.value.length) ElMessage.info('目录已加载 ' + L.nodes.filter(n => n.type === 'Table').length + ' 张表资产')
  } catch (e) { ElMessage.error('目录加载失败(Neo4j 未就绪?): ' + (e.message || e)) } finally { loading.value = false }
}
function tlabel(id) { return (byId.value[id] || {}).label || id }

const rows = computed(() => {
  const kw = tk.value.trim() || tk2.value.trim()
  const tables = L.nodes.filter(n => n.type === 'Table')
  const out = tables.map(t => {
    const tid = t.id, prefix = 'field:' + t.label + '.'
    let fields = 0, jobs = 0, rules = 0, apps = 0
    const js = new Set(), rs = new Set(), as = new Set()
    L.edges.forEach(e => {
      if (e.type === 'CONTAINS' && e.from === tid) fields++
      else if ((e.type === 'SOURCE_OF' || e.type === 'DERIVED_FROM') && e.to === tid) js.add(e.f)
      else if (e.type === 'DEPENDS_ON' && e.to.startsWith(prefix)) rs.add(e.f)
      else if (e.type === 'CONSUMES' && e.to === tid) as.add(e.f)
    })
    return { id: tid, label: t.label, layer: t.layer, domain: t.domain, kind: t.kind, fields, jobs: js.size, rules: rs.size, apps: as.size, fav: favs.value.includes(t.label), _n: t, _sources: sourcesOf(t.label) }
  })
  return out.filter(r => {
    if (favOnly.value && !r.fav) return false
    if (curTag.value) {
      const ok = tagMatch(r, curTag.value)
      if (!ok) return false
    }
    if (!kw) return true
    return (r.label + ' ' + (r.layer || '') + ' ' + (r.domain || '') + ' ' + r._sources.join(' ')).includes(kw)
  })
})
/* 表 → 数据源系统名(经 SOURCE→Job→SOURCE_OF/DERIVED_FROM) */
function sourcesOf(tname) {
  const tid = 'table:' + tname
  const job2src = {}
  L.edges.forEach(e => { if (e.type === 'SOURCE') job2src[e.to] = tlabel(e.f) })
  const s = new Set()
  L.edges.forEach(e => {
    if ((e.type === 'SOURCE_OF' || e.type === 'DERIVED_FROM') && e.to === tid && job2src[e.f]) s.add(job2src[e.f])
  })
  return [...s]
}
/* 标签树(自动标签真筛选 + 人工子树骨架) */
const tp = { children: 'children', label: 'name' }
const tagTree = computed(() => {
  const tables = L.nodes.filter(n => n.type === 'Table')
  const lay = {}, src = {}, kind = {}
  tables.forEach(t => {
    const l = t.layer || '(未分层)'; lay[l] = (lay[l] || 0) + 1
    const k = t.kind || 'physical'; kind[k] = (kind[k] || 0) + 1
    sourcesOf(t.label).forEach(s => { src[s] = (src[s] || 0) + 1 })
  })
  const leaf = (id, name, items) => ({ id, name, count: items.length, children: items.map(x => ({ id: id + ':' + x[0], name: x[0], count: x[1], raw: x[0] })) })
  return [
    { id: 'all', name: '全部资产', count: tables.length, raw: '*' },
    leaf('layer', '数据分层', Object.entries(lay)),
    leaf('src', '数据来源', Object.entries(src)),
    leaf('kind', '资产类型', Object.entries(kind)),
    { id: 'biz', name: '业务域(人工打标·P0)', count: null, children: [], raw: '' }
  ]
})
function tagMatch(row, tag) {
  const [cat, val] = tag.split(':')
  if (cat === 'layer') return (row.layer || '(未分层)') === val
  if (cat === 'kind') return (row.kind || 'physical') === val
  if (cat === 'src') return row._sources.includes(val)
  if (tag === '*') return true
  return false
}
function onTag(data) {
  if (!data.raw && !data.raw === false && data.id !== 'all') return
  if (data.children && data.children.length) return
  curTag.value = data.id === 'all' ? '' : data.id
  curPath.value = data.name
}
function fieldsOf(row) {
  return L.edges.filter(e => e.type === 'CONTAINS' && e.from === row.id)
    .map(e => { const f = byId.value[e.to]; return { name: (f || {}).label || '', k: '' } }).filter(x => x.name && !x.name.startsWith('_'))
}
/* ============ 收藏 ============ */
function toggleFav(row) {
  const i = favs.value.indexOf(row.label)
  if (i >= 0) favs.value.splice(i, 1); else favs.value.push(row.label)
  localStorage.setItem(FAV_KEY, JSON.stringify(favs.value))
}
/* ============ 跳转血缘 ============ */
function goLineage(row, page) {
  router.push({ path: '/asset/lineage', query: { t: row.label, p: page || 'g' } })
}
function subscribe(row) {
  ElMessage.success('跳转订阅中心新建契约(视图预填 ' + row.label + ')—后端预填 P1')
  setTimeout(() => router.push({ path: '/datasub', query: { t: row.label } }), 300)
}
function openDetail(row) { cur.value = row; dlg.value = true }
onMounted(() => { favs.value = JSON.parse(localStorage.getItem(FAV_KEY) || '[]'); reload() })
</script>

<style scoped>
.cat-page { min-height: calc(100vh - 120px); }
.cat-card { background: #fff; border: 1px solid var(--el-border-color-lighter); border-radius: 8px; padding: 10px 14px; margin-bottom: 10px; }
.pg-title { font-size: 16px; font-weight: 700; }
.flex { display: flex; align-items: center; }
.flex-between { display: flex; justify-content: space-between; align-items: center; }
.cat-body { display: grid; grid-template-columns: 240px 1fr; gap: 10px; }
.cat-tree { background: #fff; border: 1px solid var(--el-border-color-lighter); border-radius: 8px; padding: 10px 12px; max-height: calc(100vh - 190px); overflow: auto; }
.tree-head { font-size: 13px; font-weight: 700; }
.tnode { display: flex; align-items: center; gap: 4px; width: 100%; padding-right: 6px; font-size: 13px; }
.cat-list { background: #fff; border: 1px solid var(--el-border-color-lighter); border-radius: 8px; padding: 10px 12px; }
.t-name { color: #1677ff; font-weight: 600; cursor: pointer; }
.sec { font-size: 13px; font-weight: 600; }
.dim2 { color: #999; font-size: 12px; }
</style>
