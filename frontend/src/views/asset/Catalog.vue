<template>
  <div class="cat-wrap">
    <div class="cat-head">
      <div class="pg-title">数据资产目录
        <el-tag size="small" effect="plain" style="margin-left:8px" :type="loaded ? 'success' : 'warning'">
          {{ loaded ? '已连接 · 表资产 ' + tables.length : '加载中…' }}
        </el-tag>
      </div>
      <div class="head-right">
        <el-button size="small" :type="favOnly ? 'warning' : 'default'" @click="favOnly = !favOnly">★ 收藏({{ favs.length }})</el-button>
        <el-button size="small" type="primary" plain :loading="loading" @click="load()">刷新</el-button>
      </div>
    </div>

    <div class="cat-body">
      <!-- 左: 标签目录 -->
      <aside class="side">
        <div class="side-title">🏷 标签目录</div>
        <el-tree :data="tree" node-key="id" default-expand-all highlight-current :expand-on-click-node="false" @node-click="onTag">
          <template #default="{ data }">
            <span class="node-row">
              <span class="node-name">{{ data.name }}</span>
              <span v-if="data.count != null" class="node-count">{{ data.count }}</span>
            </span>
          </template>
        </el-tree>
        <div class="side-note">点击标签筛选列表 · 自动标签: 分层/来源/类型; 业务域=人工打标(P0 API)</div>
      </aside>

      <!-- 右: 资产表格 -->
      <section class="main">
        <div class="bar">
          <el-input v-model="kw" size="small" placeholder="搜索表名 / 层 / 域…" clearable style="width:240px">
            <template #prefix>🔍</template>
          </el-input>
          <el-tag v-if="curTag" size="small" closable @close="curTag = ''">{{ curTag }}</el-tag>
          <span class="bar-dim">双击行进详情 · 共 {{ list.length }} 张</span>
        </div>
        <el-table :data="list" size="small" border v-loading="loading" height="calc(100vh - 280px)" @row-dblclick="goDetail" style="width:100%">
          <el-table-column label="收藏" width="46" align="center">
            <template #default="{ row }">
              <el-button size="small" text :type="row.fav ? 'warning' : 'info'" @click.stop="toggleFav(row)" style="font-size:16px">{{ row.fav ? '★' : '☆' }}</el-button>
            </template>
          </el-table-column>
          <el-table-column label="资产" width="190">
            <template #default="{ row }">
              <div class="tname" @click.stop="goDetail(row)">{{ row.label }}</div>
              <div class="tsub">{{ row.kind || 'physical' }}</div>
            </template>
          </el-table-column>
          <el-table-column label="层 · 域" width="110" show-overflow-tooltip>
            <template #default="{ row }">{{ row.layer || '—' }}<template v-if="row.domain"> · {{ row.domain }}</template></template>
          </el-table-column>
          <el-table-column label="数据来源" width="140" show-overflow-tooltip>
            <template #default="{ row }">
              <span v-if="row.sources.length" class="chip">{{ row.sources.join('、') }}</span>
              <span v-else class="dim">—</span>
            </template>
          </el-table-column>
          <el-table-column label="字段" prop="fields" width="60" align="center" />
          <el-table-column label="任务" prop="jobs" width="60" align="center" />
          <el-table-column label="规则" prop="rules" width="60" align="center" />
          <el-table-column label="订阅" width="80" align="center">
            <template #default="{ row }">
              <el-tooltip v-if="row.apps" :content="row.appNames.join(', ')" placement="top">
                <span class="app-num">{{ row.apps }}</span>
              </el-tooltip>
              <span v-else class="dim">—</span>
            </template>
          </el-table-column>
          <el-table-column label="操作" width="210" fixed="right">
            <template #default="{ row }">
              <el-button size="small" type="primary" plain @click.stop="goLineage(row)">血缘</el-button>
              <el-button size="small" type="warning" plain @click.stop="goLineage(row, 'i')">影响</el-button>
              <el-button size="small" plain @click.stop="goDetail(row)">详情</el-button>
            </template>
          </el-table-column>
        </el-table>
      </section>
    </div>
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
const kw = ref('')
const curTag = ref('')
const favOnly = ref(false)
const FAV_KEY = '***'
const favs = ref([])
const L = reactive({ nodes: [], edges: [] })
const byId = computed(() => { const m = {}; L.nodes.forEach(n => { m[n.id] = n }); return m })

const tables = computed(() => L.nodes.filter(n => n.type === 'Table'))
const labelOf = (id) => (byId.value[id] || {}).label || id

/* 表 → 数据源名(SOURCE: Source→Job; SOURCE_OF/DERIVED_FROM: Job→Table) */
function sourceMap() {
  const j2s = {}
  L.edges.forEach(e => { if (e.type === 'SOURCE') j2s[e.to] = labelOf(e.f) })
  const m = {}
  L.edges.forEach(e => {
    if ((e.type === 'SOURCE_OF' || e.type === 'DERIVED_FROM') && e.to.startsWith('table:') && j2s[e.f]) {
      ;(m[e.to] = m[e.to] || new Set()).add(j2s[e.f])
    }
  })
  return m
}
const sMap = computed(() => sourceMap())

/* 行模型 */
const list = computed(() => {
  const srcs = sMap.value
  return tables.value.map(t => {
    const tid = t.id
    const prefix = 'field:' + t.label + '.'
    const js = new Set(), rs = new Set(), app = new Set()
    let fields = 0
    L.edges.forEach(e => {
      if (e.type === 'CONTAINS' && e.from === tid) fields++
      else if ((e.type === 'SOURCE_OF' || e.type === 'DERIVED_FROM') && e.to === tid) js.add(e.f)
      else if (e.type === 'DEPENDS_ON' && e.to.startsWith(prefix)) rs.add(e.f)
      else if (e.type === 'CONSUMES' && e.to === tid) app.add(e.f)
    })
    return {
      id: tid, label: t.label, layer: t.layer, domain: t.domain, kind: t.kind,
      fields, jobs: js.size, rules: rs.size, apps: app.size,
      appNames: [...app].map(labelOf),
      sources: srcs[tid] ? [...srcs[tid]] : [],
      fav: favs.value.includes(t.label)
    }
  }).filter(r => {
    if (favOnly.value && !r.fav) return false
    if (curTag.value) {
      const [cat, val] = curTag.value.split(':')
      if (cat === 'layer' && (r.layer || '(未分层)') !== val) return false
      if (cat === 'kind' && (r.kind || 'physical') !== val) return false
      if (cat === 'src' && !r.sources.includes(val)) return false
      if (cat === 'fld' && !r.fields) return false
    }
    const q = kw.value.trim()
    if (!q) return true
    return (r.label + ' ' + (r.layer || '') + ' ' + (r.domain || '') + ' ' + r.sources.join(' ')).includes(q)
  })
})

/* 标签树 */
const tree = computed(() => {
  const cnt = {}
  tables.value.forEach(t => {
    const l = t.layer || '(未分层)'
    cnt['layer:' + l] = (cnt['layer:' + l] || 0) + 1
    const k = t.kind || 'physical'
    cnt['kind:' + k] = (cnt['kind:' + k] || 0) + 1
    const sm = sMap.value
    ;(sm[t.id] || []).forEach(x => { cnt['src:' + x] = (cnt['src:' + x] || 0) + 1 })
  })
  const branch = (title, prefix) => ({
    id: prefix, name: title,
    children: Object.entries(cnt).filter(([k]) => k.startsWith(prefix + ':'))
      .map(([k, c]) => ({ id: k, name: k.split(':').slice(1).join(':'), count: c }))
  })
  return [
    { id: 'all', name: '全部资产', count: tables.value.length },
    branch('数据分层', 'layer'),
    branch('数据来源', 'src'),
    branch('资产类型', 'kind'),
    { id: 'biz', name: '业务域(人工打标·P0)', children: [] }
  ]
})
function onTag(d) {
  if (d.children && d.children.length) return
  curTag.value = d.id === 'all' ? '' : d.id
}

/* 动作 */
function toggleFav(row) {
  const i = favs.value.indexOf(row.label)
  if (i >= 0) favs.value.splice(i, 1); else favs.value.push(row.label)
  localStorage.setItem(FAV_KEY, JSON.stringify(favs.value))
}
function goDetail(row) { router.push({ path: '/asset/detail/' + encodeURIComponent(row.label) }) }
function goLineage(row, p) { router.push({ path: '/asset/lineage', query: { t: row.label, p: p || 'g' } }) }

async function load() {
  loading.value = true
  try {
    const d = await request.get('/lineage/graph')
    L.nodes = d.nodes || []
    L.edges = (d.edges || []).map(e => ({ ...e, f: e.f || e.from, to: e.to }))
    loaded.value = true
  } catch (e) { ElMessage.error('目录加载失败(Neo4j?): ' + (e.message || e)) } finally { loading.value = false }
}
onMounted(() => {
  try { favs.value = JSON.parse(localStorage.getItem(FAV_KEY) || '[]') } catch (e) { favs.value = [] }
  load()
})
</script>

<style scoped>
.cat-wrap { min-height: calc(100vh - 120px); }
.cat-head { display: flex; justify-content: space-between; align-items: center; background: #fff; border: 1px solid var(--el-border-color-lighter); border-radius: 10px; padding: 10px 14px; margin-bottom: 10px; }
.head-right { display: flex; gap: 8px; }
.pg-title { font-size: 16px; font-weight: 700; }
.cat-body { display: grid; grid-template-columns: 228px 1fr; gap: 10px; align-items: start; }
.side { background: #fff; border: 1px solid var(--el-border-color-lighter); border-radius: 10px; padding: 10px 12px; max-height: calc(100vh - 200px); overflow: auto; }
.side-title { font-size: 13px; font-weight: 700; margin-bottom: 6px; }
.node-row { display: flex; justify-content: space-between; align-items: center; width: 100%; padding-right: 8px; }
.node-name { font-size: 13px; }
.node-count { color: #99a2ad; font-size: 11px; }
.side-note { color: #b0b7c2; font-size: 11px; margin-top: 8px; line-height: 1.7; border-top: 1px dashed var(--el-border-color-lighter); padding-top: 6px; }
.main { background: #fff; border: 1px solid var(--el-border-color-lighter); border-radius: 10px; padding: 10px 12px; min-width: 0; }
.bar { display: flex; align-items: center; gap: 10px; margin-bottom: 8px; flex-wrap: wrap; }
.bar-dim { margin-left: auto; color: #b0b7c2; font-size: 12px; }
.tname { color: #1677ff; font-weight: 600; cursor: pointer; line-height: 1.4; }
.tsub { color: #a5aeb9; font-size: 11px; }
.chip { display: inline-block; max-width: 100%; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; color: #0b5bd3; background: #e6f4ff; border: 1px solid #bcdcff; border-radius: 4px; padding: 0 6px; font-size: 11px; line-height: 17px; vertical-align: middle; }
.app-num { display: inline-block; min-width: 20px; color: #237804; background: #f6ffed; border: 1px solid #b7eb8f; border-radius: 4px; padding: 0 5px; font-size: 12px; line-height: 18px; cursor: help; }
.dim { color: #b0b7c2; }
</style>
