<template>
  <div class="detail-page">
    <div class="d-top" v-loading="loading">
      <template v-if="node">
        <div>
          <el-button text @click="back">< 返回目录</el-button>
        </div>
        <div class="d-title">
          <span class="d-name">{{ node.label }}</span>
          <el-tag size="small" effect="plain">{{ node.kind || 'physical' }}</el-tag>
          <el-button size="small" text :type="fav ? 'warning' : 'info'" @click="toggleFav">{{ fav ? '★ 已收藏' : '☆ 收藏' }}</el-button>
        </div>
        <div class="d-meta">
          {{ node.layer || '—' }}<template v-if="node.domain"> · {{ node.domain }}</template> · 数据来源: {{ sources.join(', ') || '—' }}
        </div>
        <div class="d-actions">
          <el-button size="small" type="primary" plain @click="go('g')">⛓ 血缘图谱</el-button>
          <el-button size="small" type="warning" plain @click="go('i')">影响分析</el-button>
          <el-button size="small" type="danger" plain @click="go('t')">溯源</el-button>
          <el-button size="small" plain @click="go('m')">字段映射矩阵</el-button>
          <el-button size="small" @click="subscribe">订阅此表 →</el-button>
        </div>
      </template>
      <el-empty v-else-if="loaded && !node" description="未找到该资产" :image-size="70"><el-button size="small" @click="back">返回目录</el-button></el-empty>
    </div>

    <div class="d-body" v-if="node">
      <div class="d-left">
        <div class="panel">
          <div class="p-t">基本信息</div>
          <el-descriptions :column="2" size="small" border>
            <el-descriptions-item label="资产类型">{{ node.kind || 'physical' }}</el-descriptions-item>
            <el-descriptions-item label="数据分层">{{ node.layer || '—' }}</el-descriptions-item>
            <el-descriptions-item label="业务域">{{ node.domain || '—' }}</el-descriptions-item>
            <el-descriptions-item label="Owner">— <span class="dim2">(data_asset 主档 P1)</span></el-descriptions-item>
            <el-descriptions-item label="数据来源">{{ sources.join(', ') || '—' }}</el-descriptions-item>
            <el-descriptions-item label="更新频率">— <span class="dim2">(P1)</span></el-descriptions-item>
            <el-descriptions-item label="行数 / 密级">— <span class="dim2">(P1 · data_policy)</span></el-descriptions-item>
            <el-descriptions-item label="字段数">{{ fields.length }}</el-descriptions-item>
          </el-descriptions>
        </div>
        <div class="panel">
          <div class="p-t">字段列表 <span class="dim2">(血缘图字段级节点; 类型/主键/密级/样例 = P1 information_schema+data_policy)</span></div>
          <el-table :data="fields" size="small" border max-height="420" style="width:100%">
            <el-table-column prop="name" label="字段名" min-width="150" />
            <el-table-column label="说明" min-width="200">
              <template #default="{ row }">
                <span class="dim2" v-if="row.k">—</span>
                <el-tag v-if="row.dep" size="small" type="danger" effect="plain">被规则依赖</el-tag>
              </template>
            </el-table-column>
          </el-table>
        </div>
      </div>
      <div class="d-right">
        <div class="panel">
          <div class="p-t">血缘关系</div>
          <div class="rel"><div class="r-t">上游任务 ({{ upJobs.length }})</div>
            <el-tag v-for="j in upJobs" :key="j" size="small" effect="plain" style="margin:2px">{{ j }}</el-tag>
            <span v-if="!upJobs.length" class="dim2">无登记同步任务</span></div>
          <div class="rel"><div class="r-t">依赖本表字段的规则 ({{ rules.length }})</div>
            <el-tag v-for="r in rules" :key="r" size="small" type="danger" effect="plain" style="margin:2px">{{ r }}</el-tag>
            <span v-if="!rules.length" class="dim2">—</span></div>
          <div class="rel"><div class="r-t">订阅应用 ({{ apps.length }})</div>
            <el-tag v-for="a in apps" :key="a" size="small" type="success" effect="plain" style="margin:2px">{{ a }}</el-tag>
            <span v-if="!apps.length" class="dim2">未被订阅</span></div>
        </div>
        <div class="panel">
          <div class="p-t">健康与治理 <span class="dim2">(P2)</span></div>
          <div class="dim2" style="line-height:2">健康分: —(运行信号合成, P2)<br>质量档: —<br>生命周期: —</div>
        </div>
        <div class="panel">
          <div class="p-t">标签 <span class="dim2">(自动=层/来源; 业务标签已接后端)</span></div>
          <div style="line-height:2">
            <div class="dim2">自动: {{ node.layer || '未分层' }} / {{ sources.join('、') || '无来源' }}</div>
            <div class="tag-row">
              <el-tag v-for="t in myTags" :key="t" size="small" closable @close="unTag(t)" style="margin:2px">{{ t }}</el-tag>
              <span v-if="!myTags.length" class="dim2">暂无业务标签</span>
            </div>
            <div class="flex" style="gap:6px;margin-top:6px">
              <el-input v-model="tagInput" size="small" placeholder="如: 业务域/客户/核心" style="width:190px" @keyup.enter="doTag" />
              <el-button size="small" type="primary" @click="doTag">打标</el-button>
            </div>
            <div class="dim2" style="margin-top:4px">预设: <span v-for="p in presets" :key="p" class="preset" @click="tagInput = p">{{ p }}</span></div>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, reactive, computed, onMounted } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { ElMessage } from 'element-plus'
import request from '../../api/request'

const route = useRoute()
const router = useRouter()
const name = computed(() => decodeURIComponent(route.params.name || ''))
const loading = ref(false)
const loaded = ref(false)
const L = reactive({ nodes: [], edges: [] })
const byId = computed(() => { const m = {}; L.nodes.forEach(n => { m[n.id] = n }); return m })
const FAV_KEY = '***'
const favs = ref([])
const fav = ref(false)

const node = computed(() => L.nodes.find(n => n.type === 'Table' && n.label === name.value) || null)
const sources = computed(() => sourcesOf(name.value))
const fields = computed(() => L.edges.filter(e => e.type === 'CONTAINS' && e.from === 'table:' + name.value)
  .map(e => { const f = byId.value[e.to]; return { name: (f || {}).label || '' } })
  .filter(f => f.name && !f.name.startsWith('_')))
const upJobs = computed(() => L.edges.filter(e => (e.type === 'SOURCE_OF' || e.type === 'DERIVED_FROM') && e.to === 'table:' + name.value).map(e => tlabel(e.f)))
const rules = computed(() => {
  const prefix = 'field:' + name.value + '.'
  return [...new Set(L.edges.filter(e => e.type === 'DEPENDS_ON' && e.to.startsWith(prefix)).map(e => tlabel(e.f)))]
})
const apps = computed(() => L.edges.filter(e => e.type === 'CONSUMES' && e.to === 'table:' + name.value).map(e => tlabel(e.f)))

function tlabel(id) { return (byId.value[id] || {}).label || id }
function sourcesOf(tn) {
  const tid = 'table:' + tn
  const j2s = {}
  L.edges.forEach(e => { if (e.type === 'SOURCE') j2s[e.to] = tlabel(e.f) })
  const s = new Set()
  L.edges.forEach(e => { if ((e.type === 'SOURCE_OF' || e.type === 'DERIVED_FROM') && e.to === tid && j2s[e.f]) s.add(j2s[e.f]) })
  return [...s]
}
const myTags = ref([])
const tagInput = ref('')
const presets = ['业务域/客户', '业务域/交易', '项目/营销', '治理/核心资产']
async function loadMyTags() {
  try { myTags.value = (await request.get('/asset-catalog/tags', { params: { table: name.value } })) || [] } catch (e) { /* noop */ }
}
async function doTag() {
  const p = tagInput.value.trim()
  if (!p) return
  try {
    await request.post('/asset-catalog/tag', { table: name.value, path: p })
    tagInput.value = ''
    await loadMyTags()
    ElMessage.success('已打标: ' + p)
  } catch (e) { ElMessage.error('打标失败: ' + (e.message || e)) }
}
async function unTag(path) {
  try {
    await request.post('/asset-catalog/untag', { table: name.value, path })
    await loadMyTags()
  } catch (e) { ElMessage.error('去标失败: ' + (e.message || e)) }
}
function toggleFav() {
  const i = favs.value.indexOf(name.value)
  if (i >= 0) favs.value.splice(i, 1); else favs.value.push(name.value)
  localStorage.setItem(FAV_KEY, JSON.stringify(favs.value))
  fav.value = favs.value.includes(name.value)
}
function go(page) {
  router.push({ path: '/asset/lineage', query: { t: name.value, p: page } })
}
function subscribe() {
  ElMessage.success('跳转订阅中心新建契约(视图预填 ' + name.value + ')—后端预填 P1')
  setTimeout(() => router.push({ path: '/datasub', query: { t: name.value } }), 300)
}
function back() { router.push('/asset/catalog') }
async function load() {
  loading.value = true
  try {
    const d = await request.get('/lineage/graph')
    L.nodes = d.nodes || []
    L.edges = (d.edges || []).map(e => ({ ...e, f: e.f || e.from, to: e.to }))
    loaded.value = true
  } catch (e) { ElMessage.error('加载失败(Neo4j?): ' + (e.message || e)) } finally { loading.value = false }
}
onMounted(() => {
  favs.value = JSON.parse(localStorage.getItem(FAV_KEY) || '[]')
  fav.value = favs.value.includes(name.value)
  load()
  loadMyTags()
})
</script>

<style scoped>
.detail-page { min-height: calc(100vh - 120px); }
.d-top { background: #fff; border: 1px solid var(--el-border-color-lighter); border-radius: 10px; padding: 12px 16px; margin-bottom: 10px; }
.d-title { display: flex; align-items: center; gap: 8px; margin-top: 4px; }
.d-name { font-size: 19px; font-weight: 700; color: #0b5bd3; }
.d-meta { color: #666; font-size: 12px; margin: 4px 0 10px; }
.d-actions { display: flex; gap: 8px; flex-wrap: wrap; }
.d-body { display: grid; grid-template-columns: 1fr 330px; gap: 10px; }
.panel { background: #fff; border: 1px solid var(--el-border-color-lighter); border-radius: 10px; padding: 12px 14px; margin-bottom: 10px; }
.p-t { font-size: 13px; font-weight: 700; margin-bottom: 10px; }
.rel { margin-bottom: 12px; }
.r-t { font-size: 12px; color: #666; margin-bottom: 6px; }
.dim2 { color: #999; font-size: 12px; }
.tag-row { display: flex; flex-wrap: wrap; align-items: center; gap: 2px; margin-top: 4px; }
.preset { color: #1677ff; cursor: pointer; margin-right: 8px; }
.flex { display: flex; align-items: center; }
</style>
