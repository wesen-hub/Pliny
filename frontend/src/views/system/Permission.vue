<template>
  <div class="page-card">
    <el-card shadow="never">
      <div class="flex-between">
        <div>
          <div class="pg-title">数据权限配置<el-tag size="small" effect="plain" style="margin-left:8px">L0-L3 × 表/列/标签 + 行规则(已接后端)</el-tag></div>
          <el-text size="small" type="info">配置针对具体 <b>角色或用户</b>; 判定 user &gt; role &gt; 分类默认 &gt; 资产密级兜底; Deny 优先。数据出口(订阅)在④。</el-text>
        </div>
        <div class="subject-bar">
          <el-radio-group v-model="subjectKind" size="small">
            <el-radio-button label="role">角色</el-radio-button>
            <el-radio-button label="user">用户</el-radio-button>
          </el-radio-group>
          <el-select v-model="subject" size="small" style="width:170px" filterable>
            <el-option v-for="r in roles" :key="r.code" :label="r.name + ' (' + r.code + ')'" :value="r.code" />
          </el-select>
          <el-select v-if="subjectKind === 'user'" v-model="subjectUser" size="small" style="width:170px" filterable placeholder="选用户">
            <el-option v-for="u in users" :key="u.username" :label="u.username + ' (' + (u.roles || []).join('/') + ')'" :value="u.username" />
          </el-select>
          <el-button size="small" :loading="loading" @click="loadAll">刷新</el-button>
        </div>
      </div>

      <el-tabs v-model="tab" style="margin-top:8px">
        <!-- ① 资源级 -->
        <el-tab-pane label="① 资源级 · 业务分类" name="scope">
          <div class="panel-tip">分类默认档作用于其下全部表/标签(前缀匹配); 下层显式配置可覆盖。</div>
          <el-table :data="scopeRows" size="small" border style="margin-top:8px">
            <el-table-column label="分类" min-width="220">
              <template #default="{ row }"><b>{{ row.path }}</b><el-tag size="small" effect="plain" style="margin-left:6px">{{ row.dim }}</el-tag></template>
            </el-table-column>
            <el-table-column label="能力档" width="300">
              <template #default="{ row }">
                <el-select v-if="row.hasPolicy" :model-value="row.level" size="small" style="width:150px" @change="(v) => setLevel('scope', row.path, v, '')">
                  <el-option v-for="(lv, i) in levelNames" :key="i" :value="i"><span :class="'lv lv' + i">{{ lv }}</span></el-option>
                </el-select>
                <el-text v-else size="small" type="info">未配置(继承下级默认)</el-text>
                <el-button v-if="row.hasPolicy" size="small" text type="danger" @click="delPolicy(row.pid)">清除</el-button>
              </template>
            </el-table-column>
            <el-table-column label="快速设置">
              <template #default="{ row }">
                <el-button v-for="i in [0,1,2,3]" :key="i" size="small" text :class="'lv lv' + i" @click="setLevel('scope', row.path, i, '')">L{{ i }}</el-button>
              </template>
            </el-table-column>
          </el-table>
        </el-tab-pane>

        <!-- ② 表级+行列 -->
        <el-tab-pane label="② 表级 + 行列级" name="table">
          <div class="flex" style="gap:12px;align-items:flex-start">
            <el-card shadow="never" class="side" style="width:260px">
              <template #header><b>数据集</b></template>
              <div v-for="t in tables" :key="t.name" class="trow" :class="{ on: curTable === t.name }" @click="pickTable(t.name)">
                {{ t.name }}
                <el-tag v-if="t.secret" size="small" effect="plain" style="margin-left:auto" :type="secType(t.secret)">{{ secName(t.secret) }}</el-tag>
              </div>
            </el-card>
            <div style="flex:1;min-width:0">
              <el-card shadow="never">
                <template #header><b>表级 · {{ curTable }}</b> <el-text size="small" type="info">(密级兜底: {{ curSecretText }})</el-text></template>
                <div class="flex" style="gap:6px;align-items:center">
                  <el-text size="small">能力档</el-text>
                  <el-button-group>
                    <el-button v-for="i in [0,1,2,3]" :key="i" size="small" :type="tableLevel === i ? 'primary' : 'default'" @click="setLevel('dataset', curTable, i, '')">L{{ i }} {{ lvShort(i) }}</el-button>
                  </el-button-group>
                  <el-button v-if="tablePolicyId" size="small" text type="danger" @click="delPolicy(tablePolicyId)">清除策略</el-button>
                </div>
              </el-card>
              <el-card shadow="never" style="margin-top:10px">
                <template #header><b>列级({{ subjectLabel }})</b><el-button size="small" text type="primary" style="float:right" @click="loadColumns">刷新列</el-button></template>
                <el-table :data="columns" size="small" v-loading="colLoading">
                  <el-table-column prop="name" label="列" width="150" />
                  <el-table-column label="能力档" width="360">
                    <template #default="{ row }">
                      <el-button v-for="i in [0,1,2,3]" :key="i" size="small" text :class="'lv lv' + i"
                        :style="colLevel(row.name) === i ? 'text-decoration:underline;font-weight:700' : ''"
                        @click="setColumn(row.name, i)">L{{ i }}</el-button>
                      <el-text v-if="colLevel(row.name) < 0" size="small" type="info">(继承表级)</el-text>
                      <el-text v-if="colLevel(row.name) === 2" size="small" type="warning">脱敏可读</el-text>
                    </template>
                  </el-table-column>
                </el-table>
              </el-card>
              <el-card shadow="never" style="margin-top:10px">
                <template #header><b>行规则(表数据直查出口)</b></template>
                <div v-for="rr in rowRules" :key="rr.id" class="rrline">
                  <el-tag size="small" effect="plain">{{ rr.name }}</el-tag>
                  <el-text size="small" type="info">{{ rr.kind }}: {{ rr.cond_json || rr.tag_ref_path || '实体清单' }}</el-text>
                  <el-button size="small" text type="danger" @click="delRowRule(rr.id)">删</el-button>
                </div>
                <el-button size="small" @click="openRowRule" style="margin-top:6px">＋ 配置行规则</el-button>
              </el-card>
            </div>
          </div>
        </el-tab-pane>

        <!-- ③ 标签级 -->
        <el-tab-pane label="③ 标签级" name="tag">
          <el-table :data="tagRows" size="small" border>
            <el-table-column label="标签(注册树)" min-width="280">
              <template #default="{ row }">
                <el-tag v-if="row.kind === 'tag'" size="small" type="primary" effect="plain">{{ row.path }}</el-tag>
                <el-text v-else size="small" type="info">{{ row.path }}(分类节点)</el-text>
              </template>
            </el-table-column>
            <el-table-column label="能力档" width="320">
              <template #default="{ row }">
                <el-button-group v-if="row.kind === 'tag'">
                  <el-button v-for="i in [0,1,2,3]" :key="i" size="small" text :class="'lv lv' + i"
                    :style="tagLevel(row.path) === i ? 'text-decoration:underline;font-weight:700' : ''"
                    @click="setTag(row.path, i)">L{{ i }}</el-button>
                </el-button-group>
                <el-text v-else size="small" type="info">分类节点: 到①资源级按业务域配</el-text>
              </template>
            </el-table-column>
            <el-table-column label="语义" min-width="220">
              <template #default="{ row }">
                <template v-if="row.kind === 'tag'">
                  <el-text size="small" type="info">{{ tagSem(tagLevel(row.path)) }}</el-text>
                </template>
              </template>
            </el-table-column>
          </el-table>
        </el-tab-pane>

        <!-- ④ 数据订阅出口 -->
        <el-tab-pane label="④ 数据订阅出口" name="sub">
          <div class="sub-warn">
            <b>出口安全</b>: 订阅=属主实时权限快照(每次命中/投递按属主能力重算); 圈选需 L2+, 明细导出需 L3(功能码 label:export)+ 审计; 属主降权自动停推。
          </div>
          <el-table :data="subs" size="small" border style="margin-top:10px">
            <el-table-column label="订阅" min-width="170">
              <template #default="{ row }"><div style="font-weight:600">{{ row.name }}</div><el-text size="small" type="info">#{{ row.id }} · 属主 {{ row.owner || '—' }}</el-text></template>
            </el-table-column>
            <el-table-column label="条件" min-width="200">
              <template #default="{ row }"><el-tag size="small" type="primary" effect="plain">{{ row.tag_path }}</el-tag><el-tag v-if="row.tag_value" size="small" style="margin-left:4px">={{ row.tag_value }}</el-tag></template>
            </el-table-column>
            <el-table-column label="当前登录可见等级" width="180">
              <template #default="{ row }">
                <el-text :class="'lv lv' + row.ownerLevel">L{{ row.ownerLevel }}</el-text>
                <el-tag v-if="row.ownerLevel < 2" size="small" type="danger" effect="plain" style="margin-left:6px">不足 L2·停推</el-tag>
                <el-tag v-else size="small" type="success" effect="plain">可圈选</el-tag>
              </template>
            </el-table-column>
            <el-table-column label="投递通道" width="170">
              <template #default="{ row }">{{ row.channel || '站内' }}<div v-if="row.freq" class="dim">{{ row.freq }}</div></template>
            </el-table-column>
            <el-table-column label="命中实体" width="90" align="center">
              <template #default="{ row }">{{ row.hit_entities ?? '—' }}</template>
            </el-table-column>
            <el-table-column label="操作" width="110">
              <template #default="{ row }">
                <el-button size="small" text type="primary" @click="exportSub(row)">导出(L3)</el-button>
              </template>
            </el-table-column>
          </el-table>
        </el-tab-pane>
      </el-tabs>
    </el-card>

    <!-- 行规则弹层 -->
    <el-dialog v-model="ruleDlg" title="配置行规则" width="560">
      <el-form label-width="80px" size="small">
        <el-form-item label="规则名"><el-input v-model="ruleForm.name" /></el-form-item>
        <el-form-item label="类型">
          <el-radio-group v-model="ruleForm.kind">
            <el-radio label="cond">字段条件</el-radio>
            <el-radio label="list">实体清单</el-radio>
            <el-radio label="tag_ref">引用标签圈选</el-radio>
          </el-radio-group>
        </el-form-item>
        <el-form-item v-if="ruleForm.kind === 'cond'" label="条件">
          <el-input v-model="ruleForm.cond" placeholder='{"field":"region","op":"=","val":"华东"}' />
        </el-form-item>
        <el-form-item v-if="ruleForm.kind === 'tag_ref'" label="引用标签">
          <el-select v-model="ruleForm.tagRef" filterable style="width:100%">
            <el-option v-for="t in tags" :key="t" :label="t" :value="t" />
          </el-select>
        </el-form-item>
      </el-form>
      <template #footer><el-button size="small" @click="ruleDlg = false">取消</el-button><el-button size="small" type="primary" @click="saveRowRule">保存</el-button></template>
    </el-dialog>
  </div>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue'
import { ElMessage } from 'element-plus'
import request from '../../api/request'

const levelNames = ['L0 不可见', 'L1 结构可见', 'L2 脱敏可读', 'L3 完全可读']
const lvShort = (i) => ['隐藏', '结构', '脱敏', '全量'][i]
const tagSem = (l) => ['目录不展示', '见定义不可圈选', '可圈选仅计数', '全量+明细'][l < 0 ? 3 : l]
const secName = (s) => ['', '公开', '内部', '机密', '绝密'][s] || '—'
const secType = (s) => ['', 'success', 'primary', 'warning', 'danger'][s] || 'info'

const tab = ref('scope')
const subjectKind = ref('role')
const subject = ref('ANALYST')
const subjectUser = ref('li')
const roles = ref([])
const users = ref([])
const loading = ref(false)
const colLoading = ref(false)

const subjectLabel = computed(() => (subjectKind.value === 'role' ? subject.value : subjectUser.value))
const subjectId = computed(() => (subjectKind.value === 'role' ? subject.value : subjectUser.value))
const subjectType = computed(() => subjectKind.value)
const st = computed(() => ({ subjectType: subjectType.value, subjectId: subjectId.value }))

/* 数据 */
const policies = ref([])         // 当前主体全部策略
const tables = ref([])
const tags = ref([])
const groups = ref([])
const scopes = ref([])
const curTable = ref('customer_wide')
const columns = ref([])
const rowRules = ref([])
const subs = ref([])

function policyOf(type, path) {
  return policies.value.find(p => p.res_type === type && p.res_path === path)
}
const tableLevel = computed(() => { const p = policyOf('dataset', curTable.value); return p ? p.level : -1 })
const tablePolicyId = computed(() => { const p = policyOf('dataset', curTable.value); return p ? p.id : null })
const curSecretText = computed(() => {
  const t = tables.value.find(x => x.name === curTable.value)
  return t && t.secret ? secName(t.secret) : '未登记(默认内部)'
})
function colLevel(name) { const p = policyOf('column', curTable.value + '.' + name); return p ? p.level : -1 }
function tagLevel(path) { const p = policyOf('tag', path); return p ? p.level : -1 }
const tagRows = computed(() => [
  ...(groups.value || []).filter(g => g.startsWith('业务域/')).map(g => ({ kind: 'group', path: g })),
  ...(tags.value || []).map(t => ({ kind: 'tag', path: t }))
])
const scopeRows = computed(() => (scopes.value || []).map(s => {
  const p = policyOf('scope', s.path)
  return { path: s.path, dim: s.dim, hasPolicy: !!p, level: p ? p.level : -1, pid: p ? p.id : null }
}))

async function loadAll() {
  loading.value = true
  try {
    const subj = await request.get('/data-policy/subjects')
    roles.value = subj.roles || []
    users.value = subj.users || []
    const res = await request.get('/data-policy/resources')
    tables.value = res.datasets || []
    tags.value = (res.tags || []).sort()
    groups.value = (res.groups || [])
    scopes.value = res.scopes || []
    await loadPolicies()
    await loadSubs()
  } catch (e) { /* 403/未登录已提示 */ } finally { loading.value = false }
}
async function loadPolicies() {
  const rows = await request.get('/data-policy', { params: st.value })
  policies.value = rows || []
  await loadColumns()
}
async function loadSubs() {
  try {
    const rows = await request.get('/tagengine/subs')
    subs.value = rows || []
  } catch (e) { subs.value = [] }
}
async function loadColumns() {
  if (!curTable.value) return
  colLoading.value = true
  try {
    const d = await request.get(`/tagengine/datasets/${encodeURIComponent(curTable.value)}/fields`)
    const fields = (d && d.fields) || []
    columns.value = fields.map(f => ({ name: f.name }))
  } catch (e) {
    try {
      const f = await request.get(`/tagengine/datasets/${encodeURIComponent(curTable.value)}/fields`, { params: {} })
      columns.value = ((f && f.fields) || []).map(x => ({ name: x.name }))
    } catch (e2) { columns.value = [] }
  } finally { colLoading.value = false }
}
async function pickTable(name) {
  curTable.value = name
  columns.value = []
  await loadColumns()
}
async function setLevel(type, path, level, dim) {
  const r = await request.post('/data-policy', { subjectType: st.value.subjectType, subjectId: st.value.subjectId, resType: type, resPath: path, level, eff: 'grant', dim })
  if (r) { ElMessage.success(`已设 ${path} → L${level}`); await loadPolicies() }
}
async function setColumn(name, level) { await setLevel('column', curTable.value + '.' + name, level, '') }
async function setTag(path, level) { await setLevel('tag', path, level, '') }
async function delPolicy(id) {
  await request.delete(`/data-policy/${id}`)
  ElMessage.success('已清除策略')
  await loadPolicies()
}

/* 行规则 */
const ruleDlg = ref(false)
const ruleForm = ref({ name: '', kind: 'cond', cond: '', tagRef: '' })
async function openRowRule() {
  ruleForm.value = { name: '', kind: 'cond', cond: '', tagRef: '' }
  ruleDlg.value = true
}
async function saveRowRule() {
  await request.post('/data-policy/row-rule', {
    name: ruleForm.value.name || '未命名', resType: 'dataset', resPath: curTable.value,
    kind: ruleForm.value.kind,
    condJson: ruleForm.value.kind === 'cond' ? ruleForm.value.cond : null,
    tagRefPath: ruleForm.value.kind === 'tag_ref' ? ruleForm.value.tagRef : null,
    listText: null
  })
  ElMessage.success('行规则已保存')
  ruleDlg.value = false
  await loadRowRules()
}
async function loadRowRules() {
  try {
    const rows = await request.get('/data-policy/row-rule', { params: { resType: 'dataset', resPath: curTable.value } })
    rowRules.value = rows || []
  } catch (e) { rowRules.value = [] }
}
async function delRowRule(id) {
  await request.delete(`/data-policy/row-rule/${id}`)
  await loadRowRules()
}
async function exportSub(row) {
  try {
    const d = await request.post(`/tagengine/subs/${row.id}/export`)
    ElMessage.success(`已导出 ${d.exported} 条(审计有痕)`)
  } catch (e) { /* 403/400 提示 */ }
}

watch(st, () => loadPolicies(), { deep: true })
onMounted(loadAll)
</script>

<style scoped>
.page-card { max-width: 1560px; }
.pg-title { font-size: 17px; font-weight: 700; }
.flex { display: flex; }
.flex-between { display: flex; justify-content: space-between; align-items: center; }
.subject-bar { display: flex; align-items: center; gap: 8px; flex-wrap: wrap; }
.panel-tip { background: #f0f7ff; border: 1px solid #c6e0f7; color: #2a5d8f; padding: 8px 10px; border-radius: 6px; font-size: 12px; }
.sub-warn { background: #fdf6ec; border: 1px solid #f3d19e; color: #7a5b1f; padding: 10px 12px; border-radius: 6px; font-size: 13px; line-height: 1.8; }
.side { flex-shrink: 0; }
.side :deep(.el-card__body) { padding: 8px; max-height: 480px; overflow: auto; }
.trow { display: flex; align-items: center; gap: 6px; padding: 6px 8px; cursor: pointer; border-radius: 6px; font-size: 13px; }
.trow:hover { background: var(--el-fill-color-light); }
.trow.on { background: var(--el-color-primary-light-9); font-weight: 600; }
.rrline { display: flex; align-items: center; gap: 8px; margin: 4px 0; }
.dim { color: #999; font-size: 12px; }
.lv { font-weight: 600; }
.lv0 { color: #909399; } .lv1 { color: #409eff; } .lv2 { color: #e6a23c; } .lv3 { color: #67c23a; }
</style>
