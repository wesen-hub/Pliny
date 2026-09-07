<template>
  <div class="page-card">
    <el-card shadow="never" v-loading="loading">
      <div class="flex" style="justify-content:space-between;align-items:center;margin-bottom:4px">
        <div>
          <div class="pg-title">标签注册表(V2)</div>
          <el-text size="small" type="info">注册表 = tag_dict 树节点(标签身份)+ 可计算/可打标声明;timing 自动推导 compute/store(sliding/instant 必填 TTL),取值域 domain 供发布校验。已接后端 /api/tagengine/registry + /api/tags/tree。</el-text>
        </div>
        <el-button type="primary" @click="openCreate">＋ 注册标签</el-button>
      </div>

      <el-table :data="rows" size="small" border stripe style="margin-top:10px">
        <el-table-column label="标签路径" min-width="220">
          <template #default="{ row }"><el-text size="small" style="font-weight:600">{{ row.tagPath }}</el-text></template>
        </el-table-column>
        <el-table-column label="结构" width="90"><template #default="{ row }"><el-tag size="small" effect="plain">{{ row.valueType }}</el-tag></template></el-table-column>
        <el-table-column label="基数" width="70" align="center"><template #default="{ row }">{{ row.cardinality }}</template></el-table-column>
        <el-table-column label="时效" width="100"><template #default="{ row }"><el-tag size="small" :type="row.timing === 'instant' ? 'success' : 'info'" effect="plain">{{ row.timing }}</el-tag></template></el-table-column>
        <el-table-column label="计算模式" width="100"><template #default="{ row }">{{ row.compute }}</template></el-table-column>
        <el-table-column label="存储" width="120"><template #default="{ row }">{{ row.store }}</template></el-table-column>
        <el-table-column label="TTL" width="70"><template #default="{ row }">{{ row.ttlDays || '—' }}</template></el-table-column>
        <el-table-column label="取值域" min-width="180">
          <template #default="{ row }">
            <el-tag v-for="d in (row.domain || [])" :key="d" size="small" effect="plain" style="margin-right:4px">{{ d }}</el-tag>
            <el-text v-if="!row.domain || !row.domain.length" size="small" type="info">—</el-text>
          </template>
        </el-table-column>
        <el-table-column label="说明" min-width="120"><template #default="{ row }"><el-text size="small" type="info">{{ row.descr }}</el-text></template></el-table-column>
        <el-table-column label="操作" width="140" fixed="right">
          <template #default="{ row }">
            <el-button size="small" text type="danger" @click="del(row)">取消注册</el-button>
          </template>
        </el-table-column>
      </el-table>
    </el-card>

    <!-- ═══ 注册 dialog ═══ -->
    <el-dialog v-model="dlg" title="注册标签(挂 tag_dict 节点 + 声明)" width="640">
      <el-form label-width="110px">
        <el-form-item label="标签节点">
          <el-select v-model="form.tagDictId" filterable style="width:420px" placeholder="选择树节点(标签路径)">
            <el-option v-for="n in nodes" :key="n.id" :value="n.id" :label="n.path + ' (' + n.tag + ')'" />
          </el-select>
        </el-form-item>
        <el-form-item label="结构 value_type">
          <el-select v-model="form.valueType" style="width:220px">
            <el-option v-for="v in ['boolean', 'enum1', 'enumN', 'number', 'score', 'datetime']" :key="v" :value="v" :label="v" />
          </el-select>
        </el-form-item>
        <el-form-item label="基数 cardinality">
          <el-radio-group v-model="form.cardinality">
            <el-radio-button value="single">single</el-radio-button>
            <el-radio-button value="multi">multi</el-radio-button>
          </el-radio-group>
        </el-form-item>
        <el-form-item label="时效 timing">
          <el-select v-model="form.timing" style="width:220px">
            <el-option v-for="v in ['static', 'slow', 'accumulate', 'sliding', 'instant']" :key="v" :value="v" :label="v" />
          </el-select>
        </el-form-item>
        <el-form-item label="计算/存储">
          <el-text size="small" type="info">{{ inferHint }} —— compute={{ form.compute }} store={{ form.store }}(可改)</el-text>
          <div style="display:flex;gap:8px;margin-top:4px">
            <el-select v-model="form.compute" size="small" style="width:130px"><el-option v-for="v in ['batch','stream','online']" :key="v" :value="v" :label="v" /></el-select>
            <el-select v-model="form.store" size="small" style="width:150px"><el-option v-for="v in ['fact','online','bitmap','fact+online']" :key="v" :value="v" :label="v" /></el-select>
          </div>
        </el-form-item>
        <el-form-item v-if="['sliding','instant'].includes(form.timing)" label="TTL(天)">
          <el-input-number v-model="form.ttlDays" :min="1" style="width:160px" />
          <el-text size="small" type="danger" style="margin-left:8px">sliding/instant 必填</el-text>
        </el-form-item>
        <el-form-item label="取值域">
          <el-select v-model="form.domain" multiple filterable allow-create default-first-option style="width:380px" placeholder="逐项回车添加, 如 大额/中额/小额" />
        </el-form-item>
        <el-form-item label="说明">
          <el-input v-model="form.descr" style="width:420px" placeholder="可选" />
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="dlg = false">取消</el-button>
        <el-button type="primary" :disabled="!form.tagDictId || !form.valueType" @click="save">注册</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import { registryApi, tagTreeApi } from '../../api/tagengine.js'

const toast = (m, t) => (t ? ElMessage[t](m) : ElMessage.info(m))
const loading = ref(false)
const rows = ref([])
const dlg = ref(false)
const nodes = ref([])
const INFER = {
  static: { c: 'batch', s: 'fact' }, slow: { c: 'batch', s: 'fact' }, accumulate: { c: 'batch', s: 'fact' },
  sliding: { c: 'stream', s: 'fact+online' }, instant: { c: 'online', s: 'online' }
}
const form = ref({ tagDictId: null, valueType: 'boolean', cardinality: 'single', timing: 'static', compute: 'batch', store: 'fact', ttlDays: null, domain: [], descr: '' })
const inferHint = computed(() => {
  const inf = INFER[form.value.timing]
  return inf ? 'timing=' + form.value.timing + ' → 默认 compute=' + inf.c + ' store=' + inf.s : ''
})

async function load() {
  loading.value = true
  try { rows.value = await registryApi.list() } catch (e) { /* 已提示 */ } finally { loading.value = false }
}
onMounted(async () => {
  load()
  try { nodes.value = await tagTreeApi.flatten() } catch (e) { /* 已提示 */ }
})

function openCreate() {
  form.value = { tagDictId: null, valueType: 'boolean', cardinality: 'single', timing: 'static', compute: 'batch', store: 'fact', ttlDays: null, domain: [], descr: '' }
  dlg.value = true
}
function save() {
  const inf = INFER[form.value.timing]
  registryApi.create({ ...form.value, compute: form.value.compute || inf.c, store: form.value.store || inf.s, status: 1 })
    .then(() => { toast('注册成功'); dlg.value = false; load() })
    .catch(() => {})
}
function del(row) {
  ElMessageBox.confirm('取消注册「' + row.tagPath + '」? 不影响 tag_dict 节点。', '取消注册', { type: 'warning' })
    .then(() => registryApi.remove(row.id).then(() => { toast('已取消'); load() }).catch(() => {}))
    .catch(() => {})
}
</script>

<style scoped>
.page-card { max-width: 1380px; }
.pg-title { font-size: 17px; font-weight: 700; margin-bottom: 4px; }
.flex { display: flex; }
</style>
