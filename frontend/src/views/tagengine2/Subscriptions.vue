<template>
  <div class="page-card">
    <el-card shadow="never">
      <div class="flex-between">
        <div class="pg-title">标签订阅 V2<el-text type="info" style="margin-left:8px;font-weight:400">订阅 = 标签条件 · 命中实体数/变更流来自执行端审计(tag_notify: 批/流/在线三通道)</el-text></div>
        <el-button type="primary" @click="openCreate">＋ 新建订阅</el-button>
      </div>

      <el-table :data="subs" stripe size="small" v-loading="loading" style="margin-top:12px">
        <el-table-column prop="name" label="订阅名" min-width="140" />
        <el-table-column label="标签条件" min-width="220">
          <template #default="{ row }">
            <el-tag size="small" type="primary" effect="plain">{{ row.tag_path }}</el-tag>
            <el-tag v-if="row.tag_value" size="small" style="margin-left:4px">{{ row.tag_value }}</el-tag>
            <el-text v-else size="small" type="info" style="margin-left:4px">(任意值)</el-text>
          </template>
        </el-table-column>
        <el-table-column label="命中实体" width="100" align="center">
          <template #default="{ row }"><el-text size="small" :type="row.hit_entities ? 'success' : 'info'">{{ row.hit_entities ?? '—' }}</el-text></template>
        </el-table-column>
        <el-table-column label="最近变更" width="170">
          <template #default="{ row }"><el-text size="small" type="info">{{ row.latest_at ? String(row.latest_at).slice(0, 19).replace('T', ' ') : '—' }}</el-text></template>
        </el-table-column>
        <el-table-column label="状态" width="80">
          <template #default="{ row }">
            <el-tag size="small" :type="row.status === 1 ? 'success' : 'info'" effect="plain">{{ row.status === 1 ? '启用' : '暂停' }}</el-tag>
          </template>
        </el-table-column>
        <el-table-column label="操作" width="130">
          <template #default="{ row }">
            <el-button size="small" text :type="row.status === 1 ? 'warning' : 'success'" @click="toggle(row)">{{ row.status === 1 ? '暂停' : '启用' }}</el-button>
            <el-button size="small" text type="danger" @click="del(row)">删</el-button>
          </template>
        </el-table-column>
      </el-table>
    </el-card>

    <el-card shadow="never" style="margin-top:16px">
      <template #header>
        <div class="flex-between">
          <span>标签变更流(最近 30 条 · 执行端审计)</span>
          <el-select v-model="modeFilter" size="small" style="width:140px" clearable placeholder="按通道过滤" @change="loadNotify">
            <el-option label="批 batch" value="batch" /><el-option label="流 stream" value="stream" /><el-option label="在线 online" value="online" />
          </el-select>
        </div>
      </template>
      <el-table :data="notifies" size="small" v-loading="notifyLoading">
        <el-table-column label="时间" width="170"><template #default="{ row }"><el-text size="small" type="info">{{ String(row.occur_ts).slice(0, 19).replace('T', ' ') }}</el-text></template></el-table-column>
        <el-table-column prop="entity" label="实体" width="130" />
        <el-table-column label="标签" min-width="200">
          <template #default="{ row }"><el-tag size="small" type="primary" effect="plain">{{ row.tag_path }}</el-tag></template>
        </el-table-column>
        <el-table-column label="值" width="120"><template #default="{ row }">{{ row.tag_value ?? '∅' }}</template></el-table-column>
        <el-table-column label="任务" width="110"><template #default="{ row }">#{{ row.task_id }}</template></el-table-column>
        <el-table-column label="通道" width="90">
          <template #default="{ row }"><el-tag size="small" :type="row.task_mode === 'batch' ? 'success' : row.task_mode === 'stream' ? 'warning' : 'info'" effect="plain">{{ row.task_mode }}</el-tag></template>
        </el-table-column>
      </el-table>
    </el-card>

    <!-- 新建订阅 -->
    <el-dialog v-model="dlg" title="新建标签订阅" width="480px">
      <el-form label-width="90px">
        <el-form-item label="订阅名"><el-input v-model="form.name" placeholder="如 黑名单监控" /></el-form-item>
        <el-form-item label="标签">
          <el-select v-model="form.tagPath" filterable style="width:100%" placeholder="选标签(自标签树)">
            <el-option v-for="t in tagOpts" :key="t.path" :label="t.path" :value="t.path" />
          </el-select>
        </el-form-item>
        <el-form-item label="值(可选)"><el-input v-model="form.tagValue" placeholder="留空 = 任意值(命中即订阅)" /></el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="dlg = false">取消</el-button>
        <el-button type="primary" :loading="saving" @click="create">创建</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup>
import { ref, onMounted } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import { subApi, notifyApi, tagTreeApi } from '../../api/tagengine.js'

const toast = (m, t) => (t ? ElMessage[t](m) : ElMessage.info(m))
const loading = ref(false)
const notifyLoading = ref(false)
const subs = ref([])
const notifies = ref([])
const modeFilter = ref('')
const dlg = ref(false)
const saving = ref(false)
const tagOpts = ref([])
const form = ref({ name: '', tagPath: '', tagValue: '' })

async function load() {
  loading.value = true
  try { subs.value = await subApi.list() } catch (e) { /* 已提示 */ } finally { loading.value = false }
}
async function loadNotify() {
  notifyLoading.value = true
  try {
    const d = await notifyApi.list({ limit: 30, taskMode: modeFilter.value || undefined })
    notifies.value = (d && d.list) || []
  } catch (e) { /* 已提示 */ } finally { notifyLoading.value = false }
}
async function openCreate() {
  if (!tagOpts.value.length) {
    try { tagOpts.value = await tagTreeApi.flatten() } catch (e) { /* 已提示 */ }
  }
  form.value = { name: '', tagPath: '', tagValue: '' }
  dlg.value = true
}
function create() {
  if (!form.value.name || !form.value.tagPath) { toast('订阅名与标签必填', 'warning'); return }
  saving.value = true
  subApi.create({ ...form.value, tagValue: form.value.tagValue || undefined })
    .then(() => { toast('订阅已创建'); dlg.value = false; load(); loadNotify() })
    .catch(() => {})
    .finally(() => { saving.value = false })
}
function toggle(row) {
  subApi.update(row.id, { status: row.status === 1 ? 0 : 1 })
    .then(() => { toast(row.status === 1 ? '已暂停' : '已启用'); load() })
    .catch(() => {})
}
function del(row) {
  ElMessageBox.confirm('删除订阅「' + row.name + '」? 仅删订阅条件, 不影响标签数据。', '删除订阅', { type: 'warning' })
    .then(() => subApi.remove(row.id).then(() => { toast('已删除'); load() }).catch(() => {}))
    .catch(() => {})
}
onMounted(() => { load(); loadNotify() })
</script>

<style scoped>
.page-card { max-width: 1400px; }
.pg-title { font-size: 17px; font-weight: 700; }
.flex-between { display: flex; justify-content: space-between; align-items: center; }
</style>
