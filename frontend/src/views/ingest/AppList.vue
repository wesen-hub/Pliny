<template>
  <div class="page-card">
    <el-alert type="success" :closable="false" show-icon style="margin-bottom: 16px"
      title="事件接入: 新建「接入应用」即领取 appKey(任务随之产生),对方带 key 提交消息(op 在消息包内指定);平台按 appKey 鉴权并聚合该任务的同步情况(ok/死信/表级 lag)。提交 → dcp.in.{逻辑表} 削峰 → 验证 → dcp.{逻辑表} → MySQL" />

    <el-card shadow="never">
      <div class="toolbar">
        <div>
          <el-radio-group v-model="filter" size="small">
            <el-radio-button value="">全部</el-radio-button>
            <el-radio-button value="ENABLED">启用中</el-radio-button>
            <el-radio-button value="DISABLED">已停用</el-radio-button>
          </el-radio-group>
          <el-text size="small" type="info" style="margin-left:12px">自动刷新: <el-switch v-model="autoRefresh" size="small" /></el-text>
        </div>
        <div>
          <el-button size="small" :loading="loading" @click="load">刷新</el-button>
          <el-button type="primary" @click="openCreate">＋ 新建接入应用(领 appKey)</el-button>
        </div>
      </div>

      <el-table :data="filtered" stripe v-loading="loading">
        <el-table-column label="应用名称" min-width="130">
          <template #default="{ row }">
            <el-link type="primary" @click="router.push(`/ingest/app/${row.app_key}`)">{{ row.app_name }}</el-link>
          </template>
        </el-table-column>
        <el-table-column label="appKey" width="190">
          <template #default="{ row }">
            <code class="key">{{ row.app_key }}</code>
            <el-link type="primary" underline="never" style="margin-left:6px" @click="copy(row.app_key)">复制</el-link>
          </template>
        </el-table-column>
        <el-table-column label="状态" width="90">
          <template #default="{ row }">
            <el-tag size="small" :type="row.status === 'ENABLED' ? 'success' : 'info'" effect="plain">
              {{ row.status === 'ENABLED' ? '启用' : '停用' }}</el-tag>
          </template>
        </el-table-column>
        <el-table-column label="绑定逻辑表(白名单)" min-width="180">
          <template #default="{ row }">
            <el-tag v-for="lt in row.logicalTables || []" :key="lt" size="small" effect="plain" style="margin-right:4px">{{ lt }}</el-tag>
          </template>
        </el-table-column>
        <el-table-column label="已处理" width="90">
          <template #default="{ row }">{{ row.ok ?? 0 }}</template>
        </el-table-column>
        <el-table-column label="死信" width="90">
          <template #default="{ row }">
            <el-link v-if="row.dlt" type="danger" :underline="false" @click="router.push(`/ingest/app/${row.app_key}`)">{{ row.dlt }}</el-link>
            <el-text v-else size="small" type="info">0</el-text>
          </template>
        </el-table-column>
        <el-table-column label="操作" width="150">
          <template #default="{ row }">
            <el-link type="primary" underline="never" @click="router.push(`/ingest/app/${row.app_key}`)">监控</el-link>
            <el-link v-if="row.status === 'ENABLED'" type="danger" underline="never" style="margin-left:10px" @click="toggle(row, false)">停用</el-link>
            <el-link v-else type="success" underline="never" style="margin-left:10px" @click="toggle(row, true)">启用</el-link>
            <el-link type="danger" underline="never" style="margin-left:10px" @click="remove(row)">删除</el-link>
          </template>
        </el-table-column>
      </el-table>
      <div class="tip">停用 = 禁止该 key 提交(401);某逻辑表无任何启用中的应用时,该表入站消费自动停止。ok 计数存内存(应用重启归零),死信落库可持久查询</div>
    </el-card>

    <!-- 新建应用 -->
    <el-dialog v-model="createVisible" title="新建接入应用(领取 appKey)" width="560px">
      <el-form label-width="110px">
        <el-form-item label="应用名称" required>
          <el-input v-model="form.appName" placeholder="业务系统名, 如: OMS 订单服务" />
        </el-form-item>
        <el-form-item label="绑定逻辑表" required>
          <el-select v-model="form.logicalTables" multiple filterable style="width:100%" placeholder="该应用可提交哪些逻辑表(白名单)">
            <el-option v-for="m in models" :key="m.table_name" :value="m.table_name"
              :label="`${m.table_name} (${m.name || m.table_name})`" />
          </el-select>
        </el-form-item>
        <el-form-item label="描述">
          <el-input v-model="form.description" type="textarea" :rows="2" placeholder="对接人/用途, 可选" />
        </el-form-item>
        <el-alert type="info" :closable="false" show-icon title="创建后平台颁发 appKey(仅显示一次);op 由消息包内指定,页面不配置操作类型" />
      </el-form>
      <template #footer>
        <el-button @click="createVisible = false">取消</el-button>
        <el-button type="primary" :disabled="!form.appName || !form.logicalTables.length" :loading="saving" @click="create">创建并颁发 appKey</el-button>
      </template>
    </el-dialog>

    <!-- 颁发结果 -->
    <el-dialog v-model="keyVisible" title="接入凭证(请立即保存, 只显示一次)" width="620px">
      <el-alert type="warning" :closable="false" show-icon style="margin-bottom:12px"
        title="appKey 是对方提交消息的凭证(Authorization: Bearer),请安全交给业务系统;丢失需重建应用" />
      <el-descriptions :column="1" border size="small">
        <el-descriptions-item label="应用名称">{{ issued.appName }}</el-descriptions-item>
        <el-descriptions-item label="appKey">
          <code class="key">{{ issued.appKey }}</code>
          <el-button size="small" style="margin-left:10px" @click="copy(issued.appKey)">复制</el-button>
        </el-descriptions-item>
      </el-descriptions>
      <div class="sec-title" style="margin-top:14px">接入示例(HTTP, 任意语言)</div>
      <pre class="code">curl -X POST http://localhost:8081/api/ingest/{{ issued.logicalTables[0] }} \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer {{ issued.appKey }}' \
  -d '{"op":"I","data":{"字段1":"值1","字段2":"值2"}}'   # op: I/U/D, 缺省 INSERT</pre>
      <template #footer>
        <el-button type="primary" @click="keyVisible = false; load()">我已保存, 关闭</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup>
import { ref, computed, onMounted, onUnmounted } from 'vue'
import { useRouter } from 'vue-router'
import { ElMessage, ElMessageBox } from 'element-plus'
import request from '../../api/request'

const router = useRouter()
const filter = ref('')
const autoRefresh = ref(true)
const loading = ref(false)
const saving = ref(false)
const apps = ref([])
const models = ref([])
const createVisible = ref(false)
const keyVisible = ref(false)
const issued = ref({})
const form = ref({ appName: '', description: '', logicalTables: [] })
let timer = null

async function load() {
  loading.value = true
  try {
    apps.value = (await request.get('/ingest/apps')) || []
  } catch (e) {
    ElMessage.error('加载失败: ' + (e?.message || e))
  } finally {
    loading.value = false
  }
}

function startAuto() {
  stopAuto()
  timer = setInterval(() => { if (autoRefresh.value) load() }, 5000)
}
function stopAuto() { if (timer) { clearInterval(timer); timer = null } }
onMounted(() => { load(); startAuto() })
onUnmounted(stopAuto)

const filtered = computed(() => apps.value.filter(a => !filter.value || a.status === filter.value))

async function openCreate() {
  try {
    models.value = (await request.get('/models')) || []
  } catch (e) { /* 静默 */ }
  form.value = { appName: '', description: '', logicalTables: [] }
  createVisible.value = true
}

async function create() {
  saving.value = true
  try {
    issued.value = await request.post('/ingest/apps', form.value)
    createVisible.value = false
    keyVisible.value = true
  } catch (e) {
    ElMessage.error('创建失败: ' + (e?.message || e))
  } finally {
    saving.value = false
  }
}

async function toggle(row, enable) {
  try {
    await request.post(`/ingest/apps/${row.app_key}/${enable ? 'enable' : 'disable'}`)
    ElMessage.success(enable ? '已启用' : '已停用(该 key 提交将被拒绝)')
    await load()
  } catch (e) {
    ElMessage.error('操作失败: ' + (e?.message || e))
  }
}

async function remove(row) {
  await ElMessageBox.confirm(`删除接入应用 ${row.app_name}?(历史死信保留, 已入库数据不受影响)`, '提示', { type: 'warning' })
  try {
    await request.delete(`/ingest/apps/${row.app_key}`)
    ElMessage.success('已删除')
    await load()
  } catch (e) {
    ElMessage.error('删除失败: ' + (e?.message || e))
  }
}

async function copy(text) {
  try {
    await navigator.clipboard.writeText(text)
    ElMessage.success('已复制: ' + text)
  } catch (e) {
    ElMessage.warning('复制失败, 请手动选择复制')
  }
}
</script>

<style scoped>
.toolbar { display: flex; justify-content: space-between; margin-bottom: 14px; align-items: center; }
.tip { margin-top: 12px; font-size: 12px; color: #8f959e; }
.key { background: #f6f8fa; border: 1px solid #eee; border-radius: 4px; padding: 2px 6px; font-size: 12px; font-family: ui-monospace, Menlo, monospace; }
.sec-title { font-weight: 600; margin: 4px 0 10px; font-size: 13px; }
.code { background: #f6f8fa; border: 1px solid #eee; border-radius: 6px; padding: 10px; font-size: 12px; font-family: ui-monospace, Menlo, monospace; overflow-x: auto; }
</style>
