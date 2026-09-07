<template>
  <div class="page-card">
    <el-card shadow="never" v-loading="loading">
      <template v-if="app">
        <div class="hd">
          <div>
            <div class="hd-name">
              <el-tag size="small" type="warning" effect="plain" style="margin-right:8px">事件接入</el-tag>
              <b>{{ app.app_name }}</b>
              <el-tag size="small" :type="app.status === 'ENABLED' ? 'success' : 'info'" effect="plain" style="margin-left:8px">
                {{ app.status === 'ENABLED' ? '启用' : '停用' }}</el-tag>
            </div>
            <div class="hd-desc">
              appKey: <code class="key">{{ app.app_key }}</code>
              <el-link type="primary" underline="never" style="margin-left:6px" @click="copy(app.app_key)">复制</el-link>
              · {{ app.description || '无描述' }}
            </div>
          </div>
          <div>
            <el-button size="small" @click="router.push('/ingest/app')">← 返回</el-button>
            <el-button size="small" type="danger" plain v-if="app.status === 'ENABLED'" @click="toggle(false)">停用</el-button>
            <el-button size="small" type="success" plain v-else @click="toggle(true)">启用</el-button>
          </div>
        </div>

        <!-- 指标 -->
        <el-row :gutter="12" style="margin-top:14px">
          <el-col :span="8"><el-card shadow="never" class="stat"><div class="stat-num">{{ app.ok ?? 0 }}</div><div class="stat-label">已处理(该应用提交 → MySQL)</div></el-card></el-col>
          <el-col :span="8"><el-card shadow="never" class="stat"><div class="stat-num" style="color:#f56c6c">{{ app.dlt ?? 0 }}</div><div class="stat-label">死信(该应用, 落库可查)</div></el-card></el-col>
          <el-col :span="8"><el-card shadow="never" class="stat"><div class="stat-num">{{ rate }}</div><div class="stat-label">处理速率(条/秒)</div></el-card></el-col>
        </el-row>

        <!-- 吞吐图 -->
        <el-card shadow="never" style="margin-top:12px">
          <template #header><b>近 60s 吞吐(绿=成功, 红=死信)</b></template>
          <div class="chart">
            <div v-for="(s, i) in app.samples || []" :key="i" class="bar-wrap" :title="`${s.t} ok=${s.ok} dlt=${s.dlt}`">
              <div class="bar-stack">
                <div class="bar bar-ok" :style="{ height: barH(s.ok) }" />
                <div class="bar bar-dlt" :style="{ height: barH(s.dlt) }" />
              </div>
            </div>
            <el-text v-if="!(app.samples || []).length" size="small" type="info">暂无流量(用下方 curl 提交后出图)</el-text>
          </div>
        </el-card>

        <!-- 绑定表 -->
        <el-card shadow="never" style="margin-top:12px">
          <template #header>
            <div class="flex-between">
              <b>绑定逻辑表(白名单)</b>
              <el-button size="small" @click="openBind">管理绑定</el-button>
            </div>
          </template>
          <el-table :data="app.tables || []" size="small" border stripe>
            <el-table-column prop="logicalTable" label="逻辑表" min-width="150" />
            <el-table-column label="入站消费" width="100">
              <template #default="{ row }">
                <el-tag size="small" :type="row.running ? 'success' : 'info'" effect="plain">{{ row.running ? '运行中' : '未运行' }}</el-tag>
              </template>
            </el-table-column>
            <el-table-column label="本应用已处理" width="120">
              <template #default="{ row }">{{ row.ok ?? 0 }}</template>
            </el-table-column>
            <el-table-column label="本应用死信" width="110">
              <template #default="{ row }">
                <el-link v-if="row.dlt" type="danger" :underline="false" @click="dltTab = true">{{ row.dlt }}</el-link>
                <el-text v-else size="small" type="info">0</el-text>
              </template>
            </el-table-column>
            <el-table-column label="表级 lag" width="100">
              <template #default="{ row }">{{ row.lag === -1 ? '—' : row.lag }}</template>
            </el-table-column>
          </el-table>
          <el-text size="small" type="info">lag 为该逻辑表入站消费共享指标(dcp.in.{{ app.app_key ? '' : '' }} 组 dcp-ingest-{表});ok 计数存内存,应用重启归零</el-text>
        </el-card>

        <!-- 接入说明 + 死信 -->
        <el-tabs v-model="tab" style="margin-top:12px">
          <el-tab-pane label="接入说明" name="howto">
            <el-descriptions :column="1" border size="small" style="max-width:900px">
              <el-descriptions-item label="SDK bootstrap">
                <code class="code">GET /api/ingest/meta/{逻辑表}</code> + <code class="code">Authorization: Bearer {{ app.app_key }}</code> → keyColumns(业务键), 用于拼 Kafka key 保序
              </el-descriptions-item>
              <el-descriptions-item label="HTTP 提交">
                <pre class="code">curl -X POST http://localhost:8081/api/ingest/t_order_model \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer {{ app.app_key }}' \
  -d '{"op":"U","data":{"order_id":"A1001","amount":299.00,"status":"PAID"}}'
# 200 已入队(异步入库) / 400 预检未通过(原因在 message) / 401 key 无效·停用 / 403 未绑定该表</pre>
              </el-descriptions-item>
              <el-descriptions-item label="op 语义">
                op 在消息包内指定(I/U/D 或 INSERT/UPDATE/DELETE), 缺省按 INSERT; 页面不配置操作类型
              </el-descriptions-item>
              <el-descriptions-item label="字段规则">
                与逻辑表字段同名直取、不映射; 多余字段忽略; 业务键不传(平台按逻辑表 key 字段从 payload 拼接); 类型按模型字段校验, 失败 400/DLT
              </el-descriptions-item>
              <el-descriptions-item label="链路">
                dcp.in.{逻辑表}(削峰) → 验证 → 失败 DLT(source_table={{ app.app_key }}) / 通过: 领 seq + 信封 → dcp.{逻辑表} → 攒批 ODKU → MySQL
              </el-descriptions-item>
            </el-descriptions>
          </el-tab-pane>
          <el-tab-pane name="dlt">
            <template #label>死信({{ app.dlt || 0 }})</template>
            <div class="toolbar-sm">
              <el-button size="small" type="primary" plain @click="goDltAll">查看全部死信</el-button>
              <el-text size="small" type="info">按 appKey={{ app.app_key }} 过滤;修正消息后重放(仍不过会再次入死信)</el-text>
            </div>
            <el-table :data="dltRows" size="small" border stripe v-loading="dltLoading">
              <el-table-column prop="id" label="ID" width="70" />
              <el-table-column prop="logical_table" label="逻辑表" width="140" />
              <el-table-column label="分区/offset" width="120">
                <template #default="{ row }">{{ row.partition_id }}/{{ row.offset_id }}</template>
              </el-table-column>
              <el-table-column prop="error_msg" label="原因" min-width="240" />
              <el-table-column prop="last_fail_time" label="时间" width="160" />
              <el-table-column label="操作" width="80">
                <template #default="{ row }">
                  <el-link type="primary" underline="never" @click="replay(row)">重放</el-link>
                </template>
              </el-table-column>
            </el-table>
          </el-tab-pane>
        </el-tabs>
      </template>
    </el-card>

    <!-- 管理绑定 -->
    <el-dialog v-model="bindVisible" title="管理绑定逻辑表" width="520px">
      <el-select v-model="bindTables" multiple filterable style="width:100%" placeholder="选择逻辑表(白名单, 整体替换)">
        <el-option v-for="m in models" :key="m.table_name" :value="m.table_name"
          :label="`${m.table_name} (${m.name || m.table_name})`" />
      </el-select>
      <template #footer>
        <el-button @click="bindVisible = false">取消</el-button>
        <el-button type="primary" @click="saveBind">保存</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup>
import { ref, computed, onMounted, onUnmounted } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { ElMessage } from 'element-plus'
import request from '../../api/request'

const route = useRoute()
const router = useRouter()
const appKey = route.params.appKey
const app = ref(null)
const loading = ref(false)
const dltRows = ref([])
const dltLoading = ref(false)
const tab = ref('howto')
const dltTab = ref(false)
const bindVisible = ref(false)
const bindTables = ref([])
const models = ref([])
let timer = null

const rate = computed(() => {
  const s = app.value?.samples || []
  if (!s.length) return 0
  const last = s[s.length - 1]
  return last.ok + last.dlt
})

async function load() {
  loading.value = true
  try {
    app.value = await request.get(`/ingest/apps/${appKey}`)
  } catch (e) {
    ElMessage.error('加载失败: ' + (e?.message || e))
  } finally {
    loading.value = false
  }
}

async function loadDlt() {
  dltLoading.value = true
  try {
    const res = await request.get('/sync/dlt', {
      params: { sourceTable: appKey, status: 'PENDING', page: 1, pageSize: 10 }
    })
    dltRows.value = (res && res.list) || []
  } catch (e) {
    dltRows.value = []
  } finally {
    dltLoading.value = false
  }
}

function startAuto() {
  stopAuto()
  timer = setInterval(() => { if (app.value?.status === 'ENABLED') load() }, 5000)
}
function stopAuto() { if (timer) { clearInterval(timer); timer = null } }
onMounted(() => { load(); loadDlt(); startAuto() })
onUnmounted(stopAuto)

async function toggle(enable) {
  try {
    await request.post(`/ingest/apps/${appKey}/${enable ? 'enable' : 'disable'}`)
    ElMessage.success(enable ? '已启用' : '已停用')
    await load()
  } catch (e) {
    ElMessage.error('操作失败: ' + (e?.message || e))
  }
}

async function openBind() {
  try {
    models.value = (await request.get('/models')) || []
  } catch (e) { /* 静默 */ }
  bindTables.value = (app.value?.tables || []).map(t => t.logicalTable)
  bindVisible.value = true
}

async function saveBind() {
  try {
    await request.post(`/ingest/apps/${appKey}/bind`, { logicalTables: bindTables.value })
    ElMessage.success('已保存')
    bindVisible.value = false
    await load()
  } catch (e) {
    ElMessage.error('保存失败: ' + (e?.message || e))
  }
}

function goDltAll() { router.push('/sync/dlt') }

async function replay(row) {
  try {
    await request.post(`/sync/dlt/${row.id}/replay`)
    ElMessage.success('已重放(仍不过会再次入死信)')
    await loadDlt(); await load()
  } catch (e) {
    ElMessage.error('重放失败: ' + (e?.message || e))
  }
}

async function copy(text) {
  try {
    await navigator.clipboard.writeText(text)
    ElMessage.success('已复制')
  } catch (e) {
    ElMessage.warning('复制失败')
  }
}

function barH(v) {
  const arr = app.value?.samples || []
  const max = Math.max(1, ...arr.map(s => Math.max(s.ok, s.dlt)))
  return Math.max(v ? Math.round(v / max * 56) : 1, v ? 2 : 0) + 'px'
}
</script>

<style scoped>
.hd { display: flex; justify-content: space-between; align-items: flex-start; }
.hd-name { font-size: 16px; }
.hd-desc { margin-top: 6px; font-size: 12px; color: #8f959e; }
.stat { text-align: center; }
.stat-num { font-size: 26px; font-weight: 700; color: #409eff; }
.stat-label { font-size: 12px; color: #8f959e; margin-top: 4px; }
.chart { display: flex; align-items: flex-end; gap: 2px; height: 72px; }
.bar-wrap { flex: 1; display: flex; align-items: flex-end; }
.bar-stack { display: flex; flex-direction: column-reverse; width: 100%; gap: 1px; }
.bar { width: 100%; border-radius: 1px; }
.bar-ok { background: #67c23a; }
.bar-dlt { background: #f56c6c; }
.key { background: #f6f8fa; border: 1px solid #eee; border-radius: 4px; padding: 2px 6px; font-size: 12px; font-family: ui-monospace, Menlo, monospace; }
.code { background: #f6f8fa; border: 1px solid #eee; border-radius: 6px; padding: 8px; font-size: 12px; font-family: ui-monospace, Menlo, monospace; display: block; overflow-x: auto; }
.toolbar-sm { display: flex; align-items: center; gap: 12px; margin-bottom: 12px; }
.flex-between { display: flex; justify-content: space-between; align-items: center; }
</style>
