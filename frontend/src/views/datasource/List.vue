<template>
  <div class="page-card">
    <el-card shadow="never">
      <div class="toolbar">
        <div style="display: flex; gap: 8px; flex-wrap: wrap">
          <el-input v-model="query.keyword" placeholder="搜索名称" clearable style="width: 180px" @keyup.enter="load" />
          <el-select v-model="query.type" placeholder="类型" clearable style="width: 130px" @change="load">
            <el-option v-for="t in typeOptions" :key="t.value" :label="t.label" :value="t.value" />
          </el-select>
          <el-select v-model="query.group" placeholder="分组" clearable style="width: 130px" @change="load">
            <el-option v-for="g in groups" :key="g" :label="g" :value="g" />
          </el-select>
          <el-button type="primary" @click="load"><el-icon><Search /></el-icon>查询</el-button>
        </div>
        <el-button v-if="userStore.canWrite" type="success" @click="openForm()"><el-icon><Plus /></el-icon>新建数据源</el-button>
      </div>

      <el-table :data="rows" v-loading="loading" stripe>
        <el-table-column prop="id" label="ID" width="60" />
        <el-table-column prop="name" label="名称" min-width="150" show-overflow-tooltip />
        <el-table-column label="类型" width="110">
          <template #default="{ row }">
            <el-tag :type="typeTag(row.type)">{{ typeLabel(row.type) }}</el-tag>
          </template>
        </el-table-column>
        <el-table-column label="地址" min-width="180">
          <template #default="{ row }">{{ row.host }}:{{ row.port }}/{{ row.databaseName }}</template>
        </el-table-column>
        <el-table-column prop="groupName" label="分组" width="100" />
        <el-table-column prop="env" label="环境" width="80" />
        <el-table-column label="状态" width="90">
          <template #default="{ row }">
            <el-tag :type="row.enabled === 1 ? 'success' : 'info'" size="small">
              {{ row.enabled === 1 ? '启用' : '禁用' }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column label="健康" width="90">
          <template #default="{ row }">
            <el-tag v-if="row.lastHealthCheckResult" :type="row.lastHealthCheckResult === 'OK' ? 'success' : 'danger'" size="small">
              {{ row.lastHealthCheckResult }}
            </el-tag>
            <span v-else>-</span>
          </template>
        </el-table-column>
        <el-table-column label="操作" width="290" fixed="right">
          <template #default="{ row }">
            <el-button size="small" @click="browse(row)">浏览</el-button>
            <el-button size="small" @click="test(row)">测试连接</el-button>
            <el-button size="small" type="primary" @click="openForm(row)">编辑</el-button>
            <el-button v-if="row.enabled === 1" size="small" type="warning" @click="toggle(row, false)">禁用</el-button>
            <el-button v-else size="small" type="success" @click="toggle(row, true)">启用</el-button>
            <el-button size="small" type="danger" @click="remove(row)">删除</el-button>
          </template>
        </el-table-column>
      </el-table>

      <div class="pagination">
        <el-pagination background layout="total, prev, pager, next" :total="total"
                       :page-size="query.pageSize" :current-page="query.page" @current-change="p => { query.page = p; load() }" />
      </div>
    </el-card>

    <!-- 新建/编辑抽屉 -->
    <el-drawer v-model="drawer" :title="form.id ? '编辑数据源' : '新建数据源'" size="480px">
      <el-form :model="form" label-width="90px">
        <el-form-item label="名称" required>
          <el-input v-model="form.name" placeholder="业务名称, 如: 订单库" />
        </el-form-item>
        <el-form-item label="类型" required>
          <el-select v-model="form.type" style="width: 100%" @change="onTypeChange">
            <el-option v-for="t in typeOptions" :key="t.value" :label="t.label" :value="t.value" />
          </el-select>
        </el-form-item>
        <el-form-item label="主机" required>
          <el-input v-model="form.host" placeholder="IP 或域名" />
        </el-form-item>
        <el-form-item label="端口" required>
          <el-input-number v-model="form.port" :min="1" :max="65535" style="width: 100%" />
        </el-form-item>
        <el-form-item :label="formDbLabel" required>
          <el-input v-model="form.databaseName" :placeholder="formDbLabel + ' / Service 名'" />
        </el-form-item>
        <el-form-item label="用户名" required>
          <el-input v-model="form.username" />
        </el-form-item>
        <el-form-item label="密码">
          <el-input v-model="form.password" type="password" show-password
                    :placeholder="form.id ? '留空则不修改' : '必填'" />
        </el-form-item>
        <el-form-item label="扩展参数">
          <el-input v-model="form.extraParams" type="textarea" :rows="2"
                    placeholder='JSON, 如 {"connectTimeout":"5000"}' />
        </el-form-item>
        <el-form-item label="分组">
          <el-input v-model="form.groupName" placeholder="业务域分组" />
        </el-form-item>
        <el-form-item label="环境">
          <el-select v-model="form.env" clearable style="width: 100%">
            <el-option label="开发 dev" value="dev" />
            <el-option label="测试 test" value="test" />
            <el-option label="生产 prod" value="prod" />
            <el-option label="演示 demo" value="demo" />
          </el-select>
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="drawer = false">取消</el-button>
        <el-button type="primary" :loading="saving" @click="save">保存</el-button>
      </template>
    </el-drawer>
    <!-- 库表浏览: 实例 → 库 → Schema → 表(按类型分层) -->
    <el-drawer v-model="browseDrawer" :title="`库表浏览 — ${browseRow?.name || ''}`" size="440px">
      <el-alert type="info" :closable="false" show-icon style="margin-bottom: 12px"
        :title="`层级: 实例 → ${browseLevels.database ? browseLevels.dbLabel : ''}${browseLevels.schema ? ' → Schema' : ''} → ${browseLevels.tableLabel}(按类型区分)`" />
      <el-tree :data="browseTree" :props="{ label: 'label', children: 'children' }" default-expand-all />
      <div class="tip">// TODO: 接入后端 API(GET /datasources/{id}/databases → /databases/{db}/schemas → /schemas/{s}/tables),当前为示例数据</div>
    </el-drawer>
  </div>
</template>

<script setup>
import { computed, onMounted, reactive, ref } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import { listDatasources, createDatasource, updateDatasource, deleteDatasource,
         enableDatasource, disableDatasource, testDatasource } from '../../api'
import { useUserStore } from '../../store/user'
import { DB_TYPE_OPTIONS, DEFAULT_PORTS, dbLevels } from '../../utils/dbLevels'

const userStore = useUserStore()
const typeOptions = DB_TYPE_OPTIONS
const defaultPorts = DEFAULT_PORTS

const rows = ref([])
const total = ref(0)
const loading = ref(false)
const drawer = ref(false)
const saving = ref(false)
const groups = ref([])
const query = reactive({ page: 1, pageSize: 10, keyword: '', type: '', group: '' })
const form = reactive({ id: null, name: '', type: 'mysql', host: '', port: 3306, databaseName: '', username: '', password: '', extraParams: '', groupName: '', env: 'dev' })

function typeLabel(t) { return typeOptions.find(x => x.value === t)?.label || t }
function typeTag(t) {
  return { mysql: 'primary', postgresql: 'success', oracle: 'warning', sqlserver: 'danger', mongodb: 'info' }[t] || 'info'
}

// 表单「库名」字段的称谓按类型变化(MySQL/PG/SQLServer/MongoDB=数据库名, Oracle=Service/Schema)
const formDbLabel = computed(() => dbLevels(form.type).database ? '数据库名' : '服务名 / Schema(用户)')

// ---------- 库表浏览(实例 → 库 → Schema → 表) ----------
const browseDrawer = ref(false)
const browseRow = ref(null)
const browseLevels = computed(() => dbLevels(browseRow.value?.type || 'mysql'))
const browseTree = computed(() => browseRow.value ? mockCatalogTree(browseRow.value) : [])
function browse(row) { browseRow.value = row; browseDrawer.value = true }
// 示例数据: 按类型生成 库(→ Schema) → 表 树; 后端接入后由 /datasources/{id}/databases 系列接口返回
function mockCatalogTree(row) {
  const lv = dbLevels(row.type)
  const db = row.databaseName || 'main'
  const mockTables = p => [1, 2, 3].map(i => ({ label: `${p}_t${i}` }))
  if (row.type === 'oracle') {
    return ['OMS_APP', 'OMS_ARCH'].map(s => ({ label: s, children: mockTables(s) }))
  }
  const dbs = [db, `${db}_arch`]
  return dbs.map(d => lv.schema
    ? { label: d, children: ['public', 'archive'].map(s => ({ label: s, children: mockTables(`${d}_${s}`) })) }
    : { label: d, children: mockTables(d) })
}

function onTypeChange() {
  form.port = defaultPorts[form.type] || 3306
}

async function load() {
  loading.value = true
  try {
    const data = await listDatasources(query)
    rows.value = data.list
    total.value = data.total
    groups.value = [...new Set(data.list.map(r => r.groupName).filter(Boolean))]
  } finally {
    loading.value = false
  }
}

function openForm(row) {
  Object.assign(form, row ? {
    id: row.id, name: row.name, type: row.type, host: row.host, port: row.port,
    databaseName: row.databaseName, username: row.username, password: '',
    extraParams: row.extraParams || '', groupName: row.groupName || '', env: row.env || 'dev'
  } : { id: null, name: '', type: 'mysql', host: '', port: 3306, databaseName: '', username: '', password: '', extraParams: '', groupName: '', env: 'dev' })
  drawer.value = true
}

async function save() {
  if (!form.name || !form.host || !form.databaseName || !form.username) {
    ElMessage.warning('请填写必填项')
    return
  }
  saving.value = true
  try {
    if (form.id) {
      await updateDatasource(form.id, form)
    } else {
      await createDatasource(form)
    }
    ElMessage.success('保存成功')
    drawer.value = false
    load()
  } finally {
    saving.value = false
  }
}

async function toggle(row, enabled) {
  enabled ? await enableDatasource(row.id) : await disableDatasource(row.id)
  ElMessage.success(enabled ? '已启用' : '已禁用')
  load()
}

async function remove(row) {
  await ElMessageBox.confirm(`确定删除数据源「${row.name}」吗? (被任务引用时会被拦截)`, '提示', { type: 'warning' })
  try {
    await deleteDatasource(row.id)
    ElMessage.success('已删除')
    load()
  } catch (e) { /* 业务错误信息已提示 */ }
}

async function test(row) {
  try {
    await testDatasource(row.id)
    ElMessage.success('连接成功 ✔')
    load()
  } catch (e) { /* 错误信息由拦截器提示 */ }
}

onMounted(load)
</script>
