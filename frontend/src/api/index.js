import request from './request'
import axios from 'axios'

// ---------- 认证 ----------
export const login = (data) => request.post('/auth/login', data)
export const getMe = () => request.get('/auth/me')

// ---------- 数据源 ----------
export const listDatasources = (params) => request.get('/datasources', { params })
export const getDatasource = (id) => request.get(`/datasources/${id}`)
export const createDatasource = (data) => request.post('/datasources', data)
export const updateDatasource = (id, data) => request.put(`/datasources/${id}`, data)
export const deleteDatasource = (id) => request.delete(`/datasources/${id}`)
export const enableDatasource = (id) => request.post(`/datasources/${id}/enable`)
export const disableDatasource = (id) => request.post(`/datasources/${id}/disable`)
export const testDatasource = (id) => request.post(`/datasources/${id}/test`)
export const datasourceTables = (id) => request.get(`/datasources/${id}/tables`)
export const datasourceColumns = (id, table) => request.get(`/datasources/${id}/tables/${table}/columns`)
export const datasourcePreview = (id, sql, limit) => request.post(`/datasources/${id}/preview`, { sql, limit })

// ---------- 数据源 V3 库/Schema/表 级联浏览 ----------
export const datasourceDatabases = (id) => request.get(`/datasources/${id}/databases`)
export const datasourceSchemas = (id, database) => request.get(`/datasources/${id}/databases/${database}/schemas`)
export const datasourceDbTables = (id, database, schema) => request.get(`/datasources/${id}/databases/${database}/tables`, { params: { schema } })
export const datasourceDbColumns = (id, database, table, schema) => request.get(`/datasources/${id}/databases/${database}/tables/${table}/columns`, { params: { schema } })

// ---------- V3 同步链路: 逻辑表配置(业务主键) ----------
export const getLogicalTable = (table) => request.get(`/sync/logical-tables/${table}`)
export const saveLogicalTable = (table, data) => request.put(`/sync/logical-tables/${table}`, data)

// ---------- V3 同步任务(字段映射/行级链提交) ----------
export const listSyncTasks = () => request.get('/sync/tasks')
export const validateSyncTask = (data) => request.post('/sync/tasks/validate', data)
export const saveSyncTask = (data) => request.post('/sync/tasks', data)
export const startSeatunnelCdc = (data) => request.post('/sync/tasks/seatunnel-cdc/start', data)

// ---------- V3 SeaTunnel CDC 采集(唯一采集链路; 多表合并: 一逻辑表多源表) ----------
export const cdcRegister = (data) => request.post('/sync/cdc-st/task', data)
export const cdcStart = (logicalTable) => request.post(`/sync/cdc-st/start/${logicalTable}`)
export const cdcStop = (logicalTable) => request.post(`/sync/cdc-st/stop/${logicalTable}`)
export const cdcStartInstance = (taskId) => request.post(`/sync/cdc-st/start-instance/${taskId}`)
export const cdcStopInstance = (taskId) => request.post(`/sync/cdc-st/stop-instance/${taskId}`)
export const cdcStatus = () => request.get('/sync/cdc-st/status')

// ---------- V3 湖/权威副本 ----------
export const lakeStatus = (table) => request.get(`/sync/lake/status/${table}`)
export const lakeFlush = (table) => request.post(`/sync/lake/flush/${table}`)
export const scaleLag = (logicalTable) => request.get(`/sync/scale/lag/${logicalTable}`)

// ---------- V3 死信队列 ----------
export const dltPage = (params) => request.get('/sync/dlt', { params })
export const dltStats = () => request.get('/sync/dlt/stats')
export const dltDetail = (id) => request.get(`/sync/dlt/${id}`)
export const dltReplay = (id) => request.post(`/sync/dlt/${id}/replay`)
export const dltReplayBatch = (ids) => request.post('/sync/dlt/replay-batch', { ids })
export const dltIgnore = (id) => request.post(`/sync/dlt/${id}/ignore`)

// ---------- 任务 ----------
export const listJobs = (params) => request.get('/jobs', { params })
export const getJob = (id) => request.get(`/jobs/${id}`)
export const createJob = (data) => request.post('/jobs', data)
export const updateJob = (id, data) => request.put(`/jobs/${id}`, data)
export const deleteJob = (id) => request.delete(`/jobs/${id}`)
export const enableJob = (id) => request.post(`/jobs/${id}/enable`)
export const disableJob = (id) => request.post(`/jobs/${id}/disable`)
export const runJob = (id) => request.post(`/jobs/${id}/run`)
export const stopJob = (id) => request.post(`/jobs/${id}/stop`)
export const copyJob = (id) => request.post(`/jobs/${id}/copy`)
export const getSyncState = (id) => request.get(`/jobs/${id}/sync-state`)
export const resetSyncState = (id, data) => request.put(`/jobs/${id}/sync-state`, data)
export const validateExpression = (data) => request.post('/jobs/mappings/expression/validate', data)
export const cronPreview = (expr) => request.get('/jobs/cron/preview', { params: { expr } })
export const autoMapping = (data) => request.post('/jobs/mappings/auto', data)

// ---------- 字典 ----------
export const listDicts = (params) => request.get('/dicts', { params })
export const allDicts = () => request.get('/dicts/all')
export const createDict = (data) => request.post('/dicts', data)
export const updateDict = (id, data) => request.put(`/dicts/${id}`, data)
export const deleteDict = (id) => request.delete(`/dicts/${id}`)
export const dictEntries = (id) => request.get(`/dicts/${id}/entries`)
export const createDictEntry = (id, data) => request.post(`/dicts/${id}/entries`, data)
export const updateDictEntry = (entryId, data) => request.put(`/dicts/entries/${entryId}`, data)
export const deleteDictEntry = (entryId) => request.delete(`/dicts/entries/${entryId}`)
export const importDict = (id, file, overwrite) => {
  const form = new FormData()
  form.append('file', file)
  return request.post(`/dicts/${id}/import`, form, { params: { overwrite } })
}
export const exportDictUrl = (id) => `/api/dicts/${id}/export`
export const dictTemplateUrl = '/api/dicts/template'

// ---------- 执行监控 ----------
export const listExecutions = (params) => request.get('/executions', { params })
export const getExecution = (id) => request.get(`/executions/${id}`)
export const executionLogs = (id) => request.get(`/executions/${id}/logs`)

// ---------- 统计 ----------
export const statsOverview = () => request.get('/stats/overview')
export const statsTrend = (days) => request.get('/stats/trend', { params: { days } })
export const statsTaskRank = (limit) => request.get('/stats/task-rank', { params: { limit } })

// ---------- 统一调度框架告警事件流(主写 /dcp/{ns}/alerts/, 后端 jetcd 只读透传) ----------
export const listSchedulerAlerts = (ns) => request.get('/scheduler/alerts', { params: { ns } })

// ---------- 统一调度框架运维大盘(四级监控下钻 + ops 指令通道) ----------
export const listSchedulerMetrics = (ns, taskId) => request.get('/scheduler/metrics', { params: { ns, taskId } })
export const listSchedulerOps = (ns) => request.get('/scheduler/ops', { params: { ns } })
export const listSchedulerInstances = (ns) => request.get('/scheduler/instances', { params: { ns } })
export const submitSchedulerOp = (data) => request.post('/scheduler/ops', data)

// ---------- 统一调度框架 namespace 管理(创建/删除 ns、默认规则走 SET_RULES ops) ----------
export const listSchedulerNamespaces = () => request.get('/scheduler/namespaces')
export const createSchedulerNs = (data) => request.post('/scheduler/namespaces', data)
export const deleteSchedulerNs = (ns) => request.delete(`/scheduler/namespaces/${ns}`)
export const listSchedulerAudits = () => request.get('/scheduler/audits')

// ---------- 告警 ----------
export const listAlarms = (jobId) => request.get('/alarms', { params: { jobId } })
export const createAlarm = (data) => request.post('/alarms', data)
export const updateAlarm = (id, data) => request.put(`/alarms/${id}`, data)
export const deleteAlarm = (id) => request.delete(`/alarms/${id}`)

// ---------- 系统 ----------
export const listUsers = (params) => request.get('/users', { params })
export const createUser = (data) => request.post('/users', data)
export const updateUser = (id, data) => request.put(`/users/${id}`, data)
export const deleteUser = (id) => request.delete(`/users/${id}`)
export const listRoles = () => request.get('/roles')
export const listAuditLogs = (params) => request.get('/audit-logs', { params })

// ===== 数据资产(真实数据: 逻辑表 + 来源 + 水位 + 分表预览) =====
export const listAssets = () => request.get('/assets')
export const getAssetDetail = (table) => request.get(`/assets/${table}`)
export const getAssetPreview = (table, limit = 10) => request.get(`/assets/${table}/preview`, { params: { limit } })
export const getAssetLineage = (table) => request.get(`/assets/${table}/lineage`)

// ===== 资产编目(可维护: 多层级节点 + 多维挂载 + 资产元数据) =====
export const getCatalogTree = (dim) => request.get('/asset-catalog/tree', { params: { dim } })
export const getCatalogAssets = () => request.get('/asset-catalog/assets')
export const createCatalogNode = (data) => request.post('/asset-catalog/nodes', data)
export const renameCatalogNode = (id, name) => request.put(`/asset-catalog/nodes/${id}`, { name })
export const deleteCatalogNode = (id) => request.delete(`/asset-catalog/nodes/${id}`)
export const bindAsset = (nodeId, tableName) => request.post('/asset-catalog/bind', { nodeId, tableName })
export const unbindAsset = (nodeId, tableName) => request.delete('/asset-catalog/bind', { params: { nodeId, tableName } })
export const saveAssetMeta = (table, data) => request.put(`/asset-catalog/assets/${table}`, data)

// ===== 标签体系(多级 tag_dict + 资产打标) =====
export const getTagTree = () => request.get('/tags/tree')
export const getTagAssets = (path) => request.get('/tags/assets', { params: { path } })
export const createTag = (data) => request.post('/tags', data)
export const updateTag = (id, data) => request.put(`/tags/${id}`, data)
export const deleteTag = (id) => request.delete(`/tags/${id}`)
export const moveTag = (id, parentId) => request.put(`/tags/${id}/move`, { parentId })
export const getTagSubset = (tags, op = 'AND', exclude = []) => request.get('/tags/subset', { params: { tags, op, exclude } })

// ===== 订阅中心(标签条件订阅, 命中 = 标签子集) =====
export const listSubscriptions = () => request.get('/subscriptions')
export const getSubscription = (id) => request.get(`/subscriptions/${id}`)
export const getSubscriptionAssets = (id) => request.get(`/subscriptions/${id}/assets`)
export const createSubscription = (data) => request.post('/subscriptions', data)
export const updateSubscription = (id, data) => request.put(`/subscriptions/${id}`, data)
export const setSubscriptionStatus = (id, status) => request.put(`/subscriptions/${id}/status`, { status })
export const deleteSubscription = (id) => request.delete(`/subscriptions/${id}`)
export const listRunHistory = (logicalTable, limit = 50) => request.get('/sync/history', { params: { logicalTable, limit } })

// ===== 模型管理(建模即发布: 建 topic/分区 + 预建物理表) =====
export const listModels = () => request.get('/models')
export const getModel = (id) => request.get(`/models/${id}`)
export const createModel = (data) => request.post('/models', data)
export const deleteModel = (id) => request.delete(`/models/${id}`)

// ===== 湖表同步(任务配置式 · 一任务一表; 数据获取统一走消费侧) =====
export const listLakeSyncTasks = () => request.get('/lake-sync/tasks')
export const getLakeSyncTask = (id) => request.get(`/lake-sync/tasks/${id}`)
export const createLakeSyncTask = (data) => request.post('/lake-sync/tasks', data)
export const updateLakeSyncTask = (id, data) => request.put(`/lake-sync/tasks/${id}`, data)
export const deleteLakeSyncTask = (id) => request.delete(`/lake-sync/tasks/${id}`)
export const runLakeSyncTask = (id, trigger = 'manual') => request.post(`/lake-sync/tasks/${id}/run`, null, { params: { trigger } })
export const listLakeSyncRuns = (taskId) => request.get('/lake-sync/runs', { params: { taskId } })
export const getLakeSyncRun = (id) => request.get(`/lake-sync/runs/${id}`)
// 源表清单(暂复用数据订阅目录: 表/列/行数; TODO 切到 /lake-sync/catalog/tables 走出口元数据)
export const lakeSyncCatalog = () => request.get('/data-sub/catalog')
// 消费侧指标(任务详情页: 现场读, 不依赖 Prometheus)
export const consumerMetrics = (consumerId, recent = 10) => request.get(`/chg/consumers/${consumerId}/metrics`, { params: { recent } })
// 同上·静默版:未绑定消费方时后端返 6002「消费方不存在」,自动加载不该弹错误提示 → 静默归 null
export const consumerMetricsQuiet = async (consumerId, recent = 10) => {
  try {
    const r = await axios.get(`/api/chg/consumers/${consumerId}/metrics`, {
      params: { recent },
      headers: { Authorization: `Bearer ${localStorage.getItem('dcp_token')}` }
    })
    return r.data?.code === 0 ? r.data.data : null
  } catch (e) {
    return null
  }
}
