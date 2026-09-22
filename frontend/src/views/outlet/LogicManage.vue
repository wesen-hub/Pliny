<template>
  <div class="page-card">
    <el-card shadow="never" v-loading="loading">
      <div class="flex" style="justify-content:space-between;align-items:center;margin-bottom:4px">
        <div>
          <div class="pg-title">逻辑管理</div>
          <el-text size="small" type="info">消费逻辑 jar 的上传发布(一次动作七步:大小/魔数校验 → 试加载预检 → 算哈希 → 原子落库 → 事务登记 → 审计)。版本启停/废弃为管理面一等操作;停用拦新建任务、不影响运行中单元。</el-text>
        </div>
        <el-button type="primary" @click="openUpload">＋ 上传发布</el-button>
      </div>

      <el-table :data="logics" size="small" border stripe style="margin-top:10px">
        <el-table-column label="逻辑 ID" min-width="170">
          <template #default="{ row }">
            <el-link type="primary" :underline="false" style="font-weight:600" @click="openVersions(row)">{{ row.logicId }}</el-link>
          </template>
        </el-table-column>
        <el-table-column label="最新版本" width="110">
          <template #default="{ row }"><el-tag size="small" effect="plain">v{{ row.latestVersion }}</el-tag></template>
        </el-table-column>
        <el-table-column label="版本数" width="80" align="center">
          <template #default="{ row }">{{ row.versionCount }}</template>
        </el-table-column>
        <el-table-column label="绑定任务数" width="100" align="center">
          <template #default="{ row }">{{ row.boundTasks }}</template>
        </el-table-column>
        <el-table-column label="状态" width="110">
          <template #default="{ row }">
            <el-tag size="small" :type="statusType(row.status)" effect="plain">{{ statusText(row.status) }}</el-tag>
          </template>
        </el-table-column>
        <el-table-column label="操作" width="180" fixed="right">
          <template #default="{ row }">
            <el-button size="small" text type="primary" @click="openVersions(row)">版本</el-button>
            <el-button v-if="row.status !== 'disabled'" size="small" text type="warning" @click="toggleLogic(row, 'disable')">停用</el-button>
            <el-button v-else size="small" text type="success" @click="toggleLogic(row, 'enable')">启用</el-button>
          </template>
        </el-table-column>
      </el-table>
      <el-text size="small" type="info" style="margin-top:8px;display:block">停用 = 逻辑级冻结:该 logicId 全部版本拦新建任务,已加载 jar 的运行中单元不受影响;升级 = 发新版本 + 任务改绑定,回滚 = 改回老版本绑定。</el-text>
    </el-card>

    <!-- ═══ 版本详情抽屉 ═══ -->
    <el-drawer v-model="versionsDlg" :title="'版本 · ' + curLogicId" size="620px">
      <el-table :data="versions" size="small" border>
        <el-table-column label="版本" width="90">
          <template #default="{ row }"><el-tag size="small" effect="plain">v{{ row.version }}</el-tag></template>
        </el-table-column>
        <el-table-column label="状态" width="90">
          <template #default="{ row }">
            <el-tag size="small" :type="statusType(row.status)" effect="plain">{{ statusText(row.status) }}</el-tag>
          </template>
        </el-table-column>
        <el-table-column label="大小" width="80" align="right">
          <template #default="{ row }">{{ fmtSize(row.jarSize) }}</template>
        </el-table-column>
        <el-table-column label="发布人" width="80"><template #default="{ row }">{{ row.publishedBy || '—' }}</template></el-table-column>
        <el-table-column label="时间" width="140">
          <template #default="{ row }"><el-text size="small" type="info">{{ row.registeredAt || '—' }}</el-text></template>
        </el-table-column>
        <el-table-column label="操作" width="130" fixed="right">
          <template #default="{ row }">
            <el-button v-if="row.status !== 'disabled'" size="small" text type="warning" @click="toggleVersion(row, 'disable')">停用</el-button>
            <el-button v-else size="small" text type="success" @click="toggleVersion(row, 'enable')">启用</el-button>
            <el-button v-if="row.status !== 'archived'" size="small" text type="danger" @click="archiveVersion(row)">废弃</el-button>
          </template>
        </el-table-column>
      </el-table>
      <div class="sec-title" style="margin-top:14px">版本详情</div>
      <template v-if="curVersion">
        <el-descriptions :column="1" size="small" border>
          <el-descriptions-item label="哈希(sha256)"><el-text size="small" style="word-break:break-all">{{ curVersion.artifactHash || '—' }}</el-text></el-descriptions-item>
          <el-descriptions-item label="预检结果"><el-text size="small">{{ curVersion.preflight || '—' }}</el-text></el-descriptions-item>
          <el-descriptions-item label="描述"><el-text size="small">{{ curVersion.description || '—' }}</el-text></el-descriptions-item>
          <el-descriptions-item label="绑定任务">
            <el-tag v-for="t in (curVersion.boundTaskIds || [])" :key="t" size="small" type="primary" effect="plain" style="margin:2px">{{ t }}</el-tag>
            <el-text v-if="!(curVersion.boundTaskIds || []).length" size="small" type="info">无</el-text>
          </el-descriptions-item>
        </el-descriptions>
      </template>
    </el-drawer>

    <!-- ═══ 上传弹窗 ═══ -->
    <el-dialog v-model="uploadDlg" title="上传发布逻辑" width="520" append-to-body>
      <el-form :model="form" label-width="90px" size="small">
        <el-form-item label="逻辑 ID" required>
          <el-input v-model="form.logicId" placeholder="如 es-sync" />
        </el-form-item>
        <el-form-item label="版本" required>
          <el-input v-model="form.version" placeholder="如 1.0.0(同版本禁止覆盖)" />
        </el-form-item>
        <el-form-item label="描述">
          <el-input v-model="form.description" type="textarea" :rows="2" placeholder="可选" />
        </el-form-item>
        <el-form-item label="jar 文件" required>
          <el-upload :auto-upload="false" :limit="1" :on-change="onFileChange" :on-remove="onFileRemove" drag style="width:100%">
            <el-icon style="margin-right:4px"><UploadFilled /></el-icon>
            <span>拖拽或点击选择 jar</span>
          </el-upload>
        </el-form-item>
        <el-alert type="info" :closable="false" show-icon title="发布预检" description="上传后先试加载预检(校验注解声明 / SPI / 哈希),失败即就地报错,不落库。" />
      </el-form>
      <template #footer>
        <el-button size="small" @click="uploadDlg = false">取消</el-button>
        <el-button size="small" type="primary" :loading="uploading" @click="submitUpload">发布</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup>
import { ref, onMounted } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'

const toast = (m, t) => (t ? ElMessage[t](m) : ElMessage.info(m))
const loading = ref(false)

// ── mock 数据(接后端时替换 load() 内的实现)──
const logics = ref([])
const versions = ref([])
const curLogicId = ref('')
const curVersion = ref(null)
const versionsDlg = ref(false)
const uploadDlg = ref(false)
const uploading = ref(false)
const form = ref({ logicId: '', version: '', description: '' })
const file = ref(null)

const statusText = (s) => ({ active: '启用', disabled: '停用', archived: '废弃' }[s] || s)
const statusType = (s) => ({ active: 'success', disabled: 'warning', archived: 'info' }[s] || 'info')
const fmtSize = (n) => (n == null ? '—' : n > 1024 * 1024 ? (n / 1024 / 1024).toFixed(2) + ' MB' : (n / 1024).toFixed(1) + ' KB')

function load() {
  loading.value = true
  // TODO: 接入后端 API —— GET /api/outlet/logics(列表:logicId 聚合 + 最新版本 + 版本数 + 绑定任务数 + 状态)
  logics.value = [
    { logicId: 'es-sync', latestVersion: '1.2.0', versionCount: 3, boundTasks: 2, status: 'active' },
    { logicId: 'tag-eval', latestVersion: '2.0.0', versionCount: 2, boundTasks: 1, status: 'active' },
    { logicId: 'lake-flush', latestVersion: '0.9.1', versionCount: 1, boundTasks: 0, status: 'disabled' },
    { logicId: 'legacy-normalize', latestVersion: '1.0.0', versionCount: 1, boundTasks: 0, status: 'archived' }
  ]
  loading.value = false
}
onMounted(load)

function openVersions(row) {
  curLogicId.value = row.logicId
  curVersion.value = null
  // TODO: 接入后端 API —— GET /api/outlet/logics/{logicId}/versions(状态/哈希/大小/发布人/时间/预检/绑定任务)
  versions.value = [
    { logicId: row.logicId, version: row.latestVersion, status: row.status, jarSize: 284771, artifactHash: 'a1b2c3d4e5f6…', publishedBy: 'wesen', registeredAt: '2026-09-22 11:20', preflight: '通过: sha256=a1b2…', description: '最新', boundTaskIds: row.boundTasks ? ['tag-eval', 'es-sync-task'] : [] },
    { logicId: row.logicId, version: '1.0.0', status: 'disabled', jarSize: 274091, artifactHash: 'c3d4e5f6…', publishedBy: 'wesen', registeredAt: '2026-09-18 09:02', preflight: '通过: sha256=c3d4…', description: '', boundTaskIds: [] }
  ]
  versionsDlg.value = true
}

function showVersion(row) { curVersion.value = row }

function toggleLogic(row, action) {
  const verb = action === 'disable' ? '停用' : '启用'
  ElMessageBox.confirm(`${verb}逻辑「${row.logicId}」? ${action === 'disable' ? '停用后该逻辑全部版本拦新建任务,运行中单元不受影响。' : '启用后恢复新建任务。'}`, verb + '逻辑', { type: 'warning' })
    .then(() => {
      // TODO: 接入后端 API —— POST /api/outlet/logics/{logicId}/disable | enable
      row.status = action === 'disable' ? 'disabled' : 'active'
      toast(`已${verb}`)
    })
    .catch(() => {})
}

function toggleVersion(row, action) {
  const verb = action === 'disable' ? '停用' : '启用'
  ElMessageBox.confirm(`${verb}版本 ${row.version}? ${action === 'disable' ? '停用拦新建任务,不影响已加载的运行中单元。' : '启用后恢复。'}`, verb + '版本', { type: 'warning' })
    .then(() => {
      // TODO: 接入后端 API —— POST /api/outlet/logics/{logicId}/versions/{v}/disable | enable
      row.status = action === 'disable' ? 'disabled' : 'active'
      toast(`已${verb}`)
    })
    .catch(() => {})
}

function archiveVersion(row) {
  ElMessageBox.confirm(`废弃版本 ${row.version}? ${row.boundTaskIds && row.boundTaskIds.length ? '该版本被 ' + row.boundTaskIds.length + ' 个任务引用,将拒绝删除。' : '被引用版本拒绝删除;未引用则标记废弃(软删除,保审计)。'}`, '废弃版本', { type: 'warning' })
    .then(() => {
      // TODO: 接入后端 API —— DELETE /api/outlet/logics/{logicId}/versions/{v}(引用检查,被引用拒绝)
      row.status = 'archived'
      toast('已标记废弃')
    })
    .catch(() => {})
}

function openUpload() {
  form.value = { logicId: '', version: '', description: '' }
  file.value = null
  uploadDlg.value = true
}
function onFileChange(f) { file.value = f.raw }
function onFileRemove() { file.value = null }

function submitUpload() {
  if (!form.value.logicId || !form.value.version) { toast('逻辑 ID 与版本必填', 'warning'); return }
  if (!file.value) { toast('请选择 jar 文件', 'warning'); return }
  uploading.value = true
  // TODO: 接入后端 API —— POST /api/outlet/logics(multipart: jar + logicId + version + description)
  setTimeout(() => {
    uploading.value = false
    uploadDlg.value = false
    toast(`已发布 ${form.value.logicId}@${form.value.version}`)
    load()
  }, 400)
}
</script>

<style scoped>
.page-card { max-width: 1400px; }
.pg-title { font-size: 17px; font-weight: 700; margin-bottom: 4px; }
.flex { display: flex; }
.sec-title { font-size: 14px; font-weight: 700; margin: 8px 0 6px; }
</style>
