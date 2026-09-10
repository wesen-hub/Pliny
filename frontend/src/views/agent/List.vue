<template>
  <div class="page-card">
    <el-card shadow="never">
      <div class="toolbar">
        <el-text size="small" type="info">采集 Agent(dcp-agent)部署在业务机上,把本地目录变为文件同步源;Agent 启动后凭注册码自注册,平台任务下发、Agent 本地 mc 直推目标 MinIO</el-text>
        <el-button type="success" @click="openCreate"><el-icon><Plus /></el-icon>注册 Agent(生成注册码)</el-button>
      </div>

      <el-table :data="rows" v-loading="loading" stripe>
        <el-table-column prop="id" label="ID" width="60" />
        <el-table-column prop="name" label="名称" min-width="130" />
        <el-table-column prop="host" label="主机" min-width="140" />
        <el-table-column label="状态" width="110">
          <template #default="{ row }">
            <el-tag v-if="row.display_status === 'online'" type="success" effect="plain" size="small">● 在线</el-tag>
            <el-tag v-else-if="row.status === 'pending'" type="info" effect="plain" size="small">待注册(未启动)</el-tag>
            <el-tag v-else type="danger" effect="plain" size="small">○ 离线</el-tag>
          </template>
        </el-table-column>
        <el-table-column label="最后心跳" width="170">
          <template #default="{ row }">{{ row.last_heartbeat || '—' }}</template>
        </el-table-column>
        <el-table-column prop="os" label="系统" width="110">
          <template #default="{ row }">{{ row.os ? row.os + ' / ' + (row.arch || '') : '—' }}</template>
        </el-table-column>
        <el-table-column prop="agent_version" label="版本" width="90" />
        <el-table-column prop="task_cnt" label="绑定任务" width="90" />
        <el-table-column label="操作" width="130">
          <template #default="{ row }">
            <el-link v-if="row.status === 'pending'" type="warning" underline="never" @click="showCode(row)">查看注册码</el-link>
            <el-link v-else type="primary" underline="never" style="margin-left:6px" @click="copyDeploy(row)">部署说明</el-link>
            <el-link type="danger" underline="never" style="margin-left:6px" @click="remove(row)">删除</el-link>
          </template>
        </el-table-column>
      </el-table>

      <el-empty v-if="!rows.length && !loading" description="还没有 Agent——点右上角「注册 Agent」生成注册码,再在业务机启动 dcp-agent" />
    </el-card>

    <!-- 新建 -->
    <el-dialog v-model="createOpen" title="注册采集 Agent" width="520px">
      <el-form label-width="90px">
        <el-form-item label="名称" required>
          <el-input v-model="createForm.name" placeholder="如:业务机A-订单服务" />
        </el-form-item>
        <el-form-item label="主机/备注">
          <el-input v-model="createForm.host" placeholder="如 10.0.0.11 / 北京机房" />
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="createOpen = false">取消</el-button>
        <el-button type="primary" :loading="saving" @click="create">生成注册码</el-button>
      </template>
    </el-dialog>

    <!-- 注册码一次性展示 -->
    <el-dialog v-model="codeOpen" title="Agent 注册码(仅此一次显示)" width="620px">
      <el-alert type="success" :closable="false" show-icon style="margin-bottom:12px"
        title="请在业务机执行: docker run / 或下载 dcp-agent 后设置环境变量启动;注册成功后凭据保存在 Agent 本地,重启无需注册码" />
      <div class="code-box">
        <div class="code-line">DCP_AGENT_PLATFORM_URL=http://&lt;平台地址&gt;:8081</div>
        <div class="code-line code">DCP_AGENT_REGISTER_CODE={{ registerCode }}</div>
        <div class="code-line">java -jar dcp-agent.jar</div>
      </div>
      <el-text size="small" type="info">容器方式(与平台同机演示):docker compose 内 dcp-agent 服务已内置,设置环境变量 DCP_AGENT_REGISTER_CODE={{ registerCode }} 后 docker compose up -d agent</el-text>
      <template #footer>
        <el-button type="primary" @click="codeOpen = false">我已复制</el-button>
      </template>
    </el-dialog>

    <!-- 部署说明 -->
    <el-dialog v-model="deployOpen" title="Agent 部署说明" width="600px">
      <el-text size="small" type="info" style="display:block;margin-bottom:10px">Agent 已注册过(凭据在本地),重启/新机器部署命令:</el-text>
      <div class="code-box">
        <div class="code-line">DCP_AGENT_PLATFORM_URL=http://&lt;平台地址&gt;:8081</div>
        <div class="code-line">DCP_AGENT_DATA_DIR=/var/lib/dcp-agent   # 持久化目录(含凭据,勿丢)</div>
        <div class="code-line">DCP_AGENT_RCLONE=/usr/local/bin/rclone    # 需预装 rclone(镜像内已预置 v1.71.0)</div>
        <div class="code-line">java -jar dcp-agent.jar</div>
      </div>
      <el-text size="small" type="info" style="display:block;margin-top:8px">mc 下载:<code>curl -o mc https://dl.min.io/client/mc/release/&#123;os&#125;-&#123;arch&#125;/mc && chmod +x mc</code></el-text>
    </el-dialog>
  </div>
</template>

<script setup>
import { ref, onMounted } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import request from '../../api/request'

const rows = ref([])
const loading = ref(false)
const createOpen = ref(false)
const codeOpen = ref(false)
const deployOpen = ref(false)
const saving = ref(false)
const registerCode = ref('')
const createForm = ref({ name: '', host: '' })
let lastCreated = null

async function load() {
  loading.value = true
  try {
    rows.value = (await request.get('/agents')) || []
  } catch (e) {
    ElMessage.error('加载失败: ' + (e?.message || e))
  } finally {
    loading.value = false
  }
}
function openCreate() {
  createForm.value = { name: '', host: '' }
  createOpen.value = true
}
async function create() {
  if (!createForm.value.name.trim()) return ElMessage.warning('请填写名称')
  saving.value = true
  try {
    const r = await request.post('/agents', { name: createForm.value.name.trim(), host: createForm.value.host || null })
    lastCreated = r
    registerCode.value = r.registerCode
    createOpen.value = false
    codeOpen.value = true
    await load()
  } catch (e) {
    ElMessage.error('创建失败: ' + (e?.message || e))
  } finally {
    saving.value = false
  }
}
function showCode(row) {
  // pending 的 agent: 注册码只有创建时返回一次; 无存储 → 重新生成(删除旧的再建)
  ElMessageBox.confirm('注册码只在生成时显示一次。要重新生成吗?(会替换该待注册记录)', '注册码', { type: 'warning' })
    .then(async () => {
      const r = await request.post('/agents', { name: row.name + '-new', host: row.host })
      registerCode.value = r.registerCode
      codeOpen.value = true
      await load()
    }).catch(() => {})
}
function copyDeploy() {
  deployOpen.value = true
}
async function remove(row) {
  await ElMessageBox.confirm(`删除 Agent「${row.name}」?`, '提示', { type: 'warning' })
  try {
    await request.delete(`/agents/${row.id}`)
    ElMessage.success('已删除')
    await load()
  } catch (e) {
    ElMessage.error('删除失败: ' + (e?.message || e))
  }
}
onMounted(load)
</script>

<style scoped>
.toolbar { display: flex; justify-content: space-between; align-items: center; margin-bottom: 14px; flex-wrap: wrap; gap: 8px; }
.code-box { background: #0d1b2a; color: #d7e3f4; border-radius: 8px; padding: 14px 16px; font-size: 12px; line-height: 2; }
.code-line { word-break: break-all; }
.code { color: #7ee787; font-weight: 600; font-size: 14px; }
</style>
