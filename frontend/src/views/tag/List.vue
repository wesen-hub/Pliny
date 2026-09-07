<template>
  <div class="page-card">
    <el-card shadow="never">
      <div class="toolbar">
        <div style="font-weight: 600">标签体系<el-text type="info" style="margin-left: 8px; font-weight: 400">多级标签 · 系统标签自动统计(来源/分层/密级) · 手动标签资产打标 · 父子聚合</el-text></div>
        <div>
          <el-button :loading="loading" @click="load">刷新</el-button>
          <el-button @click="openDlg(null)">＋ 新建一级标签</el-button>
        </div>
      </div>
      <el-table :data="tags" row-key="id" default-expand-all :tree-props="{ children: 'children' }" v-loading="loading">
        <el-table-column label="标签路径" min-width="280">
          <template #default="{ row }">
            <span v-if="row.level === 1" class="root-tag">{{ row.tag }}</span>
            <template v-else><span class="path-sep">/</span>{{ row.tag }}</template>
            <span class="path-sub">{{ row.path }}</span>
          </template>
        </el-table-column>
        <el-table-column label="层级" width="70">
          <template #default="{ row }"><el-tag size="small" :type="levelType(row.level)" effect="plain">L{{ row.level }}</el-tag></template>
        </el-table-column>
        <el-table-column prop="category" label="类型" width="110" />
        <el-table-column prop="granularity" label="粒度" width="90" />
        <el-table-column prop="source" label="来源" min-width="150" show-overflow-tooltip />
        <el-table-column label="挂载资产" width="90">
          <template #default="{ row }">
            <el-link v-if="row.assets" type="primary" underline="never" @click="showAssets(row)">{{ row.assets }}</el-link>
            <span v-else class="sub">0</span>
          </template>
        </el-table-column>
        <el-table-column label="状态" width="80">
          <template #default="{ row }"><el-tag size="small" :type="row.status === '启用' ? 'success' : 'warning'" effect="plain">{{ row.status }}</el-tag></template>
        </el-table-column>
        <el-table-column label="操作" width="180">
          <template #default="{ row }">
            <el-link type="primary" underline="never" @click="openDlg(row)">＋ 子标签</el-link>
            <el-link type="primary" underline="never" style="margin-left:8px" @click="openEdit(row)">编辑</el-link>
            <el-link type="danger" underline="never" style="margin-left:8px" @click="remove(row)">删除</el-link>
          </template>
        </el-table-column>
      </el-table>
      <div class="tip">父子语义: 打子标签自动继承父链; 父标签资产数 = 全部子孙聚合; 系统标签(来源系统/分层/密级)由资产属性实时派生, 无需手动打标; 资产打标入口: 资产目录 → 资产编辑 → 标签。</div>
    </el-card>

    <!-- 标签编辑对话框 -->
    <el-dialog v-model="dlg.visible" :title="dlg.title" width="420px">
      <el-form label-width="70px">
        <el-form-item label="标签名"><el-input v-model="dlg.name" placeholder="如: 订单 / 大促核心" /></el-form-item>
        <el-form-item label="类型">
          <el-select v-model="dlg.category" style="width: 100%">
            <el-option label="业务标签" value="业务标签" /><el-option label="系统标签" value="系统标签" />
            <el-option label="敏感标签" value="敏感标签" /><el-option label="自定义标签" value="自定义标签" />
          </el-select>
        </el-form-item>
        <el-form-item label="粒度">
          <el-select v-model="dlg.granularity" style="width: 100%">
            <el-option label="表级" value="表级" /><el-option label="字段级" value="字段级" /><el-option label="记录级" value="记录级" />
          </el-select>
        </el-form-item>
        <el-form-item label="来源"><el-input v-model="dlg.source" placeholder="如: 模型定义 / 手动 / 规则引擎" /></el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="dlg.visible = false">取消</el-button>
        <el-button type="primary" @click="submit">保存</el-button>
      </template>
    </el-dialog>

    <!-- 标签下资产列表 -->
    <el-dialog v-model="assetDlg.visible" :title="`标签「${assetDlg.path}」下的资产`" width="640px">
      <el-table :data="assetDlg.assets" stripe size="small">
        <el-table-column label="资产" min-width="160">
          <template #default="{ row }">
            <el-link type="primary" @click="$router.push(`/asset/${row.table}`)">{{ row.displayName || row.table }}</el-link>
          </template>
        </el-table-column>
        <el-table-column label="来源" min-width="150">
          <template #default="{ row }">
            <el-tag v-for="s in row.sources" :key="s.table" size="small">{{ s.dsName }}</el-tag>
          </template>
        </el-table-column>
        <el-table-column label="表数" width="80">
          <template #default="{ row }"><span>{{ row.shardCount ?? '—' }}</span></template>
        </el-table-column>
      </el-table>
    </el-dialog>
  </div>
</template>

<script setup>
import { ref, onMounted } from 'vue'
import { useRouter } from 'vue-router'
import { ElMessage, ElMessageBox } from 'element-plus'
import { getTagTree, getTagAssets, createTag, updateTag, deleteTag } from '../../api'

const router = useRouter()
const tags = ref([])
const loading = ref(false)
const dlg = ref({ visible: false, title: '', id: null, parentId: 0, name: '', category: '自定义标签', granularity: '表级', source: '' })
const assetDlg = ref({ visible: false, path: '', assets: [] })

function levelType(level) { return { 1: 'danger', 2: 'warning', 3: 'primary', 4: 'info' }[level] || 'info' }

async function load() {
  loading.value = true
  try {
    const res = await getTagTree()
    tags.value = res || []
  } finally {
    loading.value = false
  }
}

function openDlg(parent) {
  dlg.value = {
    visible: true,
    title: parent ? `新建子标签(挂到: ${parent.path})` : '新建一级标签',
    id: null, parentId: parent ? parent.id : 0,
    name: '', category: parent ? parent.category : '自定义标签',
    granularity: parent ? parent.granularity : '表级', source: ''
  }
}
function openEdit(row) {
  dlg.value = { visible: true, title: `编辑标签: ${row.path}`, id: row.id, parentId: 0,
    name: row.tag, category: row.category, granularity: row.granularity, source: row.source }
}
async function remove(row) {
  await ElMessageBox.confirm(`删除标签「${row.path}」及其子标签?`, '确认', { type: 'warning' })
  await deleteTag(row.id)
  ElMessage.success('已删除')
  load()
}
async function submit() {
  if (!dlg.value.name) return
  if (dlg.value.id) {
    await updateTag(dlg.value.id, { name: dlg.value.name, category: dlg.value.category, granularity: dlg.value.granularity, source: dlg.value.source })
  } else {
    await createTag({ name: dlg.value.name, parentId: dlg.value.parentId, category: dlg.value.category, granularity: dlg.value.granularity, source: dlg.value.source })
  }
  dlg.value.visible = false
  ElMessage.success('已保存')
  load()
}
async function showAssets(row) {
  const res = await getTagAssets(row.path)
  assetDlg.value = { visible: true, path: row.path, assets: res || [] }
}

onMounted(load)
</script>

<style scoped>
.root-tag { font-weight: 600; }
.path-sep { color: #c0c4cc; margin: 0 2px; }
.path-sub { margin-left: 8px; color: #c0c4cc; font-size: 11px; }
.tip { margin-top: 12px; font-size: 12px; color: #8f959e; }
.sub { color: #8f959e; font-size: 11px; }
</style>
