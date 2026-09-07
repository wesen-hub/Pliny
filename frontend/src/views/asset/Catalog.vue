<template>
  <div class="page-card">
    <el-card shadow="never">
      <div class="catalog-layout">
        <!-- 左侧: 标签树目录(可编辑层级, 多选构建子集) -->
        <div class="tree-panel">
          <div class="tree-toolbar">
            <el-button size="small" style="width: 100%; margin-bottom: 8px" @click="openTagDlg(null)">＋ 新建根标签</el-button>
          </div>
          <el-tree
            ref="tagTree"
            :data="tagTree"
            node-key="id"
            default-expand-all
            show-checkbox
            :props="{ label: 'path', children: 'children' }"
            :check-strictly="true"
            @check="onCheck"
          >
            <template #default="{ data }">
              <span class="tree-label">
                <el-icon v-if="data.children"><Folder /></el-icon>
                <el-icon v-else><Document /></el-icon>
                <span>{{ data.tag }}</span>
                <span class="tree-count">{{ data.assets }}</span>
                <span class="tree-ops" @click.stop>
                  <el-icon title="新建子标签" @click="openTagDlg(data)"><Plus /></el-icon>
                  <el-icon title="重命名" @click="openTagDlg(data, true)"><Edit /></el-icon>
                  <el-icon title="移动到" @click="openMoveDlg(data)"><Rank /></el-icon>
                  <el-icon title="删除" @click="removeTag(data)"><Delete /></el-icon>
                </span>
              </span>
            </template>
          </el-tree>
          <div class="tree-tip">目录 = 标签树(自定义层级) · 勾选标签组合 = 动态查询子集 · 打子标签自动继承父链</div>
        </div>

        <!-- 右侧: 子集结果(标签组合筛选) -->
        <div class="list-panel">
          <div class="toolbar">
            <el-select v-model="op" size="small" style="width: 90px" @change="load">
              <el-option label="AND" value="AND" /><el-option label="OR" value="OR" />
            </el-select>
            <el-input v-model="keyword" placeholder="搜索资产…" clearable style="width: 200px">
              <template #prefix><el-icon><Search /></el-icon></template>
            </el-input>
            <el-tag v-for="t in selectedTags" :key="t" closable type="primary" size="small" @close="uncheckTag(t)">{{ t }}</el-tag>
            <el-button size="small" :loading="loading" @click="load">刷新</el-button>
            <span class="sub">共 {{ assets.length }} 个资产</span>
          </div>
          <el-table :data="filteredAssets" stripe v-loading="loading">
            <el-table-column label="资产名称" min-width="160">
              <template #default="{ row }">
                <el-link type="primary" @click="$router.push(`/asset/${row.table}`)">{{ row.displayName || row.name }}</el-link>
                <div class="sub">{{ row.table }}</div>
              </template>
            </el-table-column>
            <el-table-column label="标签" min-width="220">
              <template #default="{ row }">
                <el-tag v-for="t in (row.tags || [])" :key="t" size="small" style="margin-right:4px">{{ t }}</el-tag>
                <el-tag v-if="row.layer" size="small" type="info" style="margin-right:4px">分层/{{ row.layer }}</el-tag>
                <el-tag v-if="row.domain" size="small" type="info" style="margin-right:4px">业务域/{{ row.domain }}</el-tag>
              </template>
            </el-table-column>
            <el-table-column label="来源" min-width="140">
              <template #default="{ row }">
                <el-tag v-for="s in row.sources" :key="s.table" size="small">{{ s.dsName }}</el-tag>
              </template>
            </el-table-column>
            <el-table-column label="密级" width="70">
              <template #default="{ row }"><span :class="`secret sec-${row.secretLevel}`">{{ secretName(row.secretLevel) }}</span></template>
            </el-table-column>
            <el-table-column label="表数" width="80">
              <template #default="{ row }"><span>{{ row.shardCount ?? '—' }}</span></template>
            </el-table-column>
            <el-table-column label="状态" width="80">
              <template #default="{ row }">
                <el-tag size="small" :type="row.status==='就绪'?'success':'info'" effect="plain">{{ row.status }}</el-tag>
              </template>
            </el-table-column>
            <el-table-column label="操作" width="150">
              <template #default="{ row }">
                <el-link type="primary" underline="never" @click="$router.push(`/asset/${row.table}`)">详情</el-link>
                <el-link type="primary" underline="never" style="margin-left:8px" @click="openMeta(row)">打标</el-link>
              </template>
            </el-table-column>
          </el-table>
        </div>
      </div>
    </el-card>

    <!-- 标签编辑对话框 -->
    <el-dialog v-model="tagDlg.visible" :title="tagDlg.title" width="420px">
      <el-form label-width="70px">
        <el-form-item label="标签名"><el-input v-model="tagDlg.name" placeholder="如: 订单 / 大促核心" /></el-form-item>
        <el-form-item label="类型">
          <el-select v-model="tagDlg.category" style="width: 100%">
            <el-option label="业务标签" value="业务标签" /><el-option label="系统标签" value="系统标签" />
            <el-option label="敏感标签" value="敏感标签" /><el-option label="自定义标签" value="自定义标签" />
          </el-select>
        </el-form-item>
        <el-form-item label="粒度">
          <el-select v-model="tagDlg.granularity" style="width: 100%">
            <el-option label="表级" value="表级" /><el-option label="字段级" value="字段级" /><el-option label="记录级" value="记录级" />
          </el-select>
        </el-form-item>
        <el-form-item label="来源"><el-input v-model="tagDlg.source" placeholder="如: 模型定义 / 手动 / 规则引擎" /></el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="tagDlg.visible = false">取消</el-button>
        <el-button type="primary" @click="submitTag">保存</el-button>
      </template>
    </el-dialog>

    <!-- 移动标签对话框 -->
    <el-dialog v-model="moveDlg.visible" title="移动标签(调整目录层级)" width="420px">
      <el-form label-width="70px">
        <el-form-item label="目标父级">
          <el-tree-select v-model="moveDlg.parentId" :data="moveTargets" :props="{ label: 'path', value: 'id', children: 'children' }"
            check-strictly style="width: 100%" placeholder="选择新父节点(留空=根)" />
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="moveDlg.visible = false">取消</el-button>
        <el-button type="primary" @click="submitMove">移动</el-button>
      </template>
    </el-dialog>

    <!-- 资产打标对话框 -->
    <el-dialog v-model="metaDlg.visible" :title="`资产打标: ${metaDlg.table || ''}`" width="480px">
      <el-form label-width="90px">
        <el-form-item label="标签">
          <el-tree-select v-model="metaDlg.form.tags" :data="tagTree" multiple filterable
            :render-after-expand="false" :props="{ label: 'path', value: 'path', children: 'children' }"
            check-strictly style="width: 100%" placeholder="选择标签(多选) / 输入新标签" />
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="metaDlg.visible = false">取消</el-button>
        <el-button type="primary" @click="submitMeta">保存</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup>
import { ref, computed, onMounted, nextTick } from 'vue'
import { useRouter } from 'vue-router'
import { ElMessage, ElMessageBox } from 'element-plus'
import {
  getTagTree, createTag, updateTag, deleteTag, moveTag, getTagSubset,
  getCatalogAssets, saveAssetMeta
} from '../../api'

const router = useRouter()
const tagTree = ref([])
const tagTreeRef = ref(null)
const assets = ref([])
const selectedTags = ref([])
const op = ref('AND')
const keyword = ref('')
const loading = ref(false)

const tagDlg = ref({ visible: false, title: '', id: null, parentId: 0, name: '', category: '自定义标签', granularity: '表级', source: '' })
const moveDlg = ref({ visible: false, id: null, parentId: null, nodePath: '' })
const metaDlg = ref({ visible: false, table: '', form: { tags: [] } })

const filteredAssets = computed(() => assets.value.filter(a =>
  !keyword.value || a.table.includes(keyword.value) || (a.displayName || '').includes(keyword.value)))

function secretName(level) { return ['', '公开', '内部', '机密', '绝密'][level] }

async function load() {
  loading.value = true
  try {
    const [t, r] = await Promise.all([
      getTagTree(),
      getTagSubset(selectedTags.value, op.value)
    ])
    tagTree.value = t || []
    assets.value = r || []
  } finally {
    loading.value = false
  }
}

// ---- 树勾选 → 子集 ----
function onCheck(data, status) {
  const checked = status.checkedKeys || []
  const paths = []
  const walk = (nodes) => nodes.forEach(n => {
    if (checked.includes(n.id)) paths.push(n.path)
    walk(n.children || [])
  })
  walk(tagTree.value)
  selectedTags.value = paths
  load()
}
function uncheckTag(path) {
  // 找到对应节点取消勾选
  const find = (nodes) => {
    for (const n of nodes) {
      if (n.path === path) return n
      const r = find(n.children || [])
      if (r) return r
    }
    return null
  }
  const node = find(tagTree.value)
  if (node && tagTreeRef.value) tagTreeRef.value.setChecked(node.id, false)
  selectedTags.value = selectedTags.value.filter(t => t !== path)
  load()
}

// ---- 标签维护 ----
function openTagDlg(parent, isRename = false) {
  tagDlg.value = {
    visible: true,
    title: isRename ? `重命名: ${parent.path}` : (parent ? `新建子标签(挂到: ${parent.path})` : '新建根标签'),
    id: isRename ? parent.id : null,
    parentId: parent ? parent.id : 0,
    name: isRename ? parent.tag : '',
    category: parent ? parent.category : '自定义标签',
    granularity: parent ? parent.granularity : '表级',
    source: ''
  }
}
async function removeTag(node) {
  await ElMessageBox.confirm(`删除标签「${node.path}」及其子标签? (资产上的该标签将被移除)`, '确认', { type: 'warning' })
  await deleteTag(node.id)
  ElMessage.success('已删除')
  load()
}
async function submitTag() {
  if (!tagDlg.value.name) return
  if (tagDlg.value.id) {
    await updateTag(tagDlg.value.id, { name: tagDlg.value.name, category: tagDlg.value.category, granularity: tagDlg.value.granularity, source: tagDlg.value.source })
  } else {
    await createTag({ name: tagDlg.value.name, parentId: tagDlg.value.parentId, category: tagDlg.value.category, granularity: tagDlg.value.granularity, source: tagDlg.value.source })
  }
  tagDlg.value.visible = false
  ElMessage.success('已保存')
  load()
}

// ---- 移动(调层级) ----
const moveTargets = computed(() => {
  // 可移动目标 = 整棵树(后端防环)
  return tagTree.value
})
function openMoveDlg(node) {
  moveDlg.value = { visible: true, id: node.id, parentId: null, nodePath: node.path }
}
async function submitMove() {
  await moveTag(moveDlg.value.id, moveDlg.value.parentId || 0)
  moveDlg.value.visible = false
  ElMessage.success('已移动')
  load()
}

// ---- 资产打标 ----
async function openMeta(row) {
  metaDlg.value = {
    visible: true, table: row.table,
    form: { tags: row.tags || [] }
  }
}
async function submitMeta() {
  // 保留原有元数据(打标只更新 tags)
  await saveAssetMeta(metaDlg.value.table, { tags: metaDlg.value.form.tags })
  metaDlg.value.visible = false
  ElMessage.success('已保存')
  load()
}

onMounted(load)
</script>

<style scoped>
.catalog-layout { display: flex; gap: 16px; min-height: 560px; }
.tree-panel { width: 320px; border-right: 1px solid #f0f0f0; padding-right: 16px; flex-shrink: 0; }
.list-panel { flex: 1; min-width: 0; }
.tree-label { display: flex; align-items: center; gap: 6px; font-size: 13px; flex: 1; }
.tree-count { margin-left: auto; color: #8f959e; font-size: 11px; }
.tree-ops { display: none; gap: 2px; margin-left: 6px; color: #8f959e; }
.tree-label:hover .tree-ops { display: inline-flex; }
.tree-ops .el-icon { cursor: pointer; }
.tree-ops .el-icon:hover { color: #1677ff; }
.tree-tip { margin-top: 12px; font-size: 12px; color: #8f959e; line-height: 1.6; }
.sub { color: #8f959e; font-size: 11px; }
.secret { display: inline-flex; align-items: center; padding: 1px 8px; border-radius: 4px; font-size: 12px; line-height: 18px; font-weight: 600; }
.sec-1 { background: #f6ffed; color: #389e0d; } .sec-2 { background: #e6f4ff; color: #0958d9; }
.sec-3 { background: #fff7e6; color: #d46b08; } .sec-4 { background: #fff1f0; color: #cf1322; }
</style>
