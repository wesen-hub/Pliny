<template>
  <div class="page-card">
    <el-card shadow="never">
      <el-row :gutter="16">
        <!-- 左侧: 字典列表 -->
        <el-col :span="8">
          <div class="toolbar">
            <el-input v-model="keyword" placeholder="搜索字典" clearable style="width: 160px" @keyup.enter="load" />
            <el-button v-if="userStore.canWrite" type="success" size="small" @click="dictDialog = true">
              <el-icon><Plus /></el-icon>新建字典
            </el-button>
          </div>
          <el-table :data="dicts" highlight-current-row @current-change="selectDict" size="small">
            <el-table-column prop="dictName" label="字典名称" />
            <el-table-column prop="description" label="说明" show-overflow-tooltip />
            <el-table-column label="操作" width="130">
              <template #default="{ row }">
                <el-button size="small" text type="primary" :disabled="!userStore.canWrite" @click.stop="editDict(row)">编辑</el-button>
                <el-button size="small" text type="danger" :disabled="!userStore.canWrite" @click.stop="removeDict(row)">删除</el-button>
              </template>
            </el-table-column>
          </el-table>
        </el-col>

        <!-- 右侧: 条目表 -->
        <el-col :span="16">
          <div class="toolbar">
            <div>
              <b>{{ current?.dictName || '请选择字典' }}</b>
              <el-button v-if="current && userStore.canWrite" type="success" size="small" style="margin-left: 12px" @click="entryDialog = true">
                <el-icon><Plus /></el-icon>新增条目
              </el-button>
            </div>
            <div v-if="current">
              <el-upload :show-file-list="false" :http-request="doImport" accept=".csv">
                <el-button size="small">导入 CSV</el-button>
              </el-upload>
              <el-button size="small" @click="download(exportDictUrl(current.id), `dict_${current.dictName}.csv`)">导出</el-button>
              <el-button size="small" @click="download(dictTemplateUrl, 'dict_template.csv')">模板</el-button>
            </div>
          </div>
          <el-table :data="entries" size="small" border v-loading="entryLoading">
            <el-table-column prop="sourceValue" label="源值" />
            <el-table-column prop="targetValue" label="目标值" />
            <el-table-column prop="sortNo" label="排序" width="70" />
            <el-table-column label="操作" width="130">
              <template #default="{ row }">
                <el-button size="small" text type="primary" :disabled="!userStore.canWrite" @click="editEntry(row)">编辑</el-button>
                <el-button size="small" text type="danger" :disabled="!userStore.canWrite" @click="removeEntry(row)">删除</el-button>
              </template>
            </el-table-column>
          </el-table>
        </el-col>
      </el-row>
    </el-card>

    <!-- 字典编辑 -->
    <el-dialog v-model="dictDialog" :title="dictForm.id ? '编辑字典' : '新建字典'" width="400px">
      <el-form label-width="70px">
        <el-form-item label="名称" required>
          <el-input v-model="dictForm.dictName" placeholder="如: ORDER_STATUS" />
        </el-form-item>
        <el-form-item label="说明">
          <el-input v-model="dictForm.description" />
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="dictDialog = false">取消</el-button>
        <el-button type="primary" @click="saveDict">保存</el-button>
      </template>
    </el-dialog>

    <!-- 条目编辑 -->
    <el-dialog v-model="entryDialog" :title="entryForm.id ? '编辑条目' : '新增条目'" width="420px">
      <el-form label-width="70px">
        <el-form-item label="源值" required>
          <el-input v-model="entryForm.sourceValue" placeholder="如: 0" />
        </el-form-item>
        <el-form-item label="目标值" required>
          <el-input v-model="entryForm.targetValue" placeholder="如: 待支付" />
        </el-form-item>
        <el-form-item label="排序">
          <el-input-number v-model="entryForm.sortNo" :min="0" />
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="entryDialog = false">取消</el-button>
        <el-button type="primary" @click="saveEntry">保存</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup>
import { onMounted, reactive, ref } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import {
  listDicts, createDict, updateDict, deleteDict, dictEntries, createDictEntry,
  updateDictEntry, deleteDictEntry, importDict, exportDictUrl, dictTemplateUrl
} from '../../api'
import { useUserStore } from '../../store/user'

const userStore = useUserStore()
const keyword = ref('')
const dicts = ref([])
const entries = ref([])
const entryLoading = ref(false)
const current = ref(null)
const dictDialog = ref(false)
const entryDialog = ref(false)
const dictForm = reactive({ id: null, dictName: '', description: '' })
const entryForm = reactive({ id: null, sourceValue: '', targetValue: '', sortNo: 0 })

async function load() {
  const data = await listDicts({ page: 1, pageSize: 100, keyword: keyword.value })
  dicts.value = data.list
  if (current.value) {
    const c = dicts.value.find(d => d.id === current.value.id)
    if (c) {
      current.value = c
      loadEntries()
    } else {
      current.value = null
    }
  }
}

async function selectDict(row) {
  current.value = row
  loadEntries()
}

async function loadEntries() {
  if (!current.value) return
  entryLoading.value = true
  try {
    entries.value = await dictEntries(current.value.id)
  } finally {
    entryLoading.value = false
  }
}

function editDict(row) {
  Object.assign(dictForm, { id: row.id, dictName: row.dictName, description: row.description })
  dictDialog.value = true
}

async function saveDict() {
  if (dictForm.id) {
    await updateDict(dictForm.id, dictForm)
  } else {
    await createDict(dictForm)
  }
  ElMessage.success('已保存')
  dictDialog.value = false
  load()
}

async function removeDict(row) {
  await ElMessageBox.confirm(`确定删除字典「${row.dictName}」? (被映射引用时会被拦截)`, '提示', { type: 'warning' })
  try {
    await deleteDict(row.id)
    ElMessage.success('已删除')
    if (current.value?.id === row.id) current.value = null
    load()
  } catch (e) { /* ignore */ }
}

function editEntry(row) {
  Object.assign(entryForm, { id: row.id, sourceValue: row.sourceValue, targetValue: row.targetValue, sortNo: row.sortNo })
  entryDialog.value = true
}

async function saveEntry() {
  if (entryForm.id) {
    await updateDictEntry(entryForm.id, entryForm)
  } else {
    await createDictEntry(current.value.id, entryForm)
  }
  ElMessage.success('已保存')
  entryDialog.value = false
  loadEntries()
}

async function removeEntry(row) {
  await ElMessageBox.confirm('确定删除该条目?', '提示', { type: 'warning' })
  await deleteDictEntry(row.id)
  ElMessage.success('已删除')
  loadEntries()
}

async function doImport({ file }) {
  const n = await importDict(current.value.id, file, false)
  ElMessage.success(`导入成功 ${n} 条 (重复项跳过)`)
  loadEntries()
}

function download(url, filename) {
  const a = document.createElement('a')
  a.href = url
  a.download = filename
  a.click()
}

onMounted(load)
</script>
