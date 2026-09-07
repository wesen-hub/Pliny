<template>
  <div class="page-card">
    <el-card shadow="never">
      <div class="toolbar">
        <div style="font-weight: 600">模型管理<el-text type="info" style="margin-left: 8px; font-weight: 400">先建模后同步 · 发布即建 Kafka topic/分区 + 预建物理分表</el-text></div>
        <div>
          <el-button :loading="loading" @click="load">刷新</el-button>
          <el-button type="primary" @click="openCreate">＋ 新建模型</el-button>
        </div>
      </div>
      <el-table :data="models" stripe v-loading="loading">
        <el-table-column label="模型" min-width="160">
          <template #default="{ row }">
            <el-link type="primary" @click="toWizard(row)">{{ row.name }}</el-link>
            <el-tag size="small" :type="layerType(row.layer)" effect="plain" style="margin-left:6px">{{ row.layer }}</el-tag>
          </template>
        </el-table-column>
        <el-table-column prop="domain" label="业务域" width="90" />
        <el-table-column label="逻辑表 / Topic" min-width="240">
          <template #default="{ row }">
            <div>{{ row.table_name }}</div>
            <div class="sub mono">{{ row.topic }}</div>
          </template>
        </el-table-column>
        <el-table-column label="分区数" width="80">
          <template #default="{ row }">{{ row.partition_count }}</template>
        </el-table-column>
        <el-table-column label="物理表数" width="90">
          <template #default="{ row }">{{ row.table_count }}</template>
        </el-table-column>
        <el-table-column label="业务键" min-width="140">
          <template #default="{ row }">
            <el-tag v-for="k in keys(row)" :key="k" size="small" type="warning">{{ k }}</el-tag>
          </template>
        </el-table-column>
        <el-table-column prop="create_time" label="创建时间" width="170" />
        <el-table-column label="操作" width="200">
          <template #default="{ row }">
            <el-link type="primary" underline="never" @click="toWizard(row)">配同步任务</el-link>
            <el-link type="danger" underline="never" style="margin-left:10px" @click="remove(row)">删除</el-link>
          </template>
        </el-table-column>
      </el-table>
    </el-card>

    <!-- 新建模型 -->
    <el-dialog v-model="dlg.visible" title="新建模型(发布即建 topic/分区 + 预建物理表)" width="640px">
      <el-form label-width="100px">
        <el-row :gutter="12">
          <el-col :span="12">
            <el-form-item label="模型名称"><el-input v-model="dlg.name" placeholder="如: 客户明细" /></el-form-item>
          </el-col>
          <el-col :span="12">
            <el-form-item label="逻辑表名"><el-input v-model="dlg.tableName" placeholder="如: t_cust_model" /></el-form-item>
          </el-col>
        </el-row>
        <el-row :gutter="12">
          <el-col :span="12">
            <el-form-item label="分层">
              <el-select v-model="dlg.layer" style="width: 100%">
                <el-option v-for="x in ['ODS','DWD','DWS','ADS']" :key="x" :label="x" :value="x" />
              </el-select>
            </el-form-item>
          </el-col>
          <el-col :span="12">
            <el-form-item label="业务域"><el-input v-model="dlg.domain" /></el-form-item>
          </el-col>
        </el-row>
        <el-row :gutter="12">
          <el-col :span="12">
            <el-form-item label="分区数量"><el-input-number v-model="dlg.partitionCount" :min="1" :max="256" style="width:100%" /><div class="sub">Kafka topic 分区数(消费并行上限)</div></el-form-item>
          </el-col>
          <el-col :span="12">
          </el-col>
        </el-row>
        <el-form-item label="模型字段">
          <div style="width: 100%">
            <div v-for="(f, i) in dlg.fields" :key="i" class="field-row">
              <el-input v-model="f.name" placeholder="字段名" style="width: 170px" @input="onFieldInput" />
              <el-select v-model="f.type" placeholder="类型" style="width: 130px">
                <el-option v-for="x in ['BIGINT','VARCHAR(32)','VARCHAR(64)','DECIMAL(18,2)','INT','TIMESTAMP','DATE']" :key="x" :label="x" :value="x" />
              </el-select>
              <el-input v-model="f.desc" placeholder="描述" style="width: 160px" />
              <el-button link type="danger" @click="dlg.fields.splice(i, 1)">✕</el-button>
            </div>
            <el-button link type="primary" @click="dlg.fields.push({ name: '', type: 'VARCHAR(64)', desc: '' })">＋ 添加字段</el-button>
          </div>
        </el-form-item>
        <el-form-item label="业务键字段">
          <el-select v-model="dlg.keyColumns" multiple style="width: 100%" placeholder="从模型字段选择(目标侧路由/收敛键, 如 id_card_no)">
            <el-option v-for="f in validFields" :key="f" :label="f" :value="f" />
          </el-select>
        </el-form-item>
        <el-form-item label="说明"><el-input v-model="dlg.description" type="textarea" :rows="2" /></el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="dlg.visible = false">取消</el-button>
        <el-button type="primary" :loading="saving" @click="submit">发布(建 topic/物理表)</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue'
import { useRouter } from 'vue-router'
import { ElMessage, ElMessageBox } from 'element-plus'
import { listModels, createModel, deleteModel } from '../../api'

const router = useRouter()
const models = ref([])
const loading = ref(false)
const saving = ref(false)
const dlg = ref(empty())

function empty() {
  return { visible: false, name: '', layer: 'DWD', domain: '', tableName: '', partitionCount: 8, tableCount: 1, fields: [{ name: '', type: 'VARCHAR(64)', desc: '' }], keyColumns: [], description: '' }
}
const validFields = computed(() => dlg.value.fields.map(f => f.name).filter(Boolean))

function layerType(layer) { return { ODS: 'info', DWD: 'primary', DWS: 'warning', ADS: 'success' }[layer] || 'info' }
function keys(row) {
  try { return JSON.parse(row.key_columns || '[]') } catch { return [] }
}
function onFieldInput() {
  // 业务键选项实时跟随字段名(简单处理: 组件 select 已绑定 validFields)
}

async function load() {
  loading.value = true
  try {
    models.value = await listModels() || []
  } finally {
    loading.value = false
  }
}
function openCreate() {
  dlg.value = empty()
  dlg.value.visible = true
}
async function submit() {
  if (!dlg.value.name || !dlg.value.tableName) { ElMessage.warning('请填写模型名和逻辑表名'); return }
  if (!dlg.value.keyColumns.length) { ElMessage.warning('请选择业务键字段'); return }
  saving.value = true
  try {
    const fields = dlg.value.fields.filter(f => f.name)
    await createModel({
      name: dlg.value.name, layer: dlg.value.layer, domain: dlg.value.domain,
      tableName: dlg.value.tableName, partitionCount: dlg.value.partitionCount,
      tableCount: dlg.value.tableCount, keyColumns: dlg.value.keyColumns,
      fields, description: dlg.value.description
    })
    ElMessage.success(`模型已发布: topic ${'dcp.' + dlg.value.tableName}(${dlg.value.partitionCount} 分区) + TiDB 单表 ${dlg.value.tableName} 已创建`)
    dlg.value.visible = false
    load()
  } finally {
    saving.value = false
  }
}
function toWizard(row) {
  router.push({ path: '/sync/task/create', query: { model: row.id, table: row.table_name } })
}
async function remove(row) {
  await ElMessageBox.confirm(`删除模型「${row.name}」? (逻辑表/topic 保留, 数据不受影响)`, '确认', { type: 'warning' })
  await deleteModel(row.id)
  ElMessage.success('已删除')
  load()
}

onMounted(load)
</script>

<style scoped>
.sub { color: #8f959e; font-size: 11px; }
.mono { font-family: ui-monospace, Menlo, Consolas, monospace; }
.field-row { display: flex; gap: 8px; margin-bottom: 6px; }
</style>
